from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from app.config import DATASET_DIR


RAW_TABLE_PRIMARY_KEYS: dict[str, list[str]] = {
    "sales_order_headers": ["salesOrder"],
    "sales_order_items": ["salesOrder", "salesOrderItem"],
    "sales_order_schedule_lines": ["salesOrder", "salesOrderItem", "scheduleLine"],
    "outbound_delivery_headers": ["deliveryDocument"],
    "outbound_delivery_items": ["deliveryDocument", "deliveryDocumentItem"],
    "billing_document_headers": ["billingDocument"],
    "billing_document_cancellations": ["billingDocument"],
    "billing_document_items": ["billingDocument", "billingDocumentItem"],
    "journal_entry_items_accounts_receivable": ["companyCode", "fiscalYear", "accountingDocument"],
    "payments_accounts_receivable": ["companyCode", "fiscalYear", "accountingDocument"],
    "business_partners": ["businessPartner"],
    "business_partner_addresses": ["addressId"],
    "customer_company_assignments": ["customer", "companyCode"],
    "customer_sales_area_assignments": ["customer", "salesOrganization", "distributionChannel", "division"],
    "products": ["product"],
    "product_descriptions": ["product", "language"],
    "plants": ["plant"],
    "product_plants": ["product", "plant"],
    "product_storage_locations": ["product", "plant", "storageLocation"],
}


@dataclass
class NodeRecord:
    node_id: str
    node_type: str
    label: str
    source_table: str
    entity_key: str
    metadata: dict[str, Any]


@dataclass
class EdgeRecord:
    edge_id: str
    source_node_id: str
    target_node_id: str
    edge_type: str
    source_table: str
    evidence: dict[str, Any]
    inferred: bool = False
    confidence: float = 1.0


class DatasetLoader:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.dataset_dir = DATASET_DIR

    def load(self, force_reload: bool = False) -> None:
        if not self.dataset_dir.exists():
            raise FileNotFoundError(f"Dataset folder not found: {self.dataset_dir}")

        rows_by_table = self._read_dataset()
        if not rows_by_table:
            raise ValueError("Dataset folder is empty.")

        if force_reload and self.db_path.exists():
            self.db_path.unlink()

        conn = sqlite3.connect(self.db_path)
        try:
            conn.row_factory = sqlite3.Row
            self._create_meta_tables(conn)
            self._create_raw_tables(conn, rows_by_table)
            self._populate_raw_tables(conn, rows_by_table)
            self._create_graph_tables(conn)
            nodes, edges = self._build_graph(rows_by_table)
            self._populate_graph(conn, nodes, edges)
            self._store_dataset_profile(conn, rows_by_table, nodes, edges)
            conn.commit()
        finally:
            conn.close()

    def _read_dataset(self) -> dict[str, list[dict[str, Any]]]:
        rows_by_table: dict[str, list[dict[str, Any]]] = {}
        for folder in sorted(p for p in self.dataset_dir.iterdir() if p.is_dir()):
            rows: list[dict[str, Any]] = []
            for file_path in sorted(folder.glob("*.jsonl")):
                with file_path.open("r", encoding="utf-8-sig") as handle:
                    for raw_line in handle:
                        line = raw_line.strip()
                        if not line:
                            continue
                        rows.append(json.loads(line))
            rows_by_table[folder.name] = rows
        return rows_by_table

    def _create_meta_tables(self, conn: sqlite3.Connection) -> None:
        conn.execute("DROP TABLE IF EXISTS dataset_profile")
        conn.execute(
            """
            CREATE TABLE dataset_profile (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

    def _create_raw_tables(self, conn: sqlite3.Connection, rows_by_table: dict[str, list[dict[str, Any]]]) -> None:
        for table_name, rows in rows_by_table.items():
            conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            columns = self._all_columns(rows)
            column_sql = ", ".join(f'"{column}" TEXT' for column in columns)
            conn.execute(f'CREATE TABLE "{table_name}" ({column_sql})')

    def _populate_raw_tables(self, conn: sqlite3.Connection, rows_by_table: dict[str, list[dict[str, Any]]]) -> None:
        for table_name, rows in rows_by_table.items():
            columns = self._all_columns(rows)
            if not columns:
                continue
            placeholders = ", ".join("?" for _ in columns)
            column_sql = ", ".join(f'"{column}"' for column in columns)
            conn.executemany(
                f'INSERT INTO "{table_name}" ({column_sql}) VALUES ({placeholders})',
                [[self._to_sql_value(row.get(column)) for column in columns] for row in rows],
            )
            self._create_indexes(conn, table_name, columns)

    def _create_indexes(self, conn: sqlite3.Connection, table_name: str, columns: list[str]) -> None:
        interesting_columns = {
            column
            for column in columns
            if any(
                token in column.lower()
                for token in ("document", "order", "billing", "delivery", "customer", "material", "product", "plant")
            )
        }
        for column in sorted(interesting_columns):
            index_name = f"idx_{table_name}_{column}"
            conn.execute(f'DROP INDEX IF EXISTS "{index_name}"')
            conn.execute(f'CREATE INDEX "{index_name}" ON "{table_name}"("{column}")')

    def _create_graph_tables(self, conn: sqlite3.Connection) -> None:
        conn.execute("DROP TABLE IF EXISTS graph_nodes")
        conn.execute("DROP TABLE IF EXISTS graph_edges")
        conn.execute(
            """
            CREATE TABLE graph_nodes (
                node_id TEXT PRIMARY KEY,
                node_type TEXT NOT NULL,
                label TEXT NOT NULL,
                source_table TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE graph_edges (
                edge_id TEXT PRIMARY KEY,
                source_node_id TEXT NOT NULL,
                target_node_id TEXT NOT NULL,
                edge_type TEXT NOT NULL,
                source_table TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                inferred INTEGER NOT NULL,
                confidence REAL NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX idx_graph_edges_source ON graph_edges(source_node_id)")
        conn.execute("CREATE INDEX idx_graph_edges_target ON graph_edges(target_node_id)")
        conn.execute("CREATE INDEX idx_graph_nodes_type ON graph_nodes(node_type)")

    def _populate_graph(self, conn: sqlite3.Connection, nodes: Iterable[NodeRecord], edges: Iterable[EdgeRecord]) -> None:
        conn.executemany(
            """
            INSERT INTO graph_nodes (node_id, node_type, label, source_table, entity_key, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    node.node_id,
                    node.node_type,
                    node.label,
                    node.source_table,
                    node.entity_key,
                    json.dumps(node.metadata, default=str),
                )
                for node in nodes
            ],
        )
        conn.executemany(
            """
            INSERT INTO graph_edges (edge_id, source_node_id, target_node_id, edge_type, source_table, evidence_json, inferred, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    edge.edge_id,
                    edge.source_node_id,
                    edge.target_node_id,
                    edge.edge_type,
                    edge.source_table,
                    json.dumps(edge.evidence, default=str),
                    int(edge.inferred),
                    edge.confidence,
                )
                for edge in edges
            ],
        )

    def _store_dataset_profile(
        self,
        conn: sqlite3.Connection,
        rows_by_table: dict[str, list[dict[str, Any]]],
        nodes: list[NodeRecord],
        edges: list[EdgeRecord],
    ) -> None:
        raw_table_profiles = []
        quality_notes: list[str] = []

        delivery_orders = {row["referenceSdDocument"] for row in rows_by_table["outbound_delivery_items"] if row.get("referenceSdDocument")}
        sales_orders = {row["salesOrder"] for row in rows_by_table["sales_order_headers"] if row.get("salesOrder")}
        billed_deliveries = {row["referenceSdDocument"] for row in rows_by_table["billing_document_items"] if row.get("referenceSdDocument")}
        billing_documents = {row["billingDocument"] for row in rows_by_table["billing_document_headers"] if row.get("billingDocument")}
        journal_by_billing = {
            row["referenceDocument"]
            for row in rows_by_table["journal_entry_items_accounts_receivable"]
            if row.get("referenceDocument")
        }

        delivered_not_billed = sorted(
            order_id
            for order_id in delivery_orders
            if order_id in sales_orders
            and not any(delivery in billed_deliveries for delivery in self._deliveries_for_order(rows_by_table, order_id))
        )
        billing_without_journal = sorted(billing_documents - journal_by_billing)

        quality_notes.append("All inspected header/item document keys were unique for sales orders, deliveries, and billing documents.")
        quality_notes.append(f"{len(set(sales_orders) - delivery_orders)} sales orders have no downstream delivery in this snapshot.")
        quality_notes.append(f"{len(delivered_not_billed)} delivered sales orders have no downstream billing document.")
        quality_notes.append(f"{len(billing_without_journal)} billing documents have no matching journal entry by accounting key.")
        quality_notes.append("Payments do not carry invoiceReference or salesDocument values, so invoice-to-payment answers rely on accounting clearing status instead of explicit payment linkage.")
        quality_notes.append("Customer addresses are sparse: city, street, and postal code are null for several business partners.")
        quality_notes.append("Delivery header actual goods movement dates are mostly null, so chronology should prefer creation dates when movement timestamps are missing.")

        inferred_relationships = [
            "billing_document_items.referenceSdDocument points to outbound_delivery_headers.deliveryDocument for all observed billed flows.",
            "business_partners.customer is the stable customer key used by sales orders and accounting tables; address records link through businessPartner.",
            "payments_accounts_receivable overlaps journal_entry_items_accounts_receivable at accountingDocument level, so payment coverage is inferred from AR clearing fields instead of direct invoice references.",
        ]

        for table_name, rows in rows_by_table.items():
            columns = self._all_columns(rows)
            primary_key = RAW_TABLE_PRIMARY_KEYS.get(table_name, [])
            null_counts = {column: sum(1 for row in rows if row.get(column) in (None, "")) for column in columns}
            raw_table_profiles.append(
                {
                    "table": table_name,
                    "row_count": len(rows),
                    "columns": columns,
                    "primary_key": primary_key,
                    "null_columns": {k: v for k, v in null_counts.items() if v},
                }
            )

        node_counts = Counter(node.node_type for node in nodes)
        edge_counts = Counter(edge.edge_type for edge in edges)
        payloads = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "dataset_path": str(self.dataset_dir),
            "raw_tables": raw_table_profiles,
            "node_counts": [{"type": key, "count": value} for key, value in sorted(node_counts.items())],
            "edge_counts": [{"type": key, "count": value} for key, value in sorted(edge_counts.items())],
            "data_quality_notes": quality_notes,
            "inferred_relationships": inferred_relationships,
        }

        for key, value in payloads.items():
            conn.execute("INSERT INTO dataset_profile (key, value) VALUES (?, ?)", (key, json.dumps(value, default=str)))

    def _build_graph(self, rows_by_table: dict[str, list[dict[str, Any]]]) -> tuple[list[NodeRecord], list[EdgeRecord]]:
        nodes: dict[str, NodeRecord] = {}
        edges: dict[str, EdgeRecord] = {}

        partners_by_customer = {row["customer"]: row for row in rows_by_table["business_partners"] if row.get("customer")}
        addresses_by_bp = {row["businessPartner"]: row for row in rows_by_table["business_partner_addresses"] if row.get("businessPartner")}
        company_by_customer = {row["customer"]: row for row in rows_by_table["customer_company_assignments"] if row.get("customer")}
        sales_area_by_customer = defaultdict(list)
        for row in rows_by_table["customer_sales_area_assignments"]:
            if row.get("customer"):
                sales_area_by_customer[row["customer"]].append(row)
        descriptions_by_product = {row["product"]: row.get("productDescription", row["product"]) for row in rows_by_table["product_descriptions"] if row.get("product")}

        for row in rows_by_table["business_partners"]:
            customer_id = row.get("customer")
            if not customer_id:
                continue
            metadata = {
                **row,
                "address": addresses_by_bp.get(row["businessPartner"], {}),
                "company_assignment": company_by_customer.get(customer_id, {}),
                "sales_area_assignments": sales_area_by_customer.get(customer_id, []),
            }
            self._add_node(nodes, node_id=f"customer:{customer_id}", node_type="customer", label=row.get("businessPartnerFullName") or row.get("businessPartnerName") or customer_id, source_table="business_partners", entity_key=customer_id, metadata=metadata)
            address = addresses_by_bp.get(row["businessPartner"])
            if address:
                address_label = ", ".join(part for part in [address.get("streetName"), address.get("cityName"), address.get("country")] if part)
                self._add_node(nodes, node_id=f"address:{address['addressId']}", node_type="address", label=address_label or address["addressId"], source_table="business_partner_addresses", entity_key=address["addressId"], metadata=address)
                self._add_edge(edges, source_node_id=f"customer:{customer_id}", target_node_id=f"address:{address['addressId']}", edge_type="has_address", source_table="business_partner_addresses", evidence={"customer": customer_id, "addressId": address["addressId"]})

        for row in rows_by_table["products"]:
            product_id = row["product"]
            metadata = {**row, "description": descriptions_by_product.get(product_id)}
            self._add_node(nodes, node_id=f"product:{product_id}", node_type="product", label=descriptions_by_product.get(product_id) or product_id, source_table="products", entity_key=product_id, metadata=metadata)

        for row in rows_by_table["plants"]:
            plant_id = row["plant"]
            self._add_node(nodes, node_id=f"plant:{plant_id}", node_type="plant", label=row.get("plantName") or plant_id, source_table="plants", entity_key=plant_id, metadata=row)

        for row in rows_by_table["product_plants"]:
            if row.get("product") and row.get("plant"):
                self._add_edge(edges, source_node_id=f"product:{row['product']}", target_node_id=f"plant:{row['plant']}", edge_type="available_at_plant", source_table="product_plants", evidence=row, inferred=True, confidence=0.95)

        for row in rows_by_table["product_storage_locations"]:
            product_id = row.get("product")
            plant_id = row.get("plant")
            storage_location = row.get("storageLocation")
            if not (product_id and plant_id and storage_location):
                continue
            storage_node_id = f"storage_location:{plant_id}:{storage_location}"
            self._add_node(nodes, node_id=storage_node_id, node_type="storage_location", label=f"{plant_id}:{storage_location}", source_table="product_storage_locations", entity_key=f"{plant_id}:{storage_location}", metadata=row)
            self._add_edge(edges, source_node_id=f"product:{product_id}", target_node_id=storage_node_id, edge_type="stored_in", source_table="product_storage_locations", evidence=row, inferred=True, confidence=0.9)
            self._add_edge(edges, source_node_id=f"plant:{plant_id}", target_node_id=storage_node_id, edge_type="contains_storage_location", source_table="product_storage_locations", evidence=row, inferred=True, confidence=0.9)

        sales_items_by_order = defaultdict(list)
        for row in rows_by_table["sales_order_items"]:
            sales_items_by_order[row["salesOrder"]].append(row)
            item_node_id = f"sales_order_item:{row['salesOrder']}:{self._normalize_item(row['salesOrderItem'])}"
            self._add_node(nodes, node_id=item_node_id, node_type="sales_order_item", label=f"{row['salesOrder']}/{self._normalize_item(row['salesOrderItem'])}", source_table="sales_order_items", entity_key=f"{row['salesOrder']}:{self._normalize_item(row['salesOrderItem'])}", metadata=row)
            self._add_edge(edges, source_node_id=f"sales_order:{row['salesOrder']}", target_node_id=item_node_id, edge_type="has_item", source_table="sales_order_items", evidence={"salesOrder": row["salesOrder"], "salesOrderItem": row["salesOrderItem"]})
            if row.get("material"):
                self._add_edge(edges, source_node_id=item_node_id, target_node_id=f"product:{row['material']}", edge_type="orders_product", source_table="sales_order_items", evidence={"material": row["material"]})
            if row.get("productionPlant"):
                self._add_edge(edges, source_node_id=item_node_id, target_node_id=f"plant:{row['productionPlant']}", edge_type="planned_at_plant", source_table="sales_order_items", evidence={"plant": row["productionPlant"]})
            if row.get("storageLocation") and row.get("productionPlant"):
                self._add_edge(edges, source_node_id=item_node_id, target_node_id=f"storage_location:{row['productionPlant']}:{row['storageLocation']}", edge_type="planned_from_storage_location", source_table="sales_order_items", evidence={"plant": row["productionPlant"], "storageLocation": row["storageLocation"]}, inferred=True, confidence=0.85)

        schedules_by_item = defaultdict(list)
        for row in rows_by_table["sales_order_schedule_lines"]:
            key = f"{row['salesOrder']}:{self._normalize_item(row['salesOrderItem'])}"
            schedules_by_item[key].append(row)

        for row in rows_by_table["sales_order_headers"]:
            order_id = row["salesOrder"]
            order_metadata = {
                **row,
                "items": sales_items_by_order.get(order_id, []),
                "schedule_lines": [schedule for item in sales_items_by_order.get(order_id, []) for schedule in schedules_by_item.get(f"{order_id}:{self._normalize_item(item['salesOrderItem'])}", [])],
            }
            self._add_node(nodes, node_id=f"sales_order:{order_id}", node_type="sales_order", label=order_id, source_table="sales_order_headers", entity_key=order_id, metadata=order_metadata)
            if row.get("soldToParty"):
                self._add_edge(edges, source_node_id=f"customer:{row['soldToParty']}", target_node_id=f"sales_order:{order_id}", edge_type="placed_order", source_table="sales_order_headers", evidence={"customer": row["soldToParty"], "salesOrder": order_id})

        delivery_items_by_delivery = defaultdict(list)
        for row in rows_by_table["outbound_delivery_items"]:
            delivery_id = row["deliveryDocument"]
            item_key = self._normalize_item(row["deliveryDocumentItem"])
            item_node_id = f"delivery_item:{delivery_id}:{item_key}"
            delivery_items_by_delivery[delivery_id].append(row)
            self._add_node(nodes, node_id=item_node_id, node_type="delivery_item", label=f"{delivery_id}/{item_key}", source_table="outbound_delivery_items", entity_key=f"{delivery_id}:{item_key}", metadata=row)
            self._add_edge(edges, source_node_id=f"delivery:{delivery_id}", target_node_id=item_node_id, edge_type="has_item", source_table="outbound_delivery_items", evidence={"deliveryDocument": delivery_id, "deliveryDocumentItem": row["deliveryDocumentItem"]})
            if row.get("plant"):
                self._add_edge(edges, source_node_id=item_node_id, target_node_id=f"plant:{row['plant']}", edge_type="shipped_from_plant", source_table="outbound_delivery_items", evidence={"plant": row["plant"]})
            if row.get("referenceSdDocument"):
                order_node_id = f"sales_order:{row['referenceSdDocument']}"
                order_item_node_id = f"sales_order_item:{row['referenceSdDocument']}:{self._normalize_item(row['referenceSdDocumentItem'])}"
                self._add_edge(edges, source_node_id=order_node_id, target_node_id=f"delivery:{delivery_id}", edge_type="fulfilled_by_delivery", source_table="outbound_delivery_items", evidence=row)
                self._add_edge(edges, source_node_id=order_item_node_id, target_node_id=item_node_id, edge_type="fulfilled_by_delivery_item", source_table="outbound_delivery_items", evidence=row)

        for row in rows_by_table["outbound_delivery_headers"]:
            delivery_id = row["deliveryDocument"]
            metadata = {**row, "items": delivery_items_by_delivery.get(delivery_id, [])}
            self._add_node(nodes, node_id=f"delivery:{delivery_id}", node_type="delivery", label=delivery_id, source_table="outbound_delivery_headers", entity_key=delivery_id, metadata=metadata)

        billing_items_by_document = defaultdict(list)
        for row in rows_by_table["billing_document_items"]:
            billing_id = row["billingDocument"]
            item_key = self._normalize_item(row["billingDocumentItem"])
            item_node_id = f"billing_item:{billing_id}:{item_key}"
            billing_items_by_document[billing_id].append(row)
            self._add_node(nodes, node_id=item_node_id, node_type="billing_item", label=f"{billing_id}/{item_key}", source_table="billing_document_items", entity_key=f"{billing_id}:{item_key}", metadata=row)
            self._add_edge(edges, source_node_id=f"billing_document:{billing_id}", target_node_id=item_node_id, edge_type="has_item", source_table="billing_document_items", evidence={"billingDocument": billing_id, "billingDocumentItem": row["billingDocumentItem"]})
            if row.get("material"):
                self._add_edge(edges, source_node_id=item_node_id, target_node_id=f"product:{row['material']}", edge_type="bills_product", source_table="billing_document_items", evidence={"material": row["material"]})
            if row.get("referenceSdDocument"):
                delivery_node_id = f"delivery:{row['referenceSdDocument']}"
                delivery_item_node_id = f"delivery_item:{row['referenceSdDocument']}:{self._normalize_item(row['referenceSdDocumentItem'])}"
                self._add_edge(edges, source_node_id=delivery_node_id, target_node_id=f"billing_document:{billing_id}", edge_type="billed_by", source_table="billing_document_items", evidence=row, inferred=True, confidence=0.98)
                self._add_edge(edges, source_node_id=delivery_item_node_id, target_node_id=item_node_id, edge_type="billed_by_item", source_table="billing_document_items", evidence=row, inferred=True, confidence=0.98)

        journal_by_accounting_document = defaultdict(list)
        for row in rows_by_table["journal_entry_items_accounts_receivable"]:
            accounting_doc = row["accountingDocument"]
            journal_by_accounting_document[accounting_doc].append(row)

        payments_by_accounting_document = defaultdict(list)
        for row in rows_by_table["payments_accounts_receivable"]:
            accounting_doc = row["accountingDocument"]
            payments_by_accounting_document[accounting_doc].append(row)

        cancellation_targets = {row["billingDocument"]: row.get("cancelledBillingDocument") for row in rows_by_table["billing_document_headers"] if row.get("billingDocumentIsCancelled") == "X"}

        for row in rows_by_table["billing_document_headers"]:
            billing_id = row["billingDocument"]
            metadata = {
                **row,
                "items": billing_items_by_document.get(billing_id, []),
                "journal_entries": journal_by_accounting_document.get(row.get("accountingDocument"), []),
                "payment_records": payments_by_accounting_document.get(row.get("accountingDocument"), []),
            }
            self._add_node(nodes, node_id=f"billing_document:{billing_id}", node_type="billing_document", label=billing_id, source_table="billing_document_headers", entity_key=billing_id, metadata=metadata)
            if row.get("soldToParty"):
                self._add_edge(edges, source_node_id=f"customer:{row['soldToParty']}", target_node_id=f"billing_document:{billing_id}", edge_type="billed_customer", source_table="billing_document_headers", evidence={"customer": row["soldToParty"], "billingDocument": billing_id})
            if row.get("accountingDocument"):
                self._add_node(nodes, node_id=f"accounting_document:{row['accountingDocument']}", node_type="accounting_document", label=row["accountingDocument"], source_table="journal_entry_items_accounts_receivable", entity_key=row["accountingDocument"], metadata={"billingDocument": billing_id, "journal_entries": journal_by_accounting_document.get(row["accountingDocument"], []), "payment_records": payments_by_accounting_document.get(row["accountingDocument"], [])})
                self._add_edge(edges, source_node_id=f"billing_document:{billing_id}", target_node_id=f"accounting_document:{row['accountingDocument']}", edge_type="posted_to_accounting", source_table="billing_document_headers", evidence={"billingDocument": billing_id, "accountingDocument": row["accountingDocument"], "fiscalYear": row.get("fiscalYear"), "companyCode": row.get("companyCode")})
            cancelled_document = cancellation_targets.get(billing_id)
            if cancelled_document:
                self._add_edge(edges, source_node_id=f"billing_document:{billing_id}", target_node_id=f"billing_document:{cancelled_document}", edge_type="cancels", source_table="billing_document_headers", evidence={"billingDocument": billing_id, "cancelledBillingDocument": cancelled_document})

        for accounting_doc, rows in journal_by_accounting_document.items():
            first_row = rows[0]
            if first_row.get("customer"):
                self._add_edge(edges, source_node_id=f"accounting_document:{accounting_doc}", target_node_id=f"customer:{first_row['customer']}", edge_type="open_item_for_customer", source_table="journal_entry_items_accounts_receivable", evidence={"accountingDocument": accounting_doc, "customer": first_row["customer"]})

        return list(nodes.values()), list(edges.values())

    def _deliveries_for_order(self, rows_by_table: dict[str, list[dict[str, Any]]], order_id: str) -> set[str]:
        return {row["deliveryDocument"] for row in rows_by_table["outbound_delivery_items"] if row.get("referenceSdDocument") == order_id}

    def _add_node(self, nodes: dict[str, NodeRecord], *, node_id: str, node_type: str, label: str, source_table: str, entity_key: str, metadata: dict[str, Any]) -> None:
        if not node_id:
            return
        nodes[node_id] = NodeRecord(node_id=node_id, node_type=node_type, label=label or entity_key, source_table=source_table, entity_key=entity_key, metadata=metadata)

    def _add_edge(self, edges: dict[str, EdgeRecord], *, source_node_id: str, target_node_id: str, edge_type: str, source_table: str, evidence: dict[str, Any], inferred: bool = False, confidence: float = 1.0) -> None:
        if not source_node_id or not target_node_id:
            return
        edge_id = self._hash_id(source_node_id, target_node_id, edge_type, source_table, json.dumps(evidence, sort_keys=True, default=str))
        edges[edge_id] = EdgeRecord(edge_id=edge_id, source_node_id=source_node_id, target_node_id=target_node_id, edge_type=edge_type, source_table=source_table, evidence=evidence, inferred=inferred, confidence=confidence)

    @staticmethod
    def _hash_id(*parts: str) -> str:
        return hashlib.sha1("::".join(parts).encode("utf-8")).hexdigest()

    @staticmethod
    def _all_columns(rows: list[dict[str, Any]]) -> list[str]:
        columns = []
        seen = set()
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    columns.append(key)
        return columns

    @staticmethod
    def _normalize_item(value: Any) -> str:
        text = "" if value is None else str(value)
        return text.lstrip("0") or "0"

    @staticmethod
    def _to_sql_value(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=str)
        return str(value)
