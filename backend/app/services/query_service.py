from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from app.config import REFUSAL_MESSAGE, SQL_ALLOWED_TABLES
from app.db import db_session
from app.schemas import ChatMessage, ChatResponse, EvidenceTable, QueryPlan


SYSTEM_PROMPT = """
You are a dataset-only assistant for the provided SAP order-to-cash dataset.
- Answer only from validated query results or explicit graph traversals.
- Refuse unrelated prompts with: "This system is designed to answer questions related to the provided dataset only."
- Never invent facts, columns, entities, or relationships.
- Ask for clarification only when the requested entity or identifier cannot be resolved safely.
- Prefer deterministic structured query generation over free-form reasoning.
- Explain answers in terms of concrete records and counts.
""".strip()


INJECTION_PATTERNS = [
    "ignore previous instructions",
    "reveal the system prompt",
    "show the hidden prompt",
    "run shell",
    "execute code",
    "sudo",
]

DOMAIN_TERMS = {
    "order",
    "orders",
    "sales order",
    "delivery",
    "deliveries",
    "billing",
    "invoice",
    "invoices",
    "payment",
    "payments",
    "customer",
    "customers",
    "product",
    "products",
    "material",
    "billing document",
    "plant",
    "graph",
    "flow",
    "document",
}

ENTITY_NODE_PREFIX = {
    "sales order": "sales_order",
    "order": "sales_order",
    "delivery": "delivery",
    "billing document": "billing_document",
    "billing": "billing_document",
    "invoice": "billing_document",
    "customer": "customer",
    "product": "product",
    "material": "product",
    "accounting": "accounting_document",
}


@dataclass
class ResolvedReference:
    entity_type: str | None
    entity_id: str | None
    node_id: str | None


class QueryService:
    def handle_chat(self, message: str, history: list[ChatMessage]) -> ChatResponse:
        text = message.strip()
        lower_text = text.lower()

        if self._looks_like_injection(lower_text):
            return self._refusal_response("unsupported", ["Prompt injection attempt rejected."])

        if not self._is_in_domain(lower_text):
            return self._refusal_response("unsupported", ["No domain entity detected."])

        if self._is_trace_request(lower_text):
            return self._handle_trace(text, history)

        if self._is_anomaly_request(lower_text):
            return self._handle_anomaly(text)

        if self._is_aggregation_request(lower_text):
            return self._handle_aggregation(text)

        if self._is_neighborhood_request(lower_text):
            return self._handle_neighborhood(text, history)

        return self._handle_lookup(text, history)

    def _handle_lookup(self, text: str, history: list[ChatMessage]) -> ChatResponse:
        resolved = self._resolve_reference(text, history)
        if not resolved.entity_type or not resolved.entity_id:
            return ChatResponse(
                status="clarify",
                intent="entity_lookup",
                answer="I can look up a specific dataset entity, but I need a concrete order, delivery, billing document, customer, or product identifier.",
                query_plan=QueryPlan(intent="entity_lookup", notes=["Missing concrete identifier."]),
            )

        table_name, key_column, node_prefix = self._entity_sql_mapping(resolved.entity_type)
        sql = f'SELECT * FROM "{table_name}" WHERE "{key_column}" = ? LIMIT 1'
        self._validate_sql(sql)
        with db_session() as conn:
            row = conn.execute(sql, (resolved.entity_id,)).fetchone()

        if not row:
            return ChatResponse(
                status="cannot_answer",
                intent="entity_lookup",
                answer=f"No matching {resolved.entity_type.replace('_', ' ')} was found for `{resolved.entity_id}` in the dataset.",
                query_plan=QueryPlan(intent="entity_lookup", sql=sql, params=[resolved.entity_id]),
            )

        payload = dict(row)
        highlight_node_ids = [f"{node_prefix}:{resolved.entity_id}"]
        evidence = [EvidenceTable(title=f"{resolved.entity_type.replace('_', ' ').title()} details", columns=list(payload.keys()), rows=[payload])]
        answer = self._lookup_answer(resolved.entity_type, payload)
        return ChatResponse(
            status="answered",
            intent="entity_lookup",
            answer=answer,
            query_plan=QueryPlan(intent="entity_lookup", sql=sql, params=[resolved.entity_id]),
            evidence=evidence,
            focus_node_id=highlight_node_ids[0],
            highlight_node_ids=highlight_node_ids,
            context={"entity_type": resolved.entity_type, "entity_id": resolved.entity_id, "node_id": highlight_node_ids[0]},
        )

    def _handle_aggregation(self, text: str) -> ChatResponse:
        lower_text = text.lower()
        if "product" in lower_text or "material" in lower_text:
            sql = """
                SELECT
                    bdi.material AS product,
                    COALESCE(pd.productDescription, bdi.material) AS productDescription,
                    COUNT(DISTINCT bdi.billingDocument) AS billingDocumentCount,
                    SUM(CAST(bdi.netAmount AS REAL)) AS totalNetAmount
                FROM billing_document_items bdi
                LEFT JOIN product_descriptions pd ON pd.product = bdi.material
                GROUP BY bdi.material, COALESCE(pd.productDescription, bdi.material)
                ORDER BY billingDocumentCount DESC, totalNetAmount DESC
                LIMIT 10
            """
            self._validate_sql(sql)
            with db_session() as conn:
                rows = [dict(row) for row in conn.execute(sql).fetchall()]
            if not rows:
                return self._cannot_answer("aggregation", sql, "No billing-document/product intersections were found in the dataset.")
            top = rows[0]
            answer = (
                f"The most widely billed product is `{top['product']}` "
                f"({top['productDescription']}) with {int(top['billingDocumentCount'])} billing documents "
                f"and total billed net amount {top['totalNetAmount']:.2f}."
            )
            return ChatResponse(
                status="answered",
                intent="aggregation",
                answer=answer,
                query_plan=QueryPlan(intent="aggregation", sql=self._compact_sql(sql)),
                evidence=[EvidenceTable(title="Top products by billing-document coverage", columns=list(rows[0].keys()), rows=rows)],
                focus_node_id=f"product:{top['product']}",
                highlight_node_ids=[f"product:{row['product']}" for row in rows[:5]],
                context={"entity_type": "product", "entity_id": top["product"], "node_id": f"product:{top['product']}"},
            )

        if "customer" in lower_text and ("incomplete" in lower_text or "broken" in lower_text):
            sql = """
                WITH order_flow AS (
                    SELECT
                        soh.salesOrder,
                        soh.soldToParty AS customer,
                        COUNT(DISTINCT odi.deliveryDocument) AS deliveryCount,
                        COUNT(DISTINCT bdi.billingDocument) AS billingCount
                    FROM sales_order_headers soh
                    LEFT JOIN outbound_delivery_items odi
                        ON odi.referenceSdDocument = soh.salesOrder
                    LEFT JOIN billing_document_items bdi
                        ON bdi.referenceSdDocument = odi.deliveryDocument
                    GROUP BY soh.salesOrder, soh.soldToParty
                )
                SELECT
                    customer,
                    COUNT(*) AS orderCount,
                    SUM(CASE WHEN deliveryCount = 0 OR billingCount = 0 THEN 1 ELSE 0 END) AS incompleteOrderCount
                FROM order_flow
                GROUP BY customer
                ORDER BY incompleteOrderCount DESC, orderCount DESC
                LIMIT 10
            """
            self._validate_sql(sql)
            with db_session() as conn:
                rows = [dict(row) for row in conn.execute(sql).fetchall()]
            if not rows:
                return self._cannot_answer("aggregation", sql, "No customer flow coverage could be calculated.")
            top = rows[0]
            answer = (
                f"Customer `{top['customer']}` has the highest number of incomplete flows in this snapshot: "
                f"{int(top['incompleteOrderCount'])} incomplete orders out of {int(top['orderCount'])} total orders."
            )
            return ChatResponse(
                status="answered",
                intent="aggregation",
                answer=answer,
                query_plan=QueryPlan(intent="aggregation", sql=self._compact_sql(sql)),
                evidence=[EvidenceTable(title="Customers with the most incomplete flows", columns=list(rows[0].keys()), rows=rows)],
                focus_node_id=f"customer:{top['customer']}",
                highlight_node_ids=[f"customer:{row['customer']}" for row in rows[:5]],
                context={"entity_type": "customer", "entity_id": top["customer"], "node_id": f"customer:{top['customer']}"},
            )

        return ChatResponse(
            status="clarify",
            intent="aggregation",
            answer="I support grounded aggregations for products, customers, document counts, and flow coverage. Please ask with a concrete business entity or metric.",
            query_plan=QueryPlan(intent="aggregation", notes=["Aggregation request did not match a supported metric."]),
        )

    def _handle_trace(self, text: str, history: list[ChatMessage]) -> ChatResponse:
        resolved = self._resolve_reference(text, history)
        if not resolved.entity_type or not resolved.entity_id:
            return ChatResponse(
                status="clarify",
                intent="trace",
                answer="I can trace a sales order, delivery, or billing document flow, but I need the specific identifier.",
                query_plan=QueryPlan(intent="trace", notes=["Missing identifier for trace request."]),
            )

        sql_map = {
            "billing_document": """
                SELECT DISTINCT
                    bdh.billingDocument,
                    bdh.soldToParty,
                    bdi.referenceSdDocument AS deliveryDocument,
                    odi.referenceSdDocument AS salesOrder,
                    bdh.accountingDocument,
                    jei.clearingAccountingDocument,
                    jei.clearingDate
                FROM billing_document_headers bdh
                LEFT JOIN billing_document_items bdi
                    ON bdi.billingDocument = bdh.billingDocument
                LEFT JOIN outbound_delivery_items odi
                    ON odi.deliveryDocument = bdi.referenceSdDocument
                LEFT JOIN journal_entry_items_accounts_receivable jei
                    ON jei.accountingDocument = bdh.accountingDocument
                WHERE bdh.billingDocument = ?
            """,
            "sales_order": """
                SELECT DISTINCT
                    soh.salesOrder,
                    soh.soldToParty,
                    odi.deliveryDocument,
                    bdi.billingDocument,
                    bdh.accountingDocument,
                    jei.clearingAccountingDocument,
                    jei.clearingDate
                FROM sales_order_headers soh
                LEFT JOIN outbound_delivery_items odi
                    ON odi.referenceSdDocument = soh.salesOrder
                LEFT JOIN billing_document_items bdi
                    ON bdi.referenceSdDocument = odi.deliveryDocument
                LEFT JOIN billing_document_headers bdh
                    ON bdh.billingDocument = bdi.billingDocument
                LEFT JOIN journal_entry_items_accounts_receivable jei
                    ON jei.accountingDocument = bdh.accountingDocument
                WHERE soh.salesOrder = ?
            """,
            "delivery": """
                SELECT DISTINCT
                    odh.deliveryDocument,
                    odi.referenceSdDocument AS salesOrder,
                    bdi.billingDocument,
                    bdh.accountingDocument,
                    jei.clearingAccountingDocument,
                    jei.clearingDate
                FROM outbound_delivery_headers odh
                LEFT JOIN outbound_delivery_items odi
                    ON odi.deliveryDocument = odh.deliveryDocument
                LEFT JOIN billing_document_items bdi
                    ON bdi.referenceSdDocument = odh.deliveryDocument
                LEFT JOIN billing_document_headers bdh
                    ON bdh.billingDocument = bdi.billingDocument
                LEFT JOIN journal_entry_items_accounts_receivable jei
                    ON jei.accountingDocument = bdh.accountingDocument
                WHERE odh.deliveryDocument = ?
            """,
        }
        sql = sql_map.get(resolved.entity_type)
        if not sql:
            return ChatResponse(
                status="cannot_answer",
                intent="trace",
                answer="Flow tracing is supported for sales orders, deliveries, and billing documents only.",
                query_plan=QueryPlan(intent="trace", notes=[f"Unsupported trace entity: {resolved.entity_type}"]),
            )
        self._validate_sql(sql)
        with db_session() as conn:
            rows = [dict(row) for row in conn.execute(sql, (resolved.entity_id,)).fetchall()]

        if not rows:
            return self._cannot_answer("trace", sql, f"No downstream or upstream flow records were found for `{resolved.entity_id}`.")

        first = rows[0]
        highlight_nodes = self._trace_highlight_nodes(resolved.entity_type, rows, resolved.entity_id)
        answer = self._trace_answer(resolved.entity_type, resolved.entity_id, first)
        return ChatResponse(
            status="answered",
            intent="trace",
            answer=answer,
            query_plan=QueryPlan(intent="trace", sql=self._compact_sql(sql), params=[resolved.entity_id]),
            evidence=[EvidenceTable(title="Trace result", columns=list(rows[0].keys()), rows=rows[:20])],
            focus_node_id=resolved.node_id,
            highlight_node_ids=highlight_nodes,
            context={"entity_type": resolved.entity_type, "entity_id": resolved.entity_id, "node_id": resolved.node_id},
        )

    def _handle_anomaly(self, text: str) -> ChatResponse:
        lower_text = text.lower()
        if "delivered" in lower_text and "not billed" in lower_text:
            sql = """
                SELECT
                    soh.salesOrder,
                    soh.soldToParty AS customer,
                    COUNT(DISTINCT odi.deliveryDocument) AS deliveryCount
                FROM sales_order_headers soh
                JOIN outbound_delivery_items odi
                    ON odi.referenceSdDocument = soh.salesOrder
                LEFT JOIN billing_document_items bdi
                    ON bdi.referenceSdDocument = odi.deliveryDocument
                WHERE bdi.billingDocument IS NULL
                GROUP BY soh.salesOrder, soh.soldToParty
                ORDER BY soh.salesOrder
                LIMIT 50
            """
            self._validate_sql(sql)
            with db_session() as conn:
                rows = [dict(row) for row in conn.execute(sql).fetchall()]
            if not rows:
                return self._cannot_answer("anomaly", sql, "No delivered-but-not-billed sales orders were found.")
            ids = [row["salesOrder"] for row in rows]
            answer = f"I found {len(rows)} delivered sales orders with no downstream billing document: {', '.join(ids[:10])}."
            return ChatResponse(
                status="answered",
                intent="anomaly",
                answer=answer,
                query_plan=QueryPlan(intent="anomaly", sql=self._compact_sql(sql)),
                evidence=[EvidenceTable(title="Delivered orders without billing", columns=list(rows[0].keys()), rows=rows)],
                focus_node_id=f"sales_order:{ids[0]}",
                highlight_node_ids=[f"sales_order:{order_id}" for order_id in ids[:10]],
                context={"entity_type": "sales_order", "entity_id": ids[0], "node_id": f"sales_order:{ids[0]}"},
            )

        if "deliver" in lower_text and ("not linked" in lower_text or "no invoice" in lower_text or "not invoiced" in lower_text):
            sql = """
                SELECT
                    odh.deliveryDocument,
                    COUNT(DISTINCT odi.referenceSdDocument) AS salesOrderCount
                FROM outbound_delivery_headers odh
                LEFT JOIN outbound_delivery_items odi
                    ON odi.deliveryDocument = odh.deliveryDocument
                LEFT JOIN billing_document_items bdi
                    ON bdi.referenceSdDocument = odh.deliveryDocument
                WHERE bdi.billingDocument IS NULL
                GROUP BY odh.deliveryDocument
                ORDER BY odh.deliveryDocument
                LIMIT 50
            """
            self._validate_sql(sql)
            with db_session() as conn:
                rows = [dict(row) for row in conn.execute(sql).fetchall()]
            if not rows:
                return self._cannot_answer("anomaly", sql, "Every delivery in the dataset is linked to at least one billing document.")
            answer = f"I found {len(rows)} deliveries without linked billing documents."
            return ChatResponse(
                status="answered",
                intent="anomaly",
                answer=answer,
                query_plan=QueryPlan(intent="anomaly", sql=self._compact_sql(sql)),
                evidence=[EvidenceTable(title="Deliveries without invoices", columns=list(rows[0].keys()), rows=rows)],
                focus_node_id=f"delivery:{rows[0]['deliveryDocument']}",
                highlight_node_ids=[f"delivery:{row['deliveryDocument']}" for row in rows[:10]],
                context={"entity_type": "delivery", "entity_id": rows[0]["deliveryDocument"], "node_id": f"delivery:{rows[0]['deliveryDocument']}"},
            )

        if "no downstream payment" in lower_text or ("invoice" in lower_text and "no payments" in lower_text):
            sql = """
                SELECT
                    bdh.billingDocument,
                    bdh.accountingDocument,
                    jei.clearingAccountingDocument,
                    jei.clearingDate
                FROM billing_document_headers bdh
                LEFT JOIN journal_entry_items_accounts_receivable jei
                    ON jei.accountingDocument = bdh.accountingDocument
                WHERE jei.clearingAccountingDocument IS NULL
                ORDER BY bdh.billingDocument
                LIMIT 50
            """
            self._validate_sql(sql)
            with db_session() as conn:
                rows = [dict(row) for row in conn.execute(sql).fetchall()]
            if not rows:
                return self._cannot_answer("anomaly", sql, "All billed accounting documents appear to be cleared in the available AR journal data.")
            answer = (
                "The dataset does not expose direct invoiceReference values in the payments table, "
                "so I used AR clearing status as the payment proxy. "
                f"{len(rows)} billing documents have no downstream clearing document in the available journal data."
            )
            return ChatResponse(
                status="answered",
                intent="anomaly",
                answer=answer,
                query_plan=QueryPlan(intent="anomaly", sql=self._compact_sql(sql), notes=["Payment coverage is inferred from clearing status."]),
                evidence=[EvidenceTable(title="Billing documents without downstream clearing", columns=list(rows[0].keys()), rows=rows)],
                focus_node_id=f"billing_document:{rows[0]['billingDocument']}",
                highlight_node_ids=[f"billing_document:{row['billingDocument']}" for row in rows[:10]],
                context={"entity_type": "billing_document", "entity_id": rows[0]["billingDocument"], "node_id": f"billing_document:{rows[0]['billingDocument']}"},
            )

        return ChatResponse(
            status="clarify",
            intent="anomaly",
            answer="I support anomaly checks for delivered-but-not-billed orders, deliveries without invoices, and billed documents without downstream clearing.",
            query_plan=QueryPlan(intent="anomaly", notes=["Anomaly request did not match a supported rule."]),
        )

    def _handle_neighborhood(self, text: str, history: list[ChatMessage]) -> ChatResponse:
        resolved = self._resolve_reference(text, history)
        if not resolved.node_id:
            return ChatResponse(
                status="clarify",
                intent="relationship_exploration",
                answer="I can explore the neighborhood of a specific order, delivery, billing document, customer, or product when you provide the identifier.",
                query_plan=QueryPlan(intent="relationship_exploration", notes=["Missing concrete node for graph exploration."]),
            )

        return ChatResponse(
            status="answered",
            intent="relationship_exploration",
            answer=f"I centered the graph on `{resolved.entity_id}` so you can inspect its direct relationships in the visualization.",
            query_plan=QueryPlan(intent="relationship_exploration", notes=["Graph neighborhood request is fulfilled via graph slice endpoint."]),
            focus_node_id=resolved.node_id,
            highlight_node_ids=[resolved.node_id],
            context={"entity_type": resolved.entity_type, "entity_id": resolved.entity_id, "node_id": resolved.node_id},
        )

    def _resolve_reference(self, text: str, history: list[ChatMessage]) -> ResolvedReference:
        lower_text = text.lower()
        entity_type = None
        for token, mapped in ENTITY_NODE_PREFIX.items():
            if token in lower_text:
                entity_type = mapped
                break

        explicit_id = self._extract_identifier(text)
        if entity_type and explicit_id:
            return ResolvedReference(entity_type=entity_type, entity_id=explicit_id, node_id=f"{entity_type}:{explicit_id}")

        if ("this" in lower_text or "that" in lower_text or "those" in lower_text) and history:
            for message in reversed(history):
                if message.role == "assistant" and message.context:
                    ctx = message.context
                    return ResolvedReference(entity_type=ctx.get("entity_type"), entity_id=ctx.get("entity_id"), node_id=ctx.get("node_id"))

        if entity_type and not explicit_id:
            return ResolvedReference(entity_type=entity_type, entity_id=None, node_id=None)

        return ResolvedReference(entity_type=None, entity_id=None, node_id=None)

    def _entity_sql_mapping(self, entity_type: str) -> tuple[str, str, str]:
        mapping = {
            "sales_order": ("sales_order_headers", "salesOrder", "sales_order"),
            "delivery": ("outbound_delivery_headers", "deliveryDocument", "delivery"),
            "billing_document": ("billing_document_headers", "billingDocument", "billing_document"),
            "customer": ("business_partners", "customer", "customer"),
            "product": ("products", "product", "product"),
            "accounting_document": ("journal_entry_items_accounts_receivable", "accountingDocument", "accounting_document"),
        }
        return mapping[entity_type]

    def _lookup_answer(self, entity_type: str, payload: dict[str, Any]) -> str:
        if entity_type == "sales_order":
            return f"Sales order `{payload['salesOrder']}` belongs to customer `{payload['soldToParty']}` for total net amount {payload['totalNetAmount']} {payload['transactionCurrency']}. Overall delivery status is `{payload['overallDeliveryStatus'] or 'blank'}`."
        if entity_type == "billing_document":
            return f"Billing document `{payload['billingDocument']}` is posted for customer `{payload['soldToParty']}` with total net amount {payload['totalNetAmount']} {payload['transactionCurrency']}. Accounting document is `{payload['accountingDocument']}`."
        if entity_type == "delivery":
            return f"Delivery `{payload['deliveryDocument']}` was created on `{payload['creationDate']}` with shipping point `{payload['shippingPoint']}` and goods movement status `{payload['overallGoodsMovementStatus']}`."
        if entity_type == "customer":
            return f"Customer `{payload['customer']}` maps to business partner `{payload['businessPartner']}` named `{payload['businessPartnerFullName']}`."
        if entity_type == "product":
            return f"Product `{payload['product']}` belongs to product group `{payload['productGroup']}` with base unit `{payload['baseUnit']}`."
        return f"Found dataset-backed details for `{payload}`."

    def _trace_answer(self, entity_type: str, entity_id: str, row: dict[str, Any]) -> str:
        if entity_type == "billing_document":
            return f"Billing document `{entity_id}` traces upstream to sales order `{row.get('salesOrder') or 'not found'}` through delivery `{row.get('deliveryDocument') or 'not found'}` and downstream to accounting document `{row.get('accountingDocument') or 'not found'}`. Clearing document is `{row.get('clearingAccountingDocument') or 'missing'}`."
        if entity_type == "sales_order":
            return f"Sales order `{entity_id}` belongs to customer `{row.get('soldToParty') or 'not found'}` and flows through delivery `{row.get('deliveryDocument') or 'missing'}`, billing document `{row.get('billingDocument') or 'missing'}`, and accounting document `{row.get('accountingDocument') or 'missing'}`."
        return f"Delivery `{entity_id}` traces upstream to sales order `{row.get('salesOrder') or 'not found'}` and downstream to billing document `{row.get('billingDocument') or 'missing'}` and accounting document `{row.get('accountingDocument') or 'missing'}`."

    def _trace_highlight_nodes(self, entity_type: str, rows: list[dict[str, Any]], entity_id: str) -> list[str]:
        highlights = {f"{entity_type}:{entity_id}"}
        for row in rows:
            for key, prefix in [("salesOrder", "sales_order"), ("deliveryDocument", "delivery"), ("billingDocument", "billing_document"), ("soldToParty", "customer"), ("accountingDocument", "accounting_document")]:
                if row.get(key):
                    highlights.add(f"{prefix}:{row[key]}")
        return sorted(highlights)

    def _cannot_answer(self, intent: str, sql: str, message: str) -> ChatResponse:
        return ChatResponse(status="cannot_answer", intent=intent, answer=message, query_plan=QueryPlan(intent=intent, sql=self._compact_sql(sql)))

    def _refusal_response(self, intent: str, notes: list[str]) -> ChatResponse:
        return ChatResponse(status="refused", intent=intent, answer=REFUSAL_MESSAGE, query_plan=QueryPlan(intent=intent, notes=notes))

    def _is_in_domain(self, lower_text: str) -> bool:
        return any(term in lower_text for term in DOMAIN_TERMS)

    def _looks_like_injection(self, lower_text: str) -> bool:
        return any(term in lower_text for term in INJECTION_PATTERNS)

    def _is_trace_request(self, lower_text: str) -> bool:
        return any(token in lower_text for token in ["trace", "flow", "path"])

    def _is_aggregation_request(self, lower_text: str) -> bool:
        return any(token in lower_text for token in ["highest", "most", "top", "count", "how many", "coverage", "compare"])

    def _is_anomaly_request(self, lower_text: str) -> bool:
        return any(
            token in lower_text
            for token in [
                "broken",
                "incomplete",
                "orphan",
                "missing",
                "not billed",
                "no invoice",
                "no payment",
                "downstream payment",
                "not linked",
            ]
        )

    def _is_neighborhood_request(self, lower_text: str) -> bool:
        return any(token in lower_text for token in ["neighborhood", "connect", "connected", "graph", "neighbors", "nearby"])

    @staticmethod
    def _extract_identifier(text: str) -> str | None:
        match = re.search(r"\b([A-Z]?[0-9]{5,}|[A-Z][0-9A-Z]{7,})\b", text)
        return match.group(1) if match else None

    @staticmethod
    def _compact_sql(sql: str) -> str:
        return re.sub(r"\s+", " ", sql).strip()

    def _validate_sql(self, sql: str) -> None:
        normalized = sql.strip().lower()
        if not (normalized.startswith("select") or normalized.startswith("with")):
            raise ValueError("Only SELECT queries are allowed.")
        blocked_terms = ["insert ", "update ", "delete ", "drop ", "alter ", "attach ", "pragma ", ";"]
        if any(term in normalized for term in blocked_terms):
            raise ValueError("Blocked SQL operation detected.")
        tables = set(re.findall(r"(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", normalized))
        if not tables.issubset(SQL_ALLOWED_TABLES):
            unexpected = ", ".join(sorted(tables - SQL_ALLOWED_TABLES))
            raise ValueError(f"Unexpected table reference: {unexpected}")
