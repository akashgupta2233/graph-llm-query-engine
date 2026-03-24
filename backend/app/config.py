from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_DIR = Path(__file__).resolve().parents[1]
DATASET_DIR = REPO_ROOT / "sap-o2c-data"
DATA_DIR = BACKEND_DIR / "data"
DB_PATH = DATA_DIR / "graph_demo.db"

ALLOWED_RAW_TABLES = {
    "billing_document_cancellations",
    "billing_document_headers",
    "billing_document_items",
    "business_partners",
    "business_partner_addresses",
    "customer_company_assignments",
    "customer_sales_area_assignments",
    "journal_entry_items_accounts_receivable",
    "outbound_delivery_headers",
    "outbound_delivery_items",
    "payments_accounts_receivable",
    "plants",
    "products",
    "product_descriptions",
    "product_plants",
    "product_storage_locations",
    "sales_order_headers",
    "sales_order_items",
    "sales_order_schedule_lines",
}

SQL_ALLOWED_TABLES = ALLOWED_RAW_TABLES | {
    "graph_nodes",
    "graph_edges",
    "dataset_profile",
}

REFUSAL_MESSAGE = "This system is designed to answer questions related to the provided dataset only."

