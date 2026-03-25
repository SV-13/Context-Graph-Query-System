"""
Database module: Loads SAP O2C JSONL data into SQLite for structured querying.
SQLite is chosen for its zero-config deployment and excellent SQL support,
which pairs well with LLM-generated SQL queries.
"""

import sqlite3
import json
import os
from pathlib import Path

DB_PATH = os.environ.get("DB_PATH", "sap_o2c.db")
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "sap-o2c-data"))

# Mapping from folder name to table name and column definitions.
# We flatten nested JSON (e.g., time objects) during ingestion.
TABLE_SCHEMAS = {
    "sales_order_headers": {
        "table": "sales_order_headers",
        "columns": {
            "salesOrder": "TEXT PRIMARY KEY",
            "salesOrderType": "TEXT",
            "salesOrganization": "TEXT",
            "distributionChannel": "TEXT",
            "organizationDivision": "TEXT",
            "salesGroup": "TEXT",
            "salesOffice": "TEXT",
            "soldToParty": "TEXT",
            "creationDate": "TEXT",
            "createdByUser": "TEXT",
            "lastChangeDateTime": "TEXT",
            "totalNetAmount": "REAL",
            "overallDeliveryStatus": "TEXT",
            "overallOrdReltdBillgStatus": "TEXT",
            "overallSdDocReferenceStatus": "TEXT",
            "transactionCurrency": "TEXT",
            "pricingDate": "TEXT",
            "requestedDeliveryDate": "TEXT",
            "headerBillingBlockReason": "TEXT",
            "deliveryBlockReason": "TEXT",
            "incotermsClassification": "TEXT",
            "incotermsLocation1": "TEXT",
            "customerPaymentTerms": "TEXT",
            "totalCreditCheckStatus": "TEXT",
        },
    },
    "sales_order_items": {
        "table": "sales_order_items",
        "columns": {
            "salesOrder": "TEXT",
            "salesOrderItem": "TEXT",
            "salesOrderItemCategory": "TEXT",
            "material": "TEXT",
            "requestedQuantity": "REAL",
            "requestedQuantityUnit": "TEXT",
            "transactionCurrency": "TEXT",
            "netAmount": "REAL",
            "materialGroup": "TEXT",
            "productionPlant": "TEXT",
            "storageLocation": "TEXT",
            "salesDocumentRjcnReason": "TEXT",
            "itemBillingBlockReason": "TEXT",
        },
    },
    "sales_order_schedule_lines": {
        "table": "sales_order_schedule_lines",
        "columns": {
            "salesOrder": "TEXT",
            "salesOrderItem": "TEXT",
            "scheduleLine": "TEXT",
            "confirmedDeliveryDate": "TEXT",
            "orderQuantityUnit": "TEXT",
            "confdOrderQtyByMatlAvailCheck": "REAL",
        },
    },
    "outbound_delivery_headers": {
        "table": "outbound_delivery_headers",
        "columns": {
            "deliveryDocument": "TEXT PRIMARY KEY",
            "creationDate": "TEXT",
            "deliveryBlockReason": "TEXT",
            "hdrGeneralIncompletionStatus": "TEXT",
            "headerBillingBlockReason": "TEXT",
            "lastChangeDate": "TEXT",
            "overallGoodsMovementStatus": "TEXT",
            "overallPickingStatus": "TEXT",
            "overallProofOfDeliveryStatus": "TEXT",
            "shippingPoint": "TEXT",
            "actualGoodsMovementDate": "TEXT",
        },
    },
    "outbound_delivery_items": {
        "table": "outbound_delivery_items",
        "columns": {
            "deliveryDocument": "TEXT",
            "deliveryDocumentItem": "TEXT",
            "actualDeliveryQuantity": "REAL",
            "batch": "TEXT",
            "deliveryQuantityUnit": "TEXT",
            "itemBillingBlockReason": "TEXT",
            "lastChangeDate": "TEXT",
            "plant": "TEXT",
            "referenceSdDocument": "TEXT",
            "referenceSdDocumentItem": "TEXT",
            "storageLocation": "TEXT",
        },
    },
    "billing_document_headers": {
        "table": "billing_document_headers",
        "columns": {
            "billingDocument": "TEXT PRIMARY KEY",
            "billingDocumentType": "TEXT",
            "creationDate": "TEXT",
            "lastChangeDateTime": "TEXT",
            "billingDocumentDate": "TEXT",
            "billingDocumentIsCancelled": "INTEGER",
            "cancelledBillingDocument": "TEXT",
            "totalNetAmount": "REAL",
            "transactionCurrency": "TEXT",
            "companyCode": "TEXT",
            "fiscalYear": "TEXT",
            "accountingDocument": "TEXT",
            "soldToParty": "TEXT",
        },
    },
    "billing_document_items": {
        "table": "billing_document_items",
        "columns": {
            "billingDocument": "TEXT",
            "billingDocumentItem": "TEXT",
            "material": "TEXT",
            "billingQuantity": "REAL",
            "billingQuantityUnit": "TEXT",
            "netAmount": "REAL",
            "transactionCurrency": "TEXT",
            "referenceSdDocument": "TEXT",
            "referenceSdDocumentItem": "TEXT",
        },
    },
    "billing_document_cancellations": {
        "table": "billing_document_cancellations",
        "columns": {
            "billingDocument": "TEXT PRIMARY KEY",
            "billingDocumentType": "TEXT",
            "creationDate": "TEXT",
            "lastChangeDateTime": "TEXT",
            "billingDocumentDate": "TEXT",
            "billingDocumentIsCancelled": "INTEGER",
            "cancelledBillingDocument": "TEXT",
            "totalNetAmount": "REAL",
            "transactionCurrency": "TEXT",
            "companyCode": "TEXT",
            "fiscalYear": "TEXT",
            "accountingDocument": "TEXT",
            "soldToParty": "TEXT",
        },
    },
    "payments_accounts_receivable": {
        "table": "payments_accounts_receivable",
        "columns": {
            "companyCode": "TEXT",
            "fiscalYear": "TEXT",
            "accountingDocument": "TEXT",
            "accountingDocumentItem": "TEXT",
            "clearingDate": "TEXT",
            "clearingAccountingDocument": "TEXT",
            "clearingDocFiscalYear": "TEXT",
            "amountInTransactionCurrency": "REAL",
            "transactionCurrency": "TEXT",
            "amountInCompanyCodeCurrency": "REAL",
            "companyCodeCurrency": "TEXT",
            "customer": "TEXT",
            "invoiceReference": "TEXT",
            "invoiceReferenceFiscalYear": "TEXT",
            "salesDocument": "TEXT",
            "salesDocumentItem": "TEXT",
            "postingDate": "TEXT",
            "documentDate": "TEXT",
            "assignmentReference": "TEXT",
            "glAccount": "TEXT",
            "financialAccountType": "TEXT",
            "profitCenter": "TEXT",
            "costCenter": "TEXT",
        },
    },
    "journal_entry_items_accounts_receivable": {
        "table": "journal_entry_items",
        "columns": {
            "companyCode": "TEXT",
            "fiscalYear": "TEXT",
            "accountingDocument": "TEXT",
            "glAccount": "TEXT",
            "referenceDocument": "TEXT",
            "costCenter": "TEXT",
            "profitCenter": "TEXT",
            "transactionCurrency": "TEXT",
            "amountInTransactionCurrency": "REAL",
            "companyCodeCurrency": "TEXT",
            "amountInCompanyCodeCurrency": "REAL",
            "postingDate": "TEXT",
            "documentDate": "TEXT",
            "accountingDocumentType": "TEXT",
            "accountingDocumentItem": "TEXT",
            "assignmentReference": "TEXT",
            "lastChangeDateTime": "TEXT",
            "customer": "TEXT",
            "financialAccountType": "TEXT",
            "clearingDate": "TEXT",
            "clearingAccountingDocument": "TEXT",
            "clearingDocFiscalYear": "TEXT",
        },
    },
    "business_partners": {
        "table": "business_partners",
        "columns": {
            "businessPartner": "TEXT PRIMARY KEY",
            "customer": "TEXT",
            "businessPartnerCategory": "TEXT",
            "businessPartnerFullName": "TEXT",
            "businessPartnerGrouping": "TEXT",
            "businessPartnerName": "TEXT",
            "correspondenceLanguage": "TEXT",
            "createdByUser": "TEXT",
            "creationDate": "TEXT",
            "firstName": "TEXT",
            "formOfAddress": "TEXT",
            "industry": "TEXT",
            "lastChangeDate": "TEXT",
            "lastName": "TEXT",
            "organizationBpName1": "TEXT",
            "organizationBpName2": "TEXT",
            "businessPartnerIsBlocked": "INTEGER",
            "isMarkedForArchiving": "INTEGER",
        },
    },
    "business_partner_addresses": {
        "table": "business_partner_addresses",
        "columns": {
            "businessPartner": "TEXT",
            "addressId": "TEXT",
            "validityStartDate": "TEXT",
            "validityEndDate": "TEXT",
            "addressUuid": "TEXT",
            "addressTimeZone": "TEXT",
            "cityName": "TEXT",
            "country": "TEXT",
            "postalCode": "TEXT",
            "region": "TEXT",
            "streetName": "TEXT",
            "transportZone": "TEXT",
        },
    },
    "customer_company_assignments": {
        "table": "customer_company_assignments",
        "columns": {
            "customer": "TEXT",
            "companyCode": "TEXT",
            "accountingClerk": "TEXT",
            "paymentBlockingReason": "TEXT",
            "paymentMethodsList": "TEXT",
            "paymentTerms": "TEXT",
            "reconciliationAccount": "TEXT",
            "deletionIndicator": "INTEGER",
            "customerAccountGroup": "TEXT",
        },
    },
    "customer_sales_area_assignments": {
        "table": "customer_sales_area_assignments",
        "columns": {
            "customer": "TEXT",
            "salesOrganization": "TEXT",
            "distributionChannel": "TEXT",
            "division": "TEXT",
            "customerAccountAssignmentGroup": "TEXT",
            "customerGroup": "TEXT",
            "customerPaymentTerms": "TEXT",
            "customerPriceProcedure": "TEXT",
            "deliveryPriority": "TEXT",
            "incotermsClassification": "TEXT",
            "salesGroup": "TEXT",
            "salesOffice": "TEXT",
            "shippingCondition": "TEXT",
            "supplyingPlant": "TEXT",
        },
    },
    "products": {
        "table": "products",
        "columns": {
            "product": "TEXT PRIMARY KEY",
            "productType": "TEXT",
            "crossPlantStatus": "TEXT",
            "crossPlantStatusValidityDate": "TEXT",
            "creationDate": "TEXT",
            "createdByUser": "TEXT",
            "lastChangeDate": "TEXT",
            "lastChangeDateTime": "TEXT",
            "isMarkedForDeletion": "INTEGER",
            "productOldId": "TEXT",
            "grossWeight": "REAL",
            "weightUnit": "TEXT",
            "netWeight": "REAL",
            "productGroup": "TEXT",
            "baseUnit": "TEXT",
            "division": "TEXT",
            "industrySector": "TEXT",
        },
    },
    "product_descriptions": {
        "table": "product_descriptions",
        "columns": {
            "product": "TEXT",
            "language": "TEXT",
            "productDescription": "TEXT",
        },
    },
    "plants": {
        "table": "plants",
        "columns": {
            "plant": "TEXT PRIMARY KEY",
            "plantName": "TEXT",
            "valuationArea": "TEXT",
            "plantCustomer": "TEXT",
            "plantSupplier": "TEXT",
            "factoryCalendar": "TEXT",
            "defaultPurchasingOrganization": "TEXT",
            "salesOrganization": "TEXT",
            "addressId": "TEXT",
            "plantCategory": "TEXT",
            "distributionChannel": "TEXT",
            "division": "TEXT",
            "language": "TEXT",
            "isMarkedForArchiving": "INTEGER",
        },
    },
    "product_plants": {
        "table": "product_plants",
        "columns": {
            "product": "TEXT",
            "plant": "TEXT",
            "countryOfOrigin": "TEXT",
            "regionOfOrigin": "TEXT",
            "productionInvtryManagedLoc": "TEXT",
            "availabilityCheckType": "TEXT",
            "fiscalYearVariant": "TEXT",
            "profitCenter": "TEXT",
            "mrpType": "TEXT",
        },
    },
    "product_storage_locations": {
        "table": "product_storage_locations",
        "columns": {
            "product": "TEXT",
            "plant": "TEXT",
            "storageLocation": "TEXT",
            "creationDate": "TEXT",
            "isMarkedForDeletion": "INTEGER",
        },
    },
}


def flatten_record(record: dict) -> dict:
    """Flatten nested objects (like time fields) into simple values."""
    flat = {}
    for key, value in record.items():
        if isinstance(value, dict):
            # Time objects like {"hours": 6, "minutes": 49, "seconds": 13}
            if "hours" in value:
                flat[key] = f"{value['hours']:02d}:{value['minutes']:02d}:{value['seconds']:02d}"
            else:
                flat[key] = json.dumps(value)
        elif isinstance(value, bool):
            flat[key] = int(value)
        else:
            flat[key] = value
    return flat


def get_connection() -> sqlite3.Connection:
    """Get a SQLite connection with WAL mode for concurrent reads."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_database() -> sqlite3.Connection:
    """Create tables and load data from JSONL files into SQLite."""
    conn = get_connection()
    cursor = conn.cursor()

    # Check if data already loaded
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sales_order_headers'")
    if cursor.fetchone():
        cursor.execute("SELECT COUNT(*) FROM sales_order_headers")
        if cursor.fetchone()[0] > 0:
            print("Database already initialized, skipping ingestion.")
            return conn

    print("Initializing database and loading data...")

    for folder_name, schema in TABLE_SCHEMAS.items():
        table = schema["table"]
        columns = schema["columns"]

        # Create table
        col_defs = ", ".join(f'"{col}" {dtype}' for col, dtype in columns.items())
        cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')

        # Load JSONL files from the folder
        folder_path = Path(DATA_DIR) / folder_name
        if not folder_path.exists():
            print(f"  Warning: folder {folder_path} not found, skipping.")
            continue

        row_count = 0
        for jsonl_file in sorted(folder_path.glob("*.jsonl")):
            with open(jsonl_file, "r") as f:
                for line in f:
                    record = flatten_record(json.loads(line.strip()))
                    # Only insert columns we defined in schema
                    valid_cols = [c for c in columns if c in record]
                    placeholders = ", ".join("?" for _ in valid_cols)
                    col_names = ", ".join(f'"{c}"' for c in valid_cols)
                    values = [record.get(c) for c in valid_cols]
                    try:
                        cursor.execute(
                            f'INSERT OR IGNORE INTO "{table}" ({col_names}) VALUES ({placeholders})',
                            values,
                        )
                        row_count += 1
                    except sqlite3.Error as e:
                        print(f"  Error inserting into {table}: {e}")

        print(f"  Loaded {row_count} rows into {table}")

    # Create indexes for frequently joined columns
    indexes = [
        ("idx_soi_salesorder", "sales_order_items", "salesOrder"),
        ("idx_soi_material", "sales_order_items", "material"),
        ("idx_odi_delivery", "outbound_delivery_items", "deliveryDocument"),
        ("idx_odi_refsd", "outbound_delivery_items", "referenceSdDocument"),
        ("idx_bdi_billing", "billing_document_items", "billingDocument"),
        ("idx_bdi_refsd", "billing_document_items", "referenceSdDocument"),
        ("idx_bdh_acctdoc", "billing_document_headers", "accountingDocument"),
        ("idx_bdh_soldto", "billing_document_headers", "soldToParty"),
        ("idx_soh_soldto", "sales_order_headers", "soldToParty"),
        ("idx_jei_refdoc", "journal_entry_items", "referenceDocument"),
        ("idx_jei_acctdoc", "journal_entry_items", "accountingDocument"),
        ("idx_pay_customer", "payments_accounts_receivable", "customer"),
        ("idx_bp_customer", "business_partners", "customer"),
        ("idx_pd_product", "product_descriptions", "product"),
    ]
    for idx_name, table, column in indexes:
        cursor.execute(f'CREATE INDEX IF NOT EXISTS {idx_name} ON "{table}" ("{column}")')

    conn.commit()
    print("Database initialization complete.")
    return conn


def get_schema_description() -> str:
    """Return a human-readable schema description for the LLM prompt."""
    lines = ["DATABASE SCHEMA (SQLite):", ""]
    for folder_name, schema in TABLE_SCHEMAS.items():
        table = schema["table"]
        columns = schema["columns"]
        lines.append(f"Table: {table}")
        for col, dtype in columns.items():
            lines.append(f"  - {col} ({dtype})")
        lines.append("")

    lines.append("KEY RELATIONSHIPS:")
    lines.append("  - sales_order_headers.salesOrder -> sales_order_items.salesOrder")
    lines.append("  - sales_order_headers.soldToParty -> business_partners.customer")
    lines.append("  - sales_order_items.material -> products.product")
    lines.append("  - sales_order_items.productionPlant -> plants.plant")
    lines.append("  - outbound_delivery_items.referenceSdDocument -> sales_order_headers.salesOrder")
    lines.append("  - outbound_delivery_items.deliveryDocument -> outbound_delivery_headers.deliveryDocument")
    lines.append("  - outbound_delivery_items.plant -> plants.plant")
    lines.append("  - billing_document_items.referenceSdDocument -> outbound_delivery_headers.deliveryDocument")
    lines.append("  - billing_document_items.billingDocument -> billing_document_headers.billingDocument")
    lines.append("  - billing_document_headers.accountingDocument -> journal_entry_items.accountingDocument")
    lines.append("  - billing_document_headers.soldToParty -> business_partners.customer")
    lines.append("  - journal_entry_items.referenceDocument -> billing_document_headers.billingDocument")
    lines.append("  - payments_accounts_receivable.customer -> business_partners.customer")
    lines.append("  - products.product -> product_descriptions.product")
    lines.append("  - products.product -> product_plants.product")
    lines.append("  - product_plants.plant -> plants.plant")
    lines.append("  - business_partners.businessPartner -> business_partner_addresses.businessPartner")
    lines.append("  - customer_company_assignments.customer -> business_partners.customer")
    lines.append("  - customer_sales_area_assignments.customer -> business_partners.customer")
    lines.append("")
    lines.append("O2C FLOW: Sales Order -> Delivery -> Billing Document -> Journal Entry -> Payment")
    return "\n".join(lines)
