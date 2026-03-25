"""
Guardrails module: Validates user queries to ensure they are relevant
to the SAP O2C dataset. Rejects off-topic, harmful, or irrelevant prompts.

Strategy:
1. Keyword-based pre-filter for obviously off-topic queries
2. LLM-based classification as a second layer
"""

import re

# Topics that are clearly outside the scope of the dataset
OFF_TOPIC_PATTERNS = [
    r"\b(write|compose|create)\b.*(poem|story|essay|song|joke|letter|email)",
    r"\b(who is|tell me about|what is)\b.*(president|celebrity|actor|singer|politician)",
    r"\b(recipe|cook|food|restaurant)\b",
    r"\b(weather|forecast|temperature)\b",
    r"\b(stock|crypto|bitcoin|investment|trading)\b",
    r"\b(movie|film|tv show|series|netflix|game|play)\b",
    r"\b(translate|translation)\b",
    r"\b(code|program|python|javascript|html|css)\b.*\b(write|create|build|make)\b",
    r"\b(math|equation|calculate|solve)\b.*\b(?!amount|total|sum|count|average)\b",
    r"\b(news|politics|election|war|sports)\b",
    r"\b(health|medical|doctor|disease|symptom)\b",
    r"\b(travel|flight|hotel|vacation|tourism)\b",
]

# Keywords that indicate the query is likely about the dataset
DOMAIN_KEYWORDS = [
    "sales", "order", "delivery", "billing", "invoice", "payment", "journal",
    "customer", "product", "plant", "material", "document", "amount", "quantity",
    "shipped", "billed", "cancelled", "flow", "trace", "status", "total",
    "revenue", "transaction", "currency", "accounting", "receivable",
    "SAP", "O2C", "order-to-cash", "business partner", "company code",
    "net amount", "gross", "weight", "schedule", "delivery document",
    "billing document", "sales order", "outbound", "inbound",
    "highest", "lowest", "most", "least", "average", "count", "sum",
    "broken", "incomplete", "missing", "pending", "cleared",
    "profit center", "cost center", "fiscal year",
]

REJECTION_MESSAGE = (
    "I'm sorry, but this system is designed to answer questions related to "
    "the SAP Order-to-Cash (O2C) dataset only. This includes sales orders, "
    "deliveries, billing documents, payments, journal entries, customers, "
    "products, and plants. Please ask a question related to this data."
)


def check_query_relevance(query: str) -> tuple[bool, str]:
    """
    Check if a query is relevant to the O2C dataset.
    Returns (is_relevant, rejection_reason_or_empty_string).
    """
    query_lower = query.lower().strip()

    # Allow very short queries (might be entity IDs or simple lookups)
    if len(query_lower) < 3:
        return False, "Please provide a more detailed question about the dataset."

    # Check for off-topic patterns
    for pattern in OFF_TOPIC_PATTERNS:
        if re.search(pattern, query_lower, re.IGNORECASE):
            return False, REJECTION_MESSAGE

    # Check if any domain keywords are present
    has_domain_keyword = any(kw.lower() in query_lower for kw in DOMAIN_KEYWORDS)

    # If the query has domain keywords, it's likely relevant
    if has_domain_keyword:
        return True, ""

    # For ambiguous queries, let the LLM decide (return True and let LLM handle it)
    # This is a soft pass - the LLM system prompt also has guardrails
    return True, ""


def get_guardrail_prompt() -> str:
    """Return the guardrail instructions to include in the LLM system prompt."""
    return """
IMPORTANT GUARDRAILS:
- You MUST ONLY answer questions related to the SAP Order-to-Cash (O2C) dataset.
- The dataset contains: Sales Orders, Deliveries, Billing Documents, Journal Entries,
  Payments, Customers, Products, and Plants.
- If a user asks about topics unrelated to this dataset (general knowledge, creative writing,
  coding help, personal advice, news, etc.), respond with:
  "This system is designed to answer questions related to the SAP Order-to-Cash dataset only.
  You can ask about sales orders, deliveries, billing documents, payments, journal entries,
  customers, products, and plants."
- NEVER make up data. Only provide answers backed by SQL query results.
- If a query cannot be answered with the available data, say so clearly.
"""
