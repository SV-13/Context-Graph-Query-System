"""
LLM module: Google Gemini for NL-to-SQL translation.

Strategy:
1. User asks a natural language question
2. We send the question + DB schema to the LLM
3. LLM generates a SQL query
4. We execute the SQL and get results
5. We send results back to the LLM for a natural language answer

This two-step approach ensures answers are always data-backed.
Supports Gemini (cloud, free tier).
"""

import os
import json
import sqlite3
import re
import urllib.request
import urllib.error
from database import get_schema_description
from guardrails import check_query_relevance, get_guardrail_prompt, REJECTION_MESSAGE

# Configuration: Google Gemini as default
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "gemini") 

SQL_GENERATION_PROMPT = """You are a SQL expert for an SAP Order-to-Cash (O2C) SQLite database.

{schema}

{guardrails}

Given the user's question, generate a valid SQLite SQL query to answer it.

RULES:
- Return ONLY the SQL query, no explanation, no markdown formatting, no preamble.
- Use only tables and columns from the schema above.
- Use appropriate JOINs to connect related tables.
- Always use double quotes for column and table names to avoid case issues.
- Limit results to 50 rows max unless the user asks for all.
- For aggregations, always include meaningful labels.

CRITICAL JOIN PATTERNS for the O2C flow (Sales Order -> Delivery -> Billing -> Journal Entry -> Payment):

1. Sales Order to its Items:
   sales_order_headers."salesOrder" = sales_order_items."salesOrder"

2. Sales Order to Delivery (via delivery items):
   sales_order_items."salesOrder" = outbound_delivery_items."referenceSdDocument"
   (outbound_delivery_items."referenceSdDocument" contains the salesOrder number)

3. Delivery Items to Delivery Headers:
   outbound_delivery_items."deliveryDocument" = outbound_delivery_headers."deliveryDocument"

4. Delivery to Billing (via billing items):
   outbound_delivery_headers."deliveryDocument" = billing_document_items."referenceSdDocument"
   (billing_document_items."referenceSdDocument" contains the deliveryDocument number)

5. Billing Items to Billing Headers:
   billing_document_items."billingDocument" = billing_document_headers."billingDocument"

6. Billing to Journal Entry:
   billing_document_headers."accountingDocument" = journal_entry_items."accountingDocument"
   OR journal_entry_items."referenceDocument" = billing_document_headers."billingDocument"

7. Payment links:
   payments_accounts_receivable."customer" = business_partners."customer"

8. Customer links:
   sales_order_headers."soldToParty" = business_partners."customer"
   billing_document_headers."soldToParty" = business_partners."customer"

9. Product links:
   sales_order_items."material" = products."product"
   billing_document_items."material" = products."product"

- For "broken/incomplete flows": use LEFT JOIN and check for NULL to find missing links.
  Example: Sales orders without deliveries = LEFT JOIN delivery items ON referenceSdDocument = salesOrder WHERE deliveryDocument IS NULL.

EXAMPLE: Full O2C trace for a billing document:
SELECT soh."salesOrder", odi."deliveryDocument", bdi."billingDocument",
       bdh."accountingDocument" as journalEntry, bdh."soldToParty"
FROM billing_document_items bdi
JOIN billing_document_headers bdh ON bdi."billingDocument" = bdh."billingDocument"
JOIN outbound_delivery_items odi ON bdi."referenceSdDocument" = odi."deliveryDocument"
JOIN sales_order_headers soh ON odi."referenceSdDocument" = soh."salesOrder"
LEFT JOIN journal_entry_items jei ON bdh."accountingDocument" = jei."accountingDocument"
WHERE bdi."billingDocument" = '...'

REMEMBER: outbound_delivery_items."referenceSdDocument" = the SALES ORDER number.
billing_document_items."referenceSdDocument" = the DELIVERY DOCUMENT number.
These are DIFFERENT columns referencing different entities!

User question: {question}

SQL query:"""

ANSWER_GENERATION_PROMPT = """You are a helpful data analyst for an SAP Order-to-Cash system.

{guardrails}

The user asked: "{question}"

The following SQL query was executed:
```sql
{sql}
```

And returned these results:
{results}

Based on these results, provide a clear, concise natural language answer.
- Reference specific numbers and entities from the data.
- If the results are empty, explain what that means in business context.
- Format numbers nicely (e.g., currency with 2 decimal places).
- If relevant, mention any notable patterns or insights.
- Keep the answer focused and actionable.
- IMPORTANT: If the query results contain node IDs that could be highlighted in the graph,
  include them in this exact format at the END of your response:
  [HIGHLIGHT_NODES: SalesOrder:123, Delivery:456, BillingDocument:789]
  Use the format EntityType:ID where EntityType is one of: SalesOrder, Delivery,
  BillingDocument, JournalEntry, Payment, Customer, Product, Plant"""


class GeminiClient:
    """Client for Google Gemini API (fallback if Ollama unavailable)."""

    def __init__(self, api_key: str):
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel("gemini-2.5-flash")

    def generate(self, prompt: str) -> str:
        response = self.model.generate_content(prompt)
        return response.text.strip()


def init_llm():
    """Initialize the LLM client based on configuration."""
    if GEMINI_API_KEY:
        try:
            client = GeminiClient(GEMINI_API_KEY)
            print("Using Google Gemini as LLM provider.")
            return client
        except Exception as e:
            print(f"Gemini init failed: {e}")

    print("WARNING: No LLM provider available. Chat features will not work.")
    return None


def generate_sql(model, question: str, conversation_history: list[dict] | None = None) -> str:
    """Use the LLM to generate a SQL query from natural language."""
    schema = get_schema_description()
    guardrails = get_guardrail_prompt()
    prompt = SQL_GENERATION_PROMPT.format(
        schema=schema, guardrails=guardrails, question=question
    )

    # Add conversation context if available
    if conversation_history:
        context = "\n\nPrevious conversation for context:\n"
        for msg in conversation_history[-4:]:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            context += f"{role}: {content}\n"
        prompt += context

    sql = model.generate(prompt)

    # Clean up the SQL (remove markdown code blocks if present)
    sql = re.sub(r"^```\w*\n?", "", sql)
    sql = re.sub(r"\n?```$", "", sql)
    sql = sql.strip()

    # Remove any preamble text before SELECT
    select_idx = sql.upper().find("SELECT")
    if select_idx > 0:
        sql = sql[select_idx:]

    return sql


def execute_sql(conn: sqlite3.Connection, sql: str) -> tuple[list[dict], str | None]:
    """
    Execute SQL query safely. Returns (results, error).
    Only allows SELECT statements for safety.
    """
    sql_upper = sql.strip().upper()
    if not sql_upper.startswith("SELECT"):
        return [], "Only SELECT queries are allowed for safety."

    # Block dangerous operations
    dangerous = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "ATTACH", "DETACH"]
    for keyword in dangerous:
        if keyword in sql_upper:
            return [], f"Query contains forbidden keyword: {keyword}"

    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        return results, None
    except sqlite3.Error as e:
        return [], f"SQL Error: {str(e)}"


def generate_answer(model, question: str, sql: str, results: list[dict]) -> str:
    """Use the LLM to generate a natural language answer from SQL results."""
    guardrails = get_guardrail_prompt()

    # Truncate results if too large
    results_str = json.dumps(results[:30], indent=2, default=str)
    if len(results) > 30:
        results_str += f"\n... and {len(results) - 30} more rows"

    prompt = ANSWER_GENERATION_PROMPT.format(
        guardrails=guardrails,
        question=question,
        sql=sql,
        results=results_str,
    )

    return model.generate(prompt)


def process_query(
    model, conn: sqlite3.Connection, question: str,
    conversation_history: list[dict] | None = None
) -> dict:
    """
    Full pipeline: question -> SQL -> execute -> answer.
    Returns a dict with the answer, SQL, results, and any highlighted nodes.
    """
    # Step 1: Check guardrails
    is_relevant, rejection = check_query_relevance(question)
    if not is_relevant:
        return {
            "answer": rejection,
            "sql": None,
            "results": [],
            "highlight_nodes": [],
            "error": None,
        }

    if not model:
        return {
            "answer": "LLM is not configured. Please set GEMINI_API_KEY.",
            "sql": None,
            "results": [],
            "highlight_nodes": [],
            "error": "LLM not configured",
        }

    try:
        # Step 2: Generate SQL
        sql = generate_sql(model, question, conversation_history)

        # Step 3: Execute SQL
        results, error = execute_sql(conn, sql)

        if error:
            # If first SQL attempt fails, try once more with the error context
            retry_question = f"{question}\n\n(Previous SQL attempt failed with: {error}. Previous SQL: {sql}. Please fix the query.)"
            sql = generate_sql(model, retry_question)
            results, error = execute_sql(conn, sql)

            if error:
                return {
                    "answer": f"I couldn't execute the query successfully. Error: {error}",
                    "sql": sql,
                    "results": [],
                    "highlight_nodes": [],
                    "error": error,
                }

        # Step 4: Generate natural language answer
        answer = generate_answer(model, question, sql, results)

        # Step 5: Extract highlighted nodes from the answer
        highlight_nodes = []
        highlight_match = re.search(r"\[HIGHLIGHT_NODES:\s*(.+?)\]", answer)
        if highlight_match:
            nodes_str = highlight_match.group(1)
            highlight_nodes = [
                n.strip() for n in nodes_str.split(",")
                if n.strip() and n.strip().lower() != "none" and ":" in n.strip()
            ]
            # Remove the highlight tag from the visible answer
            answer = re.sub(r"\s*\[HIGHLIGHT_NODES:\s*.+?\]", "", answer).strip()

        return {
            "answer": answer,
            "sql": sql,
            "results": results[:50],
            "highlight_nodes": highlight_nodes,
            "error": None,
        }

    except Exception as e:
        return {
            "answer": f"An error occurred while processing your query: {str(e)}",
            "sql": None,
            "results": [],
            "highlight_nodes": [],
            "error": str(e),
        }
