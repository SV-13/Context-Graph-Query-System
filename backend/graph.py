"""
Graph module: Builds a NetworkX graph from the SQLite database.
The graph represents the SAP Order-to-Cash (O2C) flow with entities as
nodes and business relationships as edges.

Design decision: We use a condensed graph with the most important entities
(Sales Orders, Deliveries, Billing Docs, Journal Entries, Payments,
Customers, Products, Plants) to keep visualization manageable.
"""

import networkx as nx
import sqlite3
from typing import Any


# Node type colors for the frontend
NODE_COLORS = {
    "SalesOrder": "#4A90D9",
    "Delivery": "#50C878",
    "BillingDocument": "#FFB347",
    "JournalEntry": "#FF6B6B",
    "Payment": "#DDA0DD",
    "Customer": "#87CEEB",
    "Product": "#F0E68C",
    "Plant": "#98FB98",
}


def build_graph(conn: sqlite3.Connection) -> nx.DiGraph:
    """
    Build a directed graph from the O2C dataset.
    Nodes represent business entities, edges represent relationships.
    """
    G = nx.DiGraph()
    cursor = conn.cursor()

    # --- Add Customer nodes ---
    cursor.execute("""
        SELECT bp.businessPartner, bp.businessPartnerName, bp.customer,
               bpa.cityName, bpa.country, bpa.region
        FROM business_partners bp
        LEFT JOIN business_partner_addresses bpa ON bp.businessPartner = bpa.businessPartner
    """)
    for row in cursor.fetchall():
        node_id = f"Customer:{row['customer']}"
        G.add_node(node_id, **{
            "type": "Customer",
            "id": row["customer"],
            "label": row["businessPartnerName"] or row["customer"],
            "businessPartner": row["businessPartner"],
            "city": row["cityName"],
            "country": row["country"],
            "region": row["region"],
            "color": NODE_COLORS["Customer"],
        })

    # --- Add Product nodes ---
    cursor.execute("""
        SELECT p.product, p.productType, p.productGroup, p.baseUnit,
               p.grossWeight, p.weightUnit, p.division,
               pd.productDescription
        FROM products p
        LEFT JOIN product_descriptions pd ON p.product = pd.product AND pd.language = 'EN'
    """)
    for row in cursor.fetchall():
        node_id = f"Product:{row['product']}"
        G.add_node(node_id, **{
            "type": "Product",
            "id": row["product"],
            "label": row["productDescription"] or row["product"],
            "productType": row["productType"],
            "productGroup": row["productGroup"],
            "baseUnit": row["baseUnit"],
            "color": NODE_COLORS["Product"],
        })

    # --- Add Plant nodes ---
    cursor.execute("SELECT plant, plantName, salesOrganization FROM plants")
    for row in cursor.fetchall():
        node_id = f"Plant:{row['plant']}"
        G.add_node(node_id, **{
            "type": "Plant",
            "id": row["plant"],
            "label": row["plantName"] or row["plant"],
            "salesOrganization": row["salesOrganization"],
            "color": NODE_COLORS["Plant"],
        })

    # --- Add Sales Order nodes and edges ---
    cursor.execute("""
        SELECT salesOrder, salesOrderType, soldToParty, creationDate,
               totalNetAmount, transactionCurrency, overallDeliveryStatus
        FROM sales_order_headers
    """)
    for row in cursor.fetchall():
        node_id = f"SalesOrder:{row['salesOrder']}"
        G.add_node(node_id, **{
            "type": "SalesOrder",
            "id": row["salesOrder"],
            "label": f"SO-{row['salesOrder']}",
            "salesOrderType": row["salesOrderType"],
            "creationDate": row["creationDate"],
            "totalNetAmount": row["totalNetAmount"],
            "currency": row["transactionCurrency"],
            "deliveryStatus": row["overallDeliveryStatus"],
            "color": NODE_COLORS["SalesOrder"],
        })
        # Edge: Customer -> SalesOrder
        cust_id = f"Customer:{row['soldToParty']}"
        if G.has_node(cust_id):
            G.add_edge(cust_id, node_id, relation="PLACED_ORDER", label="placed")

    # Sales Order -> Product edges (via items)
    cursor.execute("""
        SELECT DISTINCT salesOrder, material, productionPlant
        FROM sales_order_items
    """)
    for row in cursor.fetchall():
        so_id = f"SalesOrder:{row['salesOrder']}"
        prod_id = f"Product:{row['material']}"
        plant_id = f"Plant:{row['productionPlant']}"
        if G.has_node(so_id) and G.has_node(prod_id):
            G.add_edge(so_id, prod_id, relation="CONTAINS_PRODUCT", label="contains")
        if G.has_node(so_id) and G.has_node(plant_id):
            G.add_edge(so_id, plant_id, relation="PRODUCED_AT", label="produced at")

    # --- Add Delivery nodes and edges ---
    cursor.execute("""
        SELECT deliveryDocument, creationDate, overallGoodsMovementStatus,
               overallPickingStatus, shippingPoint
        FROM outbound_delivery_headers
    """)
    for row in cursor.fetchall():
        node_id = f"Delivery:{row['deliveryDocument']}"
        G.add_node(node_id, **{
            "type": "Delivery",
            "id": row["deliveryDocument"],
            "label": f"DL-{row['deliveryDocument']}",
            "creationDate": row["creationDate"],
            "goodsMovementStatus": row["overallGoodsMovementStatus"],
            "pickingStatus": row["overallPickingStatus"],
            "shippingPoint": row["shippingPoint"],
            "color": NODE_COLORS["Delivery"],
        })

    # Delivery -> SalesOrder edges, Delivery -> Plant edges
    cursor.execute("""
        SELECT DISTINCT deliveryDocument, referenceSdDocument, plant
        FROM outbound_delivery_items
    """)
    for row in cursor.fetchall():
        del_id = f"Delivery:{row['deliveryDocument']}"
        so_id = f"SalesOrder:{row['referenceSdDocument']}"
        plant_id = f"Plant:{row['plant']}"
        if G.has_node(del_id) and G.has_node(so_id):
            G.add_edge(so_id, del_id, relation="DELIVERED_VIA", label="delivered via")
        if G.has_node(del_id) and G.has_node(plant_id):
            G.add_edge(del_id, plant_id, relation="SHIPPED_FROM", label="shipped from")

    # --- Add Billing Document nodes and edges ---
    cursor.execute("""
        SELECT billingDocument, billingDocumentType, creationDate,
               billingDocumentIsCancelled, totalNetAmount, transactionCurrency,
               accountingDocument, soldToParty
        FROM billing_document_headers
    """)
    for row in cursor.fetchall():
        node_id = f"BillingDocument:{row['billingDocument']}"
        G.add_node(node_id, **{
            "type": "BillingDocument",
            "id": row["billingDocument"],
            "label": f"BD-{row['billingDocument']}",
            "billingDocumentType": row["billingDocumentType"],
            "creationDate": row["creationDate"],
            "isCancelled": bool(row["billingDocumentIsCancelled"]),
            "totalNetAmount": row["totalNetAmount"],
            "currency": row["transactionCurrency"],
            "accountingDocument": row["accountingDocument"],
            "color": NODE_COLORS["BillingDocument"],
        })
        # Edge: BillingDocument -> Customer
        cust_id = f"Customer:{row['soldToParty']}"
        if G.has_node(cust_id):
            G.add_edge(node_id, cust_id, relation="BILLED_TO", label="billed to")

    # Billing -> Delivery edges (via billing items referencing delivery docs)
    cursor.execute("""
        SELECT DISTINCT billingDocument, referenceSdDocument
        FROM billing_document_items
    """)
    for row in cursor.fetchall():
        bill_id = f"BillingDocument:{row['billingDocument']}"
        del_id = f"Delivery:{row['referenceSdDocument']}"
        if G.has_node(bill_id) and G.has_node(del_id):
            G.add_edge(del_id, bill_id, relation="BILLED_AS", label="billed as")

    # --- Add Journal Entry nodes and edges ---
    cursor.execute("""
        SELECT DISTINCT accountingDocument, referenceDocument, postingDate,
               accountingDocumentType, customer
        FROM journal_entry_items
    """)
    seen_je = set()
    for row in cursor.fetchall():
        acct_doc = row["accountingDocument"]
        if acct_doc in seen_je:
            continue
        seen_je.add(acct_doc)

        node_id = f"JournalEntry:{acct_doc}"
        G.add_node(node_id, **{
            "type": "JournalEntry",
            "id": acct_doc,
            "label": f"JE-{acct_doc}",
            "referenceDocument": row["referenceDocument"],
            "postingDate": row["postingDate"],
            "documentType": row["accountingDocumentType"],
            "color": NODE_COLORS["JournalEntry"],
        })
        # Edge: BillingDocument -> JournalEntry
        bill_id = f"BillingDocument:{row['referenceDocument']}"
        if G.has_node(bill_id):
            G.add_edge(bill_id, node_id, relation="POSTED_AS", label="posted as")

    # --- Add Payment nodes and edges ---
    cursor.execute("""
        SELECT DISTINCT accountingDocument, customer, postingDate,
               amountInTransactionCurrency, transactionCurrency,
               clearingAccountingDocument
        FROM payments_accounts_receivable
    """)
    seen_pay = set()
    for row in cursor.fetchall():
        pay_doc = row["accountingDocument"]
        if pay_doc in seen_pay:
            continue
        seen_pay.add(pay_doc)

        node_id = f"Payment:{pay_doc}"
        G.add_node(node_id, **{
            "type": "Payment",
            "id": pay_doc,
            "label": f"PAY-{pay_doc}",
            "postingDate": row["postingDate"],
            "amount": row["amountInTransactionCurrency"],
            "currency": row["transactionCurrency"],
            "clearingDocument": row["clearingAccountingDocument"],
            "color": NODE_COLORS["Payment"],
        })
        # Edge: Payment -> Customer
        cust_id = f"Customer:{row['customer']}"
        if G.has_node(cust_id):
            G.add_edge(node_id, cust_id, relation="PAID_BY", label="paid by")

        # Edge: Payment -> JournalEntry (via clearing document)
        je_id = f"JournalEntry:{row['accountingDocument']}"
        if G.has_node(je_id):
            G.add_edge(je_id, node_id, relation="CLEARED_BY", label="cleared by")

    print(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G


def graph_to_json(G: nx.DiGraph, node_filter: str | None = None, center_node: str | None = None, depth: int = 2) -> dict[str, Any]:
    """
    Convert graph to JSON format for the frontend visualization.
    If center_node is provided, return only the subgraph within `depth` hops.
    If node_filter is provided, filter by node type.
    """
    if center_node and G.has_node(center_node):
        # BFS to get neighbors within depth
        nodes_in_view = {center_node}
        frontier = {center_node}
        for _ in range(depth):
            next_frontier = set()
            for n in frontier:
                next_frontier.update(G.predecessors(n))
                next_frontier.update(G.successors(n))
            nodes_in_view.update(next_frontier)
            frontier = next_frontier
        subgraph = G.subgraph(nodes_in_view)
    elif node_filter:
        filtered_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == node_filter]
        subgraph = G.subgraph(filtered_nodes)
    else:
        subgraph = G

    nodes = []
    for node_id, data in subgraph.nodes(data=True):
        node_data = dict(data)
        node_data["nodeId"] = node_id
        node_data["val"] = max(1, subgraph.degree(node_id))  # Size by connectivity
        nodes.append(node_data)

    links = []
    for source, target, data in subgraph.edges(data=True):
        links.append({
            "source": source,
            "target": target,
            "relation": data.get("relation", ""),
            "label": data.get("label", ""),
        })

    return {"nodes": nodes, "links": links}


def get_node_details(G: nx.DiGraph, node_id: str) -> dict[str, Any] | None:
    """Get full details of a node including its neighbors."""
    if not G.has_node(node_id):
        return None

    data = dict(G.nodes[node_id])
    data["nodeId"] = node_id

    # Get connected nodes
    neighbors = []
    for pred in G.predecessors(node_id):
        edge_data = G.edges[pred, node_id]
        neighbors.append({
            "nodeId": pred,
            "direction": "incoming",
            "relation": edge_data.get("relation", ""),
            "label": edge_data.get("label", ""),
            **{k: v for k, v in G.nodes[pred].items() if k in ("type", "label", "id")},
        })
    for succ in G.successors(node_id):
        edge_data = G.edges[node_id, succ]
        neighbors.append({
            "nodeId": succ,
            "direction": "outgoing",
            "relation": edge_data.get("relation", ""),
            "label": edge_data.get("label", ""),
            **{k: v for k, v in G.nodes[succ].items() if k in ("type", "label", "id")},
        })

    data["neighbors"] = neighbors
    return data
