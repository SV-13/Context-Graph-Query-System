"""
FastAPI backend for the SAP O2C Graph Query System.

Architecture:
- SQLite for structured data storage and SQL query execution
- NetworkX for in-memory graph construction and traversal
- Google Gemini for NL-to-SQL translation and answer generation
- FastAPI for the REST API layer
"""

import os
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

from database import init_database, get_schema_description
from graph import build_graph, graph_to_json, get_node_details, NODE_COLORS
from llm import init_llm, process_query

# Global state
db_conn = None
graph = None
llm_model = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database, graph, and LLM on startup."""
    global db_conn, graph, llm_model

    print("Starting up...")
    db_conn = init_database()
    graph = build_graph(db_conn)
    llm_model = init_llm()
    print("Startup complete.")

    yield

    if db_conn:
        db_conn.close()
    print("Shutdown complete.")


app = FastAPI(
    title="SAP O2C Graph Query System",
    description="Graph-based data modeling and natural language query system for SAP Order-to-Cash data",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS - allow frontend origins
allowed_origins = [
    "http://localhost:5173",
    "http://localhost:3000",
]
frontend_url = os.environ.get("FRONTEND_URL", "")
if frontend_url:
    allowed_origins.append(frontend_url)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_origin_regex=r"https://.*\.vercel\.app",  # Allow Vercel preview deploys
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Request/Response models ---

class ChatRequest(BaseModel):
    question: str
    conversation_history: list[dict] | None = None


class ChatResponse(BaseModel):
    answer: str
    sql: str | None = None
    results: list[dict] = []
    highlight_nodes: list[str] = []
    error: str | None = None


# --- API Endpoints ---

@app.get("/api/health")
def health_check():
    """Health check endpoint."""
    return {
        "status": "ok",
        "graph_nodes": graph.number_of_nodes() if graph else 0,
        "graph_edges": graph.number_of_edges() if graph else 0,
        "llm_configured": llm_model is not None,
    }


@app.get("/api/graph")
def get_graph(
    node_type: str | None = Query(None, description="Filter by node type"),
    center: str | None = Query(None, description="Center node ID for subgraph"),
    depth: int = Query(2, description="Depth of subgraph from center node"),
):
    """
    Get graph data for visualization.
    Returns nodes and links in a format compatible with react-force-graph.
    """
    return graph_to_json(graph, node_filter=node_type, center_node=center, depth=depth)


@app.get("/api/graph/node/{node_id:path}")
def get_node(node_id: str):
    """Get detailed information about a specific node."""
    details = get_node_details(graph, node_id)
    if not details:
        return {"error": "Node not found"}
    return details


@app.get("/api/graph/stats")
def get_graph_stats():
    """Get statistics about the graph."""
    type_counts = {}
    for _, data in graph.nodes(data=True):
        t = data.get("type", "Unknown")
        type_counts[t] = type_counts.get(t, 0) + 1

    return {
        "total_nodes": graph.number_of_nodes(),
        "total_edges": graph.number_of_edges(),
        "node_types": type_counts,
        "node_colors": NODE_COLORS,
    }


@app.get("/api/schema")
def get_schema():
    """Return the database schema description."""
    return {"schema": get_schema_description()}


@app.post("/api/chat", response_model=ChatResponse)
def chat(request: ChatRequest):
    """
    Process a natural language query against the O2C dataset.
    Translates the question to SQL, executes it, and returns a data-backed answer.
    """
    result = process_query(
        llm_model, db_conn, request.question, request.conversation_history
    )
    return ChatResponse(**result)


# Serve frontend static files in production (when STATIC_DIR is set)
STATIC_DIR = os.environ.get("STATIC_DIR", "")
if STATIC_DIR and Path(STATIC_DIR).exists():
    # Serve static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=os.path.join(STATIC_DIR, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        """Serve the React SPA for any non-API route."""
        file_path = os.path.join(STATIC_DIR, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
