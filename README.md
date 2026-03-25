# SAP O2C Graph Explorer

A graph-based data modeling and natural language query system for SAP Order-to-Cash (O2C) data. Users can visually explore interconnected business entities and query the dataset using conversational language.

![Architecture](https://img.shields.io/badge/Backend-FastAPI-009688?style=flat-square)
![Database](https://img.shields.io/badge/Database-SQLite-003B57?style=flat-square)
![Graph](https://img.shields.io/badge/Graph-NetworkX-4B8BBE?style=flat-square)
![Frontend](https://img.shields.io/badge/Frontend-React-61DAFB?style=flat-square)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Frontend (React)                       │
│  ┌──────────────────────┐  ┌─────────────────────────────┐  │
│  │  Graph Visualization │  │    Chat Interface           │  │
│  │  (react-force-graph) │  │    (NL Query → Answer)      │  │
│  └──────────────────────┘  └─────────────────────────────┘  │
└──────────────────────────┬──────────────────────────────────┘
                           │ REST API
┌──────────────────────────┴──────────────────────────────────┐
│                    Backend (FastAPI)                        │
│  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐ │
│  │  Graph API │  │  Chat API  │  │    Guardrails          │ │
│  │ (NetworkX) │  │  (NL→SQL)  │  │    (Domain Filter)     │ │
│  └──────┬─────┘  └─────┬──────┘  └────────────────────────┘ │
│         │              │                                    │
│  ┌──────┴──────────────┴──────┐  ┌────────────────────────┐ │
│  │      SQLite Database       │  │     LLM (Gemini)       │ │
│  │   (19 normalized tables)   │  │   (NL→SQL + Answer)    │ │
│  └────────────────────────────┘  └────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### 1. Database: SQLite
- **Why**: Zero-config, single-file deployment, excellent SQL support. Since the LLM generates SQL queries, SQLite's broad SQL compatibility makes it ideal for dynamic query execution.
- **Tradeoff**: Not suitable for concurrent writes at scale, but perfect for this read-heavy analytics use case.

### 2. Graph Engine: NetworkX (in-memory)
- **Why**: The dataset is small enough (~700 nodes, ~1200 edges) to fit entirely in memory. NetworkX provides fast traversal and subgraph extraction without needing a dedicated graph database.
- **Tradeoff**: Wouldn't scale to millions of nodes. For production, Neo4j or similar would be preferred.

### 3. LLM Integration: Two-Step Pipeline
- **Step 1**: NL question → SQL query (via LLM with full schema context)
- **Step 2**: SQL results → Natural language answer (via LLM)
- **Why**: This ensures every answer is **data-backed**. The LLM never fabricates data — it only interprets actual query results.
- **Retry logic**: If the first SQL attempt fails, the error is fed back to the LLM for self-correction.

### 4. LLM Provider: Gemini (cloud)
- **Primary**: Google Gemini free tier for cloud deployment
- **Why**: Gemini free tier works for deployment demos.

### 5. Guardrails: Two-Layer Filtering
- **Layer 1**: Regex-based pre-filter catches obviously off-topic queries (poems, recipes, news, etc.)
- **Layer 2**: LLM system prompt instructs the model to reject non-dataset queries
- **Why**: Pre-filtering saves LLM tokens on obvious rejections. LLM-level guardrails catch subtle off-topic queries.

### 6. Graph Modeling
The O2C flow is modeled as a directed graph:
```
Customer → Sales Order → Delivery → Billing Document → Journal Entry → Payment
                ↓              ↓
            Product          Plant
```
- **Nodes**: 8 entity types (Customer, SalesOrder, Delivery, BillingDocument, JournalEntry, Payment, Product, Plant)
- **Edges**: Business relationships (PLACED_ORDER, DELIVERED_VIA, BILLED_AS, POSTED_AS, etc.)
- **Node sizing**: By degree (more connections = larger node)

## Project Structure

```
├── backend/
│   ├── main.py           # FastAPI app, API endpoints
│   ├── database.py       # SQLite schema, data ingestion, indexes
│   ├── graph.py          # NetworkX graph construction, traversal
│   ├── llm.py            # LLM clients (Gemini), NL→SQL pipeline
│   ├── guardrails.py     # Query relevance filtering
│   ├── requirements.txt
│   └── .env
├── frontend/
│   ├── src/
│   │   ├── App.jsx            # Main layout, state management
│   │   ├── components/
│   │   │   ├── GraphView.jsx  # Force-directed graph visualization
│   │   │   ├── ChatPanel.jsx  # Chat interface with sample queries
│   │   │   └── NodeDetail.jsx # Node metadata inspector
│   │   └── styles/App.css
│   ├── package.json
│   └── vite.config.js
├── sap-o2c-data/              # Raw JSONL dataset (19 entity folders)
└── README.md
```

## Setup & Running

### Prerequisites
- Python 3.11+
- Node.js 18+
- A Gemini API key

### Backend
```bash
cd backend
python3.13 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure LLM :
# Gemini (set in .env)
# LLM_PROVIDER=gemini
# GEMINI_API_KEY=your_key

python main.py
# Server runs on http://localhost:8000
```

### Frontend
```bash
cd frontend
npm install
npm run dev
# App runs on http://localhost:5173
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check with graph stats |
| `/api/graph` | GET | Full graph data (nodes + links) |
| `/api/graph?center=SalesOrder:740506&depth=2` | GET | Subgraph around a node |
| `/api/graph?node_type=Customer` | GET | Filter by node type |
| `/api/graph/node/{nodeId}` | GET | Node details + neighbors |
| `/api/graph/stats` | GET | Graph statistics |
| `/api/schema` | GET | Database schema description |
| `/api/chat` | POST | Natural language query |

## Example Queries

- "Which products are associated with the highest number of billing documents?"
- "Trace the full flow of billing document 90504248"
- "Show me sales orders that were delivered but not billed"
- "Which customers have the highest total order value?"
- "Find sales orders with incomplete flows"
- "What is the average delivery time for each plant?"
