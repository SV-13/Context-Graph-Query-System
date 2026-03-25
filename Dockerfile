FROM python:3.13-slim

WORKDIR /app

# Install Node.js for building frontend
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

# Build frontend
COPY frontend/ /app/frontend/
WORKDIR /app/frontend
RUN npm install && npm run build

# Copy backend and dataset
WORKDIR /app
COPY backend/ /app/backend/
COPY sap-o2c-data/ /app/sap-o2c-data/

# Serve frontend static files from FastAPI
ENV DATA_DIR=/app/sap-o2c-data
ENV DB_PATH=/app/sap_o2c.db
ENV STATIC_DIR=/app/frontend/dist

WORKDIR /app/backend
EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
