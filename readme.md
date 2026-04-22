# AI-Powered Complaint RCA System

## Setup

```bash
# 1. Install dependencies (uv manages the venv automatically)
uv sync

# 2. Configure environment
cp .env.example .env
# Edit .env — set AZURE_PROJECT_ENDPOINT and AZURE_AGENT_ID

# 3. Authenticate with Azure
az login

# 4. Start both services
bash start.sh
```

Open **http://localhost:8501** for the UI.

---

## Run services individually

```bash
# Backend (FastAPI) — http://localhost:8000
uv run uvicorn backend.main:app --reload

# Frontend (Streamlit) — http://localhost:8501
uv run streamlit run frontend/app.py
```

## API docs

FastAPI auto-generates docs at **http://localhost:8000/docs**

---

## Environment variables (`.env`)

| Variable | Required | Description |
|---|---|---|
| `AZURE_PROJECT_ENDPOINT` | Yes | Azure AI Foundry project endpoint URL |
| `AZURE_AGENT_ID` | Yes | Agent ID from the Agents tab in AI Foundry |
| `BATCH_SIZE` | No | Rows per agent call (default: `5`) |
| `BACKEND_URL` | No | Backend URL seen by Streamlit (default: `http://localhost:8000`) |
