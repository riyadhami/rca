# AI-Powered Complaint RCA System

## Setup

`pip install uv`

```bash
uv sync
```

```bash
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
