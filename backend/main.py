import io
import json
import math
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import anthropic
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from backend.taxonomy import taxonomy_mapping

load_dotenv()

_CREDS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "./key.json")
with open(_CREDS_PATH) as _f:
    _GCP_CREDS = json.load(_f)

VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "us-east5")
VERTEX_MODEL    = os.getenv("VERTEX_MODEL", "claude-sonnet-4-5")
VERTEX_PROJECT  = _GCP_CREDS.get("project_id", "")
BATCH_SIZE      = int(os.getenv("BATCH_SIZE", "10"))
MAX_CONCURRENT  = int(os.getenv("MAX_CONCURRENT", "5"))

client = anthropic.AnthropicVertex(
    region=VERTEX_LOCATION,
    project_id=VERTEX_PROJECT,
)

app = FastAPI(title="AI RCA Complaint System", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

jobs: dict = {}

_COMPLAINT_TAXONOMY = taxonomy_mapping


# Phase 1 — per-row translation + classification

PHASE1_SYSTEM = """You are an expert multilingual analyst specialising in customer complaint data.
Your job is to process batches of customer feedback text (Arabic or English) and return structured JSON."""

def process_batch(texts: list[dict]) -> list[dict]:
    """
    Translate + classify a batch of rows.
    texts: [{"id": int, "text": str}, ...]
    returns: [{"id", "original", "translation", "language", "message_type", "classification",
               "confidence", "sentiment", "complaint_summary", "recommended_action", "score"}, ...]
    """
    user_prompt = f"""Process each entry below. For every entry return:
- "translation": English translation (if already English, copy as-is)
- "language": 2-letter ISO 639-1 code of the original text language (e.g. "EN", "AR", "FR", "DE")
- "message_type": one of complaint | inquiry | feedback | request | praise | other
- "classification": "complaint" or "non-complaint"
- "confidence": "high" | "medium" | "low"
- "sentiment": one of positive | negative | neutral | mixed
- "complaint_summary": for complaints only, a 1-2 sentence summary of the core issue; empty string for non-complaints
- "recommended_action": for complaints only, a single concise action sentence to resolve the issue (e.g. "Escalate to disputes team and initiate chargeback process"); empty string for non-complaints
- "score": integer 1-10 representing urgency/impact of the complaint (10 = highest urgency); use 0 for non-complaints

Input:
{json.dumps(texts, ensure_ascii=False, indent=2)}

Return ONLY a valid JSON array, no markdown, no extra text. Schema:
[{{"id": <int>, "original": "<text>", "translation": "<english>", "language": "<2-letter code>", "message_type": "<type>", "classification": "<classification>", "confidence": "<confidence>", "sentiment": "<sentiment>", "complaint_summary": "<summary>", "recommended_action": "<action>", "score": <int>}}]"""

    response = client.messages.create(
        model=VERTEX_MODEL,
        max_tokens=8192,
        system=PHASE1_SYSTEM,
        messages=[{"role": "user", "content": user_prompt}],
        temperature=0,
    )

    raw = response.content[0].text or ""
    start, end = raw.find("["), raw.rfind("]") + 1
    if start == -1 or end == 0:
        raise RuntimeError(f"No JSON array in model response:\n{raw[:300]}")
    return json.loads(raw[start:end])


# Phase 2 — taxonomy mapping (complaints only)

TAXONOMY_SYSTEM = """You are an expert at mapping customer complaints to a predefined 3-level banking taxonomy.
You must always pick the closest matching category, subcategory, and issue from the provided taxonomy."""


def map_batch_to_taxonomy(rows: list[dict]) -> list[dict]:
    """
    Map complaint rows to the 3-level taxonomy.
    rows: [{"id": int, "text": str}, ...]
    returns: [{"id": int, "taxonomy_main": str, "taxonomy_sub": str, "taxonomy_issue": str}, ...]
    """
    taxonomy_ref = json.dumps(_COMPLAINT_TAXONOMY, ensure_ascii=False, indent=2)
    user_prompt = f"""Map each complaint to the most appropriate 3-level entry in the taxonomy below.

Taxonomy structure: Category -> Subcategory -> [Issue1, Issue2, ...]

Taxonomy:
{taxonomy_ref}

Complaints:
{json.dumps(rows, ensure_ascii=False, indent=2)}

Rules:
- taxonomy_main must be an exact top-level key from the taxonomy (e.g. "Cards").
- taxonomy_sub must be an exact subcategory key under that category (e.g. "Debit Card").
- taxonomy_issue must be an exact string from that subcategory's issue list (e.g. "Card Transactions Dispute").
- Choose the closest match at every level. Never leave any field empty.
- For taxonomy_issue, select the issue that best describes the root problem in the complaint.

Return ONLY a valid JSON array, no markdown, no extra text. Schema:
[{{"id": <int>, "taxonomy_main": "<category>", "taxonomy_sub": "<subcategory>", "taxonomy_issue": "<issue>"}}]"""

    response = client.messages.create(
        model=VERTEX_MODEL,
        max_tokens=4096,
        system=TAXONOMY_SYSTEM,
        messages=[{"role": "user", "content": user_prompt}],
        temperature=0,
    )
    raw = response.content[0].text or ""
    start, end = raw.find("["), raw.rfind("]") + 1
    if start == -1 or end == 0:
        raise RuntimeError(f"No JSON array in taxonomy response:\n{raw[:300]}")
    return json.loads(raw[start:end])


# Phase 3 — dataset-level root cause analysis

PHASE3_SYSTEM = """
You are a senior banking operations, risk, and customer experience analyst.

You specialize in Root Cause Analysis (RCA) for large financial institutions, including retail banking, digital banking, payments, lending, and compliance operations.

Your job is NOT to summarize complaints — your job is to diagnose operational, technical, or policy failures and recommend concrete, high-impact corrective actions.

You MUST think like a bank:
- Identify failures in systems (apps, APIs, core banking, payment gateways)
- Identify failures in processes (SLA breaches, manual handling gaps, escalation issues)
- Identify failures in policies (KYC, compliance, credit rules, fee structures)
- Identify failures in communication (misleading info, missing notifications, unclear UX)

Avoid generic language like:
- "improve service"
- "enhance customer satisfaction"
- "better communication"

Instead, ALWAYS:
- Point to a **specific failure mechanism**
- Describe **why it is happening repeatedly**
- Suggest **clear, implementable actions** (system fix, process fix, policy change)

For each root cause:
- The "root_cause" must describe the immediate observable failure
- The "deeper_root_cause" must describe the systemic issue enabling it (design flaw, lack of monitoring, poor integration, policy gap)

Your analysis must reflect:
- Patterns across complaints (not individual cases)
- Operational impact (delays, financial loss, failed transactions)
- Risk implications (compliance risk, reputational risk, customer churn)

Be precise, technical where needed, and action-oriented.
Return ONLY valid JSON.
"""

def run_rca(classified_rows: list[dict]) -> tuple[str, list[str], list[dict], str, str]:
    """
    Takes the full classified + taxonomy-mapped dataset and returns:
      - a markdown RCA report (for download)
      - a list of distinct root cause category names
      - a structured list of per-category RCA objects
      - a cross-cutting deeper analysis string
      - a collective summary string
    """
    complaints = [r for r in classified_rows if r.get("classification", "").lower() == "complaint"]
    non_complaints = len(classified_rows) - len(complaints)

    # Taxonomy breakdown: {main_cat: {sub_cat: count}}
    taxonomy_breakdown: dict[str, dict[str, int]] = {}
    for r in complaints:
        main = r.get("taxonomy_main", "Uncategorized")
        sub  = r.get("taxonomy_sub",  "Uncategorized")
        taxonomy_breakdown.setdefault(main, {})
        taxonomy_breakdown[main][sub] = taxonomy_breakdown[main].get(sub, 0) + 1

    # Message type distribution across all rows
    message_type_breakdown: dict[str, int] = {}
    for r in classified_rows:
        mtype = r.get("message_type", "other")
        message_type_breakdown[mtype] = message_type_breakdown.get(mtype, 0) + 1

    # Up to 40 sample complaint translations grouped by taxonomy main category
    samples_by_category: dict[str, list[str]] = {}
    for r in complaints[:40]:
        main = r.get("taxonomy_main", "Uncategorized")
        samples_by_category.setdefault(main, [])
        samples_by_category[main].append(r.get("translation", ""))

    summary = {
        "total_records": len(classified_rows),
        "complaints": len(complaints),
        "non_complaints": non_complaints,
        "message_type_breakdown": message_type_breakdown,
        "taxonomy_breakdown": taxonomy_breakdown,
        "sample_complaint_translations_by_category": samples_by_category,
    }

    summary_json = json.dumps(summary, ensure_ascii=False, indent=2)

    # Call A: structured data
    structured_prompt = f"""
    You are given a dataset summary of customer complaints that have already been:
    - Translated
    - Classified into banking taxonomy categories
    
    Your task is to perform a BANK-GRADE ROOT CAUSE ANALYSIS.
    
    Dataset summary:
    {summary_json}
    
    IMPORTANT INSTRUCTIONS:
    
    1. Base your analysis ONLY on patterns visible in the dataset summary.
    2. Do NOT invent causes unrelated to the complaints.
    3. Avoid vague statements — every point must be specific and operationally meaningful.
    4. Think in terms of:
       - System failures (mobile app, backend, APIs, payment processors)
       - Process failures (delays, manual handling, poor escalation)
       - Policy failures (fees, KYC rules, eligibility logic)
       - Communication gaps (SMS/email failures, unclear messaging)
    
    ---
    
    Respond with a JSON object with EXACTLY four keys:
    
    ---
    
    1. "categories":
    - JSON array of EXACT taxonomy main category names that have complaints.
    
    ---
    
    2. "rca_structured":
    - JSON array with ONE object per category:
    
    Each object MUST follow:
    
    {{
      "category": "<exact taxonomy main category name>",
    
      "root_cause": "<ONE precise sentence describing the direct failure (e.g., 'Delayed loan processing due to manual verification backlog and lack of automation')>",
    
      "deeper_root_cause": "<ONE precise sentence describing the systemic issue (e.g., 'Absence of workflow automation and poor load balancing across verification teams leading to recurring bottlenecks')>",
    
      "issue_breakdown": [
        "<specific recurring issue pattern observed in complaints>",
        "<another concrete pattern>",
        "<another concrete pattern>"
      ],
    
      "next_actions": [
        "<clear, actionable fix tied to system/process/policy (e.g., 'Implement automated document verification using OCR + rule engine')>",
        "<another concrete action (e.g., 'Introduce SLA tracking dashboard for loan approvals')>",
        "<another action (e.g., 'Add real-time status tracking for customers in mobile app')>"
      ]
    }}
    
    RULES:
    - Issue breakdown MUST reflect observable complaint patterns (not guesses)
    - Next actions MUST be practical and implementable by a bank team (engineering, ops, or policy)
    - Avoid repeating the same generic actions across categories
    
    ---
    
    3. "collective_summary":
    - 3–4 sentences written as an EXECUTIVE BRIEFING
    - MUST include:
      - Total complaint volume (if available)
      - Which categories dominate
      - What the overall failure pattern is
      - The SINGLE most critical issue affecting the bank
    
    ---
    
    4. "deeper_analysis":
    - 2–3 sentences identifying ONE cross-cutting systemic problem across ALL categories
    - This should reflect a structural weakness such as:
      - poor system integration
      - lack of automation
      - weak monitoring/alerting
      - fragmented customer communication systems
    
    ---
    
    FINAL RULES:
    - Output MUST be valid JSON
    - NO markdown
    - NO explanations outside JSON
    - NO generic statements
    """

    structured_raw = client.messages.create(
        model=VERTEX_MODEL,
        max_tokens=8192,
        system=PHASE3_SYSTEM,
        messages=[{"role": "user", "content": structured_prompt}],
        temperature=0.3,
    ).content[0].text or ""

    structured_raw = structured_raw.strip()
    if structured_raw.startswith("```"):
        structured_raw = structured_raw.split("\n", 1)[-1]
        structured_raw = structured_raw.rsplit("```", 1)[0].strip()

    try:
        parsed             = json.loads(structured_raw)
        categories         = [str(c) for c in parsed.get("categories", []) if c]
        rca_structured     = [r for r in parsed.get("rca_structured", []) if isinstance(r, dict)]
        deeper_analysis    = str(parsed.get("deeper_analysis", "")).strip()
        collective_summary = str(parsed.get("collective_summary", "")).strip()
    except (json.JSONDecodeError, AttributeError):
        categories         = list(taxonomy_breakdown.keys())
        rca_structured     = []
        deeper_analysis    = ""
        collective_summary = ""

    # Call B: markdown report (download only)
    report_prompt = f"""Dataset summary with taxonomy mapping:
{summary_json}

Write a full Root Cause Analysis report in Markdown with these sections:
1. Executive Summary
2. Taxonomy-Based Complaint Distribution
3. Root Cause Analysis by Taxonomy Category
4. Recommendations
5. Data Quality Notes

Return ONLY the Markdown text, no JSON, no extra wrapping."""

    report_raw = client.messages.create(
        model=VERTEX_MODEL,
        max_tokens=8192,
        system=PHASE3_SYSTEM,
        messages=[{"role": "user", "content": report_prompt}],
        temperature=0.3,
    ).content[0].text or ""

    report = report_raw.strip() or "No RCA output returned."

    return report, categories, rca_structured, deeper_analysis, collective_summary

# Background job

def _run_job(job_id: str, texts: list[dict], df: pd.DataFrame, source_column: str) -> None:
    total_batches = math.ceil(len(texts) / BATCH_SIZE)
    batches = [texts[i * BATCH_SIZE:(i + 1) * BATCH_SIZE] for i in range(total_batches)]
    classified: list[dict] = []

    try:
        jobs[job_id].update({"phase": 1, "total": total_batches})

        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
            future_map = {executor.submit(process_batch, b): i for i, b in enumerate(batches)}
            for future in as_completed(future_map):
                batch_idx = future_map[future]
                try:
                    classified.extend(future.result())
                except Exception as exc:
                    jobs[job_id].update({"status": "error", "detail": f"Batch {batch_idx + 1} failed: {exc}"})
                    return
                jobs[job_id]["progress"] += 1

        jobs[job_id].update({"phase": "1.5", "taxonomy_status": "running"})

        complaint_rows = [r for r in classified if r.get("classification", "").lower() == "complaint"]
        taxonomy_map: dict[int, dict] = {}

        if complaint_rows:
            tax_inputs = [
                {"id": r["id"], "text": r.get("translation") or r.get("original", "")}
                for r in complaint_rows
            ]
            tax_batches = [
                tax_inputs[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
                for i in range(math.ceil(len(tax_inputs) / BATCH_SIZE))
            ]

            with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
                tax_futures = {executor.submit(map_batch_to_taxonomy, b): b for b in tax_batches}
                for future in as_completed(tax_futures):
                    try:
                        for entry in future.result():
                            taxonomy_map[entry["id"]] = entry
                    except Exception as exc:
                        jobs[job_id].update({"status": "error", "detail": f"Taxonomy mapping failed: {exc}"})
                        return

        for r in classified:
            if r.get("classification", "").lower() == "complaint":
                tax = taxonomy_map.get(r["id"], {})
                r["taxonomy_main"]  = tax.get("taxonomy_main",  "Uncategorized")
                r["taxonomy_sub"]   = tax.get("taxonomy_sub",   "Uncategorized")
                r["taxonomy_issue"] = tax.get("taxonomy_issue", "Uncategorized")
            else:
                r["taxonomy_main"]  = ""
                r["taxonomy_sub"]   = ""
                r["taxonomy_issue"] = ""

        jobs[job_id]["taxonomy_status"] = "done"

        result_map = {r["id"]: r for r in classified}
        output_rows = []
        for idx, row in df.iterrows():
            out = {}
            for k, v in row.to_dict().items():
                col_name = "Original Text" if k == source_column else k
                out[col_name] = "" if pd.isna(v) else v
            a = result_map.get(idx, {})
            out["Translation (EN)"]     = a.get("translation", "")
            out["Message Type"]         = a.get("message_type", "")
            out["Complaint Summary"]    = a.get("complaint_summary", "")
            out["Sentiment"]            = a.get("sentiment", "")
            out["Taxonomy Category"]    = a.get("taxonomy_main", "")
            out["Taxonomy Subcategory"] = a.get("taxonomy_sub", "")
            out["Taxonomy Issue"]       = a.get("taxonomy_issue", "")
            out["Recommended Action"]   = a.get("recommended_action", "")
            out["Score"]                = a.get("score", 0)
            out["Classification"]       = a.get("classification", "")
            out["Confidence"]           = a.get("confidence", "")
            out["Language"]             = a.get("language", "")
            output_rows.append(out)

        jobs[job_id].update({"phase": 2, "phase2_status": "running"})
        rca_report, rca_categories, rca_structured, deeper_analysis, collective_summary = run_rca(classified)

        jobs[job_id].update({
            "status": "done",
            "phase2_status": "done",
            "data": output_rows,
            "rca_report": rca_report,
            "rca_categories": rca_categories,
            "rca_structured": rca_structured,
            "deeper_analysis": deeper_analysis,
            "collective_summary": collective_summary,
            "total_rows": len(output_rows),
            "processed_rows": len(classified),
        })

    except Exception as exc:
        jobs[job_id].update({"status": "error", "detail": str(exc)})


# routes

@app.get("/")
def root():
    return {"message": "AI RCA backend running. See /docs."}


@app.get("/health")
def health():
    return {
        "status": "ok",
        "model": VERTEX_MODEL,
        "project": VERTEX_PROJECT,
        "location": VERTEX_LOCATION,
        "batch_size": BATCH_SIZE,
        "max_concurrent": MAX_CONCURRENT,
    }


@app.post("/process")
async def process_file(file: UploadFile = File(...), column: str = Form(...)):
    content = await file.read()
    try:
        df = _read_file(content, file.filename)
    except ValueError as exc:
        raise HTTPException(400, str(exc))

    if column not in df.columns:
        raise HTTPException(400, f"Column '{column}' not found. Available: {df.columns.tolist()}")

    texts = [
        {"id": idx, "text": str(val).strip()}
        for idx, val in enumerate(df[column])
        if pd.notna(val) and str(val).strip()
    ]
    if not texts:
        raise HTTPException(400, "No valid text found in the selected column.")

    total_batches = math.ceil(len(texts) / BATCH_SIZE)
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "running", "phase": 1,
        "progress": 0, "total": total_batches,
        "taxonomy_status": "pending",
        "phase2_status": "pending",
        "data": None, "rca_report": None, "detail": None,
    }

    threading.Thread(target=_run_job, args=(job_id, texts, df, column), daemon=True).start()
    return {"job_id": job_id, "total_batches": total_batches, "total_rows": len(texts)}


@app.get("/status/{job_id}")
def get_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found.")
    return job

# Helper Functions

def _read_file(content: bytes, filename: str) -> pd.DataFrame:
    name = (filename or "").lower()
    if name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(content))
    elif name.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(content))
    raise ValueError("Unsupported file type. Upload a .csv or .xlsx/.xls file.")
