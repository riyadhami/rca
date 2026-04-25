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


# ── Taxonomy compact formatter ────────────────────────────────────────────────

def _format_taxonomy_compact(taxonomy: dict) -> str:
    """Render taxonomy as a compact, token-efficient reference for prompts."""
    lines = []
    for cat, subs in taxonomy.items():
        lines.append(f"▸ {cat}")
        for sub, issues in subs.items():
            issue_str = " | ".join(i for i in issues if i) if issues else "—"
            lines.append(f"    • {sub}: [{issue_str}]")
    return "\n".join(lines)


_TAXONOMY_COMPACT = _format_taxonomy_compact(_COMPLAINT_TAXONOMY)


# ── Phase 1 — translate + classify + taxonomy (merged, one call per batch) ────

COMBINED_SYSTEM = """You are an expert multilingual banking analyst for a Middle Eastern retail bank.

You process batches of customer messages — complaints, inquiries, requests, and feedback in Arabic or English — and return structured JSON with three responsibilities per message:

1. TRANSLATION & LANGUAGE DETECTION
2. CLASSIFICATION & QUALITY ANALYSIS
3. TAXONOMY MAPPING

━━━ CLASSIFICATION RULES ━━━
- "complaint": reports a failure, error, or dissatisfaction — even if phrased as a question.
  Examples: "Why was I charged twice?" → complaint. "How could you block my account without warning?" → complaint.
- "inquiry": genuine information request with no embedded grievance.
- "request": asks for an action to be performed (open account, issue card, reset PIN, etc.).
- "feedback" / "praise" / "other": use only when clearly not any of the above.
- message_type captures the nuance; classification is always "complaint" or "non-complaint".

━━━ COMPLAINT SUMMARY QUALITY RULES (complaint_summary field) ━━━
Write a specific, informative summary for EVERY message type — never leave blank.

Complaints — name the exact product/service, the failure, and the customer impact:
  ✗ BAD:  "Customer is having trouble with their card."
  ✓ GOOD: "Debit card declined at two separate POS terminals on the same day despite confirmed available balance; second occurrence this week."

Inquiries — state exactly what the customer needs to know or accomplish:
  ✗ BAD:  "Customer is asking about their account."
  ✓ GOOD: "Customer wants to know how to register their newly received debit card in the mobile app for contactless payments."

Requests — state the exact action being sought:
  ✗ BAD:  "Customer wants something done."
  ✓ GOOD: "Customer requesting a second debit card issued under a child account for their daughter."

━━━ RECOMMENDED ACTION QUALITY RULES (recommended_action field) ━━━
Always name the responsible team/system AND the exact next step.
  ✗ BAD:  "Investigate the issue."
  ✓ GOOD: "Route to Debit Card Operations; pull POS decline reason codes from acquirer logs and verify card status in card management system."
For non-complaints:
  ✓ GOOD: "Provide step-by-step mobile app card registration guide; confirm card eligibility for digital wallet."

━━━ TAXONOMY MAPPING STRATEGY (follow these steps — do NOT skip) ━━━
Step 1 — Read the FULL message and identify: (a) the banking PRODUCT involved, and (b) the specific FAILURE or REQUEST.
         Then find the sub_category that best describes that specific failure or request.
         Sub_category match is MORE IMPORTANT than the category name — derive the category from the sub_category, not independently.
Step 2 — Once taxonomy_sub is chosen, look up which taxonomy category owns it.
         Assign that as taxonomy_main. Never assign a category that does not contain the chosen sub_category.
Step 3 — Select taxonomy_issue: the exact string under that sub_category that best describes the problem.
         When multiple issues fit, choose the most specific one.

Additional mapping rules:
- Map by TOPIC, not by phrasing — an inquiry about a blocked PIN maps identically to a complaint about a blocked PIN.
- Sub_categories that represent an amount threshold ("Amount Below 20,000 OMR", "Amount Above 20,000 OMR", "Amount Below 250,000 OMR", etc.) MUST only be used when the message explicitly states a specific monetary figure. Without an explicit amount, choose a descriptive sub_category instead (e.g., "Outgoing Funds Transfer").
- All three taxonomy fields MUST be exact strings from the taxonomy. No invented or modified values.
- Never leave any taxonomy field empty; when uncertain, pick the closest semantic match.

Return ONLY valid JSON. No markdown. No extra text."""


def process_batch(texts: list[dict]) -> list[dict]:
    """
    Translate, classify, and map to taxonomy in a single LLM call.
    texts: [{"id": int, "text": str}, ...]
    returns: [{"id", "original", "translation", "language", "message_type",
               "classification", "confidence", "sentiment", "complaint_summary",
               "recommended_action", "score",
               "taxonomy_main", "taxonomy_sub", "taxonomy_issue"}, ...]
    """
    user_prompt = f"""Process each message below. Return ALL fields for every entry.

━━━ TAXONOMY REFERENCE (use EXACT strings) ━━━
{_TAXONOMY_COMPACT}

━━━ FEW-SHOT TAXONOMY MAPPING EXAMPLES ━━━
Read the FULL sentence. Map based on INTENT and PRODUCT, not isolated keywords.
The product name IS the top-level category (Debit Card, Child Account, Mobile Banking, Savings Account…).
Old parent groupings like "Cards", "Accounts", "Funds Transfer" NO LONGER EXIST as categories.

── ACCOUNT TRANSACTIONS vs ACCOUNT STATUS ──
Both live under Savings Account / Current Account — do NOT confuse them:
• "Account transactions" → money movement issues: deposit not showing, unknown debit, wrong charge
• "Account Status"       → the account's state: blocked, closed, frozen, not yet opened, dormant

MSG: "لقد وضعت بعض المال في حسابي في اليوم الآخر ولم يتم إدخاله بعد، هل يمكنك المساعدة؟"
     (I deposited money into my account the other day and it hasn't been credited yet.)
→ message_type: complaint | taxonomy_main: "Savings Account" | taxonomy_sub: "Account transactions" | taxonomy_issue: "Unknown Debit/ Credit"
  REASON: deposit not appearing = a TRANSACTION issue, not an account state issue. Use Account transactions.

MSG: "I noticed an unexpected deduction from my savings account that I did not authorise."
→ message_type: complaint | taxonomy_main: "Savings Account" | taxonomy_sub: "Account transactions" | taxonomy_issue: "Unknown Debit/ Credit"

MSG: "My savings account has been frozen and I cannot make any transactions."
→ message_type: complaint | taxonomy_main: "Savings Account" | taxonomy_sub: "Account Status" | taxonomy_issue: "Account not Closed"
  REASON: account is frozen (its STATE), not a money movement problem. Use Account Status.

── CARD REQUESTS: own card vs child's card ──

MSG: "I want to request an additional debit card for myself."
→ message_type: request | taxonomy_main: "Debit Card" | taxonomy_sub: "Request" | taxonomy_issue: "Request To Issue Normal Debit Card"

MSG: "هل يمكن إعطاء بطاقة ثانية لهذا الحساب لابنتي؟"  (Can I get a second card on this account for my daughter?)
→ message_type: request | taxonomy_main: "Child Account" | taxonomy_sub: "Account Status" | taxonomy_issue: "Child Account not Linked to Guardian"
  REASON: "card" + "daughter/son/child/minor" always signals a Child Account product, NOT Debit Card.
          The guardian is requesting access to the child's account — Child Account is the product category.

── TRANSFERS: direction and destination ──

MSG: "I transferred money from my savings account to my current account but it hasn't been credited."
→ message_type: complaint | taxonomy_main: "Own Accounts" | taxonomy_sub: "Outgoing Funds Transfer" | taxonomy_issue: "Transfer Request not Done"
  REASON: Own Accounts = transfers strictly between the customer's OWN accounts.

MSG: "My colleague sent me money two days ago but it still hasn't appeared in my account."
→ message_type: complaint | taxonomy_main: "Other Accounts Within Oman" | taxonomy_sub: "Incoming Funds Transfer" | taxonomy_issue: "Request for Further Transaction Details"
  REASON: Money arriving FROM another person → Incoming Funds Transfer, not Outgoing.

── OTHER COMMON MAPPINGS ──

MSG: "I've received my card — how do I add it to the mobile app?"
→ message_type: inquiry | taxonomy_main: "Mobile Banking" | taxonomy_sub: "Registration / log in Issues" | taxonomy_issue: "Unable to register"

MSG: "Where do I go to unblock my PIN?"
→ message_type: inquiry | taxonomy_main: "Debit Card" | taxonomy_sub: "E-PIN Enquiry" | taxonomy_issue: "Enquiry on Create /Reset PIN for Debit Card"

MSG: "My credit card was declined at a shop despite having available credit limit."
→ message_type: complaint | taxonomy_main: "Credit Card" | taxonomy_sub: "Card Status" | taxonomy_issue: "Card not Working"

MSG: "My loan installment was debited twice this month."
→ message_type: complaint | taxonomy_main: "Consumer Loan" | taxonomy_sub: "Issues Related to Installments, Interest and Charges" | taxonomy_issue: "Multiple Installments Taken (Hovering )"

MSG: "I need to know the remaining balance on my personal loan."
→ message_type: inquiry | taxonomy_main: "Loans/ Financing" | taxonomy_sub: "Enquiry on Loan Related" | taxonomy_issue: "Enquiry on Loan Facilities"

━━━ SPECIFIC TERM SHORTCUTS ━━━
Only use these when the term appears — they do NOT override the full-sentence reading above.

"daughter" / "son" / "child" / "minor" / "لابنتي" / "لابني" (even if "card" also appears)
  → taxonomy_main: "Child Account"

"OTP" / "one-time password" / "SMS verification code"
  → taxonomy_main: "Mobile Banking" | taxonomy_sub: "OTP Related" | taxonomy_issue: "Not receiving OTP"

"ATM" + cash not received / money taken / not dispensed
  → taxonomy_main: "Bank Muscat Devices" | taxonomy_sub: "Cash withdrawal" | taxonomy_issue: "Cash not received from Bank Muscat ATM"

"cheque" / "check"
  → taxonomy_main: "Bank Muscat Cheques" | taxonomy_sub: "Cheque Transactions"

"standing instruction" / "scheduled auto payment"
  → taxonomy_main: "Standing Instructions" | taxonomy_sub: "Outgoing Funds Transfer"

━━━ OUTPUT SCHEMA (return a JSON array with one object per input) ━━━
{{
  "id": <int — must match input id exactly>,
  "original": "<original text verbatim>",
  "translation": "<English translation; copy if already English>",
  "language": "<2-letter ISO 639-1 code, e.g. AR / EN>",
  "message_type": "<complaint | inquiry | request | feedback | praise | other>",
  "classification": "<complaint | non-complaint>",
  "confidence": "<high | medium | low>",
  "sentiment": "<positive | negative | neutral | mixed>",
  "complaint_summary": "<specific 1-2 sentence summary for ALL types — see quality rules>",
  "recommended_action": "<named-team action — see quality rules>",
  "taxonomy_main": "<exact category from taxonomy>",
  "taxonomy_sub": "<exact subcategory from taxonomy>",
  "taxonomy_issue": "<exact issue from taxonomy>"
}}

━━━ INPUT ━━━
{json.dumps(texts, ensure_ascii=False, indent=2)}

Return ONLY the JSON array. No markdown fences. No extra keys."""

    response = client.messages.create(
        model=VERTEX_MODEL,
        max_tokens=8192,
        system=COMBINED_SYSTEM,
        messages=[{"role": "user", "content": user_prompt}],
        temperature=0,
    )

    raw = response.content[0].text or ""
    start, end = raw.find("["), raw.rfind("]") + 1
    if start == -1 or end == 0:
        raise RuntimeError(f"No JSON array in model response:\n{raw[:300]}")
    return json.loads(raw[start:end])


# ── Phase 3 — dataset-level root cause analysis ───────────────────────────────

PHASE3_SYSTEM = """You are a senior banking operations, risk, and customer experience analyst at a Gulf-region retail bank.

Your role is ROOT CAUSE ANALYSIS — not complaint summarisation.

Diagnosis standards:
• Identify the SPECIFIC failure mechanism (not "system issue" — say "OTP delivery failure due to SMS gateway timeout under peak load")
• Explain WHY it recurs (design flaw, missing monitoring, policy gap, process bottleneck)
• Recommend IMPLEMENTABLE actions tied to a named team, system, or policy (not "improve service")

Failure categories to consider:
• System/API failures: mobile app crashes, payment gateway timeouts, core banking sync delays
• Process failures: manual queue bottlenecks, SLA breaches, missing escalation paths
• Policy failures: KYC rule gaps, credit eligibility logic errors, fee calculation bugs
• Communication failures: SMS/push notification gaps, unclear UX copy, missing status updates

Tone: precise, technical where needed, action-oriented. No generic management-speak.
Output: ONLY valid JSON, no markdown fences."""

def _build_rca_markdown(
    rca_structured: list[dict],
    collective_summary: str,
    deeper_analysis: str,
    cat_counts: dict,
    sub_counts_by_cat: dict,
    total: int,
    complaints: int,
) -> str:
    """Generate the downloadable RCA markdown report locally — no LLM call needed."""
    lines = [
        "# Root Cause Analysis Report",
        "",
        "## Executive Summary",
        "",
        collective_summary or "—",
        "",
        f"**Total records analysed:** {total}  ",
        f"**Complaints:** {complaints}  ",
        f"**Non-complaints:** {total - complaints}",
        "",
        "---",
        "",
        "## Cross-Cutting Systemic Theme",
        "",
        deeper_analysis or "—",
        "",
        "---",
        "",
        "## Complaint Distribution by Category",
        "",
    ]
    for cat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1]):
        lines.append(f"- **{cat}**: {cnt} complaint{'s' if cnt != 1 else ''}")
        for sub, sc in sub_counts_by_cat.get(cat, {}).items():
            lines.append(f"    - {sub}: {sc}")
    lines += ["", "---", "", "## Root Cause Analysis by Category", ""]

    for entry in rca_structured:
        cat = entry.get("category", "Unknown")
        lines += [
            f"### {cat}",
            "",
            f"**Root Cause:** {entry.get('root_cause', '—')}",
            "",
            f"**Deeper Root Cause:** {entry.get('deeper_root_cause', '—')}",
            "",
            "**Issue Breakdown:**",
        ]
        for iss in entry.get("issue_breakdown", []):
            lines.append(f"- {iss}")
        lines += ["", "**Recommended Actions:**"]
        for k, act in enumerate(entry.get("next_actions", []), 1):
            lines.append(f"{k}. {act}")
        lines += ["", "---", ""]

    return "\n".join(lines)


def run_rca(classified_rows: list[dict]) -> tuple[str, list[str], list[dict], str, str]:
    """
    Perform dataset-level RCA in a single LLM call using enriched complaint summaries.
    Returns: (markdown_report, categories, rca_structured, deeper_analysis, collective_summary)
    """
    complaints = [r for r in classified_rows if r.get("classification", "").lower() == "complaint"]

    # Taxonomy breakdown: {main_cat: {sub_cat: count}}
    taxonomy_breakdown: dict[str, dict[str, int]] = {}
    for r in complaints:
        main = r.get("taxonomy_main", "Uncategorized")
        sub  = r.get("taxonomy_sub",  "Uncategorized")
        taxonomy_breakdown.setdefault(main, {}).setdefault(sub, 0)
        taxonomy_breakdown[main][sub] += 1

    # Message type distribution
    message_type_breakdown: dict[str, int] = {}
    for r in classified_rows:
        mtype = r.get("message_type", "other")
        message_type_breakdown[mtype] = message_type_breakdown.get(mtype, 0) + 1

    # Per-category: up to 6 complaint summaries + top recommended actions
    # Use complaint_summary (high-quality) not raw translation
    evidence_by_category: dict[str, dict] = {}
    for r in complaints:
        main = r.get("taxonomy_main", "Uncategorized")
        sub  = r.get("taxonomy_sub",  "")
        if main not in evidence_by_category:
            evidence_by_category[main] = {"summaries": [], "actions": [], "subcategories": set()}
        ev = evidence_by_category[main]
        if len(ev["summaries"]) < 6 and r.get("complaint_summary"):
            ev["summaries"].append(r["complaint_summary"])
        if len(ev["actions"]) < 4 and r.get("recommended_action"):
            ev["actions"].append(r["recommended_action"])
        if sub:
            ev["subcategories"].add(sub)

    # Serialise sets for JSON
    for ev in evidence_by_category.values():
        ev["subcategories"] = list(ev["subcategories"])

    dataset_context = {
        "total_records": len(classified_rows),
        "complaints": len(complaints),
        "non_complaints": len(classified_rows) - len(complaints),
        "message_type_breakdown": message_type_breakdown,
        "taxonomy_breakdown": taxonomy_breakdown,
        "evidence_by_category": evidence_by_category,
    }

    prompt = f"""You are given a structured dataset of classified customer messages from a Gulf-region retail bank.

Dataset context:
{json.dumps(dataset_context, ensure_ascii=False, indent=2)}

The "evidence_by_category" field contains:
- "summaries": up to 6 pre-written complaint summaries per category (specific, already refined)
- "actions": individual recommended actions from row-level analysis (reflect recurring patterns)
- "subcategories": the taxonomy sub-categories present in complaints for this category

━━━ YOUR TASK ━━━

Perform a BANK-GRADE ROOT CAUSE ANALYSIS. Produce a JSON object with EXACTLY these 4 keys:

"categories": array of taxonomy category names that have complaints (exact strings from taxonomy_breakdown keys).

"rca_structured": array with ONE entry per category. Each entry:
{{
  "category": "<exact category name>",
  "root_cause": "<ONE sentence: the specific, observable failure — e.g. 'OTP delivery to registered mobile numbers failing intermittently during peak hours due to SMS gateway queue overflow'>",
  "deeper_root_cause": "<ONE sentence: the systemic root cause enabling the failure — e.g. 'No circuit-breaker or fallback SMS provider configured; single-point dependency on gateway with no auto-scaling'>",
  "issue_breakdown": [
    "<concrete recurring pattern derived from the summaries — not a generic statement>",
    "<another pattern>",
    "<another pattern>"
  ],
  "next_actions": [
    "<implementable action with named owner — e.g. 'Digital Banking team: add secondary SMS gateway failover with automatic routing on >5% delivery failure rate'>",
    "<another action>",
    "<another action>"
  ]
}}

Rules for issue_breakdown and next_actions:
- MUST be derived directly from the summaries and subcategories provided
- Each point must name the specific product, channel, or failure — not vague descriptions
- next_actions must name the team (Engineering, Operations, Compliance, Product, CX, etc.) and the exact fix
- Do NOT repeat the same action across categories

"collective_summary": 3-4 sentence executive briefing. Must state: total complaint count, top 2 categories by volume, the dominant failure pattern, and the single most urgent risk.

"deeper_analysis": 2-3 sentences identifying ONE cross-cutting systemic weakness visible across multiple categories (e.g. absent real-time monitoring, fragmented notification systems, no automated retry logic).

OUTPUT: ONLY valid JSON. No markdown. No extra keys. No generic language."""

    raw = client.messages.create(
        model=VERTEX_MODEL,
        max_tokens=8192,
        system=PHASE3_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    ).content[0].text or ""

    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

    try:
        parsed             = json.loads(raw)
        categories         = [str(c) for c in parsed.get("categories", []) if c]
        rca_structured     = [r for r in parsed.get("rca_structured", []) if isinstance(r, dict)]
        deeper_analysis    = str(parsed.get("deeper_analysis", "")).strip()
        collective_summary = str(parsed.get("collective_summary", "")).strip()
    except (json.JSONDecodeError, AttributeError):
        categories         = list(taxonomy_breakdown.keys())
        rca_structured     = []
        deeper_analysis    = ""
        collective_summary = ""

    cat_counts = {cat: sum(subs.values()) for cat, subs in taxonomy_breakdown.items()}
    report = _build_rca_markdown(
        rca_structured, collective_summary, deeper_analysis,
        cat_counts, taxonomy_breakdown,
        len(classified_rows), len(complaints),
    )
    return report, categories, rca_structured, deeper_analysis, collective_summary


# ── Background job ────────────────────────────────────────────────────────────

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

        # Fill in any IDs the LLM silently dropped — ensures output rows == input rows
        classified_ids = {r["id"] for r in classified}
        texts_by_id = {t["id"]: t["text"] for t in texts}
        for missing_id in sorted(set(texts_by_id) - classified_ids):
            classified.append({
                "id": missing_id,
                "original": texts_by_id[missing_id],
                "translation": texts_by_id[missing_id],
                "language": "",
                "message_type": "other",
                "classification": "non-complaint",
                "confidence": "low",
                "sentiment": "neutral",
                "complaint_summary": "",
                "recommended_action": "",
                "score": 0,
                "taxonomy_main": "",
                "taxonomy_sub": "",
                "taxonomy_issue": "",
            })

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

    # Reset to 0-based index so df.iterrows() and texts IDs stay in sync
    df = df.reset_index(drop=True)

    texts = [
        {"id": int(idx), "text": str(val).strip()}
        for idx, val in df[column].items()
        if pd.notna(val) and str(val).strip()
    ]
    if not texts:
        raise HTTPException(400, "No valid text found in the selected column.")

    total_batches = math.ceil(len(texts) / BATCH_SIZE)
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "running", "phase": 1,
        "progress": 0, "total": total_batches,
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
