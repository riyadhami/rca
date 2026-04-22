import io
import os
import time

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

BACKEND_URL   = os.getenv("BACKEND_URL", "http://localhost:8000")
POLL_INTERVAL = 2
PALETTE = ['#2563eb','#ef4444','#f59e0b','#8b5cf6','#14b8a6','#10b981','#6366f1','#f97316','#94a3b8']

st.set_page_config(
    page_title="RCA Intelligence",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Global CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

/* KPI Row */
.rca-kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:20px}
.rca-kpi-card{background:#1e2028;border:1px solid #31333f;border-radius:10px;padding:18px 20px;position:relative;overflow:hidden}
.rca-kpi-card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px}
.rca-kpi-blue::before{background:#2563eb}
.rca-kpi-red::before{background:#ef4444}
.rca-kpi-amber::before{background:#f59e0b}
.rca-kpi-green::before{background:#10b981}
.rca-kpi-label{font-size:11.5px;font-weight:600;color:#8b9197;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px}
.rca-kpi-value{font-size:32px;font-weight:700;color:#fafafa;line-height:1}
.rca-kpi-sub{font-size:12px;color:#8b9197;margin-top:6px}

/* Summary Banner */
.rca-summary-banner{background:linear-gradient(135deg,#1e3a5f 0%,#2563eb 100%);border-radius:10px;padding:24px 28px;color:white;margin-bottom:20px;display:flex;gap:20px;align-items:flex-start}
.rca-summary-icon{width:48px;height:48px;min-width:48px;background:rgba(255,255,255,.15);border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:24px}
.rca-summary-title{font-size:16px;font-weight:700;margin-bottom:8px}
.rca-summary-text{font-size:13.5px;line-height:1.65;color:rgba(255,255,255,.85);margin:0}

/* Category Bars */
.rca-cat-bars{display:flex;flex-direction:column;gap:10px;padding-top:4px}
.rca-cat-row{display:flex;align-items:center;gap:10px}
.rca-cat-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0}
.rca-cat-name{flex:1;font-size:13px;font-weight:500;color:#fafafa;min-width:60px}
.rca-cat-bar-wrap{flex:2;background:#31333f;border-radius:4px;height:7px;overflow:hidden}
.rca-cat-bar{height:100%;border-radius:4px}
.rca-cat-count{font-size:12px;font-weight:600;color:#8b9197;width:32px;text-align:right}


/* Category Analysis Cards */
.rca-cat-card{background:#1e2028;border:1px solid #31333f;border-radius:10px;overflow:hidden;height:100%}
.rca-cat-color-strip{height:4px}
.rca-cat-header{padding:14px 18px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #31333f;gap:8px}
.rca-cat-title{font-size:14px;font-weight:700;color:#fafafa}
.rca-cat-badges{display:flex;gap:6px;align-items:center;flex-shrink:0}
.rca-cat-body{padding:16px 18px}
.rca-block{margin-bottom:14px}
.rca-label{font-size:10.5px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:#8b9197;margin-bottom:5px}
.rca-root-cause{font-size:13px;font-weight:600;color:#e2e8f0;line-height:1.5;padding:8px 12px;background:#12141a;border-left:3px solid #2563eb;border-radius:0 4px 4px 0;margin:0}
.rca-issues{list-style:none;padding:0;margin:0;display:flex;flex-direction:column;gap:5px}
.rca-issues li{font-size:12.5px;color:#cbd5e1;display:flex;align-items:flex-start;gap:7px;line-height:1.5}
.rca-issues li::before{content:"·";color:#60a5fa;font-weight:700;font-size:18px;line-height:1.2;flex-shrink:0}
.rca-solution{font-size:12.5px;color:#86efac;background:#0d2318;border:1px solid #166534;border-radius:6px;padding:10px 12px;line-height:1.9;margin:0}
.rca-solution ol{margin:0;padding-left:18px}
.rca-solution li{margin-bottom:4px}

/* Badges */
.rca-badge{display:inline-block;font-size:11px;font-weight:600;padding:3px 9px;border-radius:20px;text-transform:uppercase;letter-spacing:.3px}
.rca-badge-critical{background:#450a0a;color:#fca5a5}
.rca-badge-noncritical{background:#052e16;color:#86efac}
.rca-count-badge{font-size:12px;font-weight:600;padding:3px 10px;border-radius:20px;background:#31333f;color:#8b9197}
.rca-section-title{font-size:13px;font-weight:700;color:#8b9197;text-transform:uppercase;letter-spacing:1px;margin:0 0 14px 0}

/* Subcategory pills */
.sub-pill{display:inline-block;font-size:10px;font-weight:500;padding:1px 7px;border-radius:20px;
          border:1px solid;margin:0 4px 3px 0;cursor:default;white-space:nowrap;
          transition:box-shadow .18s ease,border-color .18s ease}
.sub-pill:hover{box-shadow:0 0 0 2.5px var(--pill-glow)}

/* Hide Streamlit toolbar so it never overlaps content */
header[data-testid="stHeader"]{display:none!important}
#MainMenu{display:none!important}
footer{display:none!important}

/* Tighten Streamlit page padding */
.block-container{padding-top:1.5rem!important;padding-left:1.8rem!important;padding-right:1.8rem!important}
</style>
""", unsafe_allow_html=True)

# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("System Status")
    if st.button("Check Backend", use_container_width=True):
        try:
            r = requests.get(f"{BACKEND_URL}/health", timeout=5)
            st.success("Online") if r.status_code == 200 else st.error(f"HTTP {r.status_code}")
            if r.status_code == 200:
                st.json(r.json())
        except Exception as exc:
            st.error(f"Unreachable: {exc}")
    st.caption(f"Backend: `{BACKEND_URL}`")


def _rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


# ── Results renderer ─────────────────────────────────────────────────────────
def render_results(job: dict) -> None:
    df_result = pd.DataFrame(job["data"])

    # Derived values
    total_rows         = job["total_rows"]
    rca_structured     = job.get("rca_structured", [])
    rca_categories     = job.get("rca_categories", [])
    collective_summary = job.get("collective_summary", "")
    deeper_analysis    = job.get("deeper_analysis", "")
    rca_report         = job.get("rca_report", "")

    complaints_df = (
        df_result[df_result["Classification"].str.lower() == "complaint"]
        if "Classification" in df_result.columns else pd.DataFrame()
    )
    complaints    = len(complaints_df)
    critical_count = (
        int((df_result["Severity"] == "Critical").sum())
        if "Severity" in df_result.columns else 0
    )

    cat_counts: dict[str, int] = {}
    if not complaints_df.empty and "Taxonomy Category" in complaints_df.columns:
        cat_counts = complaints_df["Taxonomy Category"].value_counts().to_dict()
    total_complaints = sum(cat_counts.values()) or 1

    rca_sorted = sorted(
        rca_structured,
        key=lambda r: cat_counts.get(r.get("category", ""), 0),
        reverse=True,
    )

    color_map = {cat: PALETTE[i % len(PALETTE)] for i, cat in enumerate(cat_counts)}

    # Cap chart to top 9 categories; group the rest as "Others"
    _sorted_cats = sorted(cat_counts.items(), key=lambda x: -x[1])
    _TOP_N = len(PALETTE)
    if len(_sorted_cats) > _TOP_N:
        chart_cats = dict(_sorted_cats[:_TOP_N])
        chart_cats["Others"] = sum(v for _, v in _sorted_cats[_TOP_N:])
        color_map["Others"] = "#64748b"
    else:
        chart_cats = dict(_sorted_cats)

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs(["📊  Overview", "🔍  Category Analysis", "📋  Report Log"])

    # ══════════════════════════ TAB 1: OVERVIEW ══════════════════════════════
    with tab1:
        pct_complaints = round(complaints / total_rows * 100, 1) if total_rows else 0.0

        # KPI cards
        st.markdown(f"""
        <div class="rca-kpi-row">
          <div class="rca-kpi-card rca-kpi-blue">
            <div class="rca-kpi-label">Total Reports</div>
            <div class="rca-kpi-value">{total_rows}</div>
            <div class="rca-kpi-sub">All uploaded rows</div>
          </div>
          <div class="rca-kpi-card rca-kpi-red">
            <div class="rca-kpi-label">Critical Issues</div>
            <div class="rca-kpi-value">{critical_count}</div>
            <div class="rca-kpi-sub">High-distress reports flagged</div>
          </div>
          <div class="rca-kpi-card rca-kpi-amber">
            <div class="rca-kpi-label">Total Complaints</div>
            <div class="rca-kpi-value">{complaints}</div>
            <div class="rca-kpi-sub">{pct_complaints}% of all reports</div>
          </div>
          <div class="rca-kpi-card rca-kpi-green">
            <div class="rca-kpi-label">Categories Found</div>
            <div class="rca-kpi-value">{len(cat_counts)}</div>
            <div class="rca-kpi-sub">Distinct complaint categories</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Summary banner
        if collective_summary:
            st.markdown(f"""
            <div class="rca-summary-banner">
              <div class="rca-summary-icon">📋</div>
              <div>
                <div class="rca-summary-title">Overall Incident Summary</div>
                <p class="rca-summary-text">{collective_summary}</p>
              </div>
            </div>
            """, unsafe_allow_html=True)

        # Donut chart + category bars
        if cat_counts:
            chart_col, bars_col = st.columns([3, 2])

            labels = list(chart_cats.keys())
            values = list(chart_cats.values())
            total_for_donut = sum(values) or 1

            with chart_col:
                # CSS conic-gradient donut — no external library needed
                gradient_parts = []
                current = 0.0
                for label, count in zip(labels, values):
                    color = color_map.get(label, '#94a3b8')
                    end   = current + (count / total_for_donut * 360)
                    gradient_parts.append(f"{color} {current:.1f}deg {end:.1f}deg")
                    current = end
                gradient = ", ".join(gradient_parts)

                # Legend rows
                legend_items = "".join(
                    f'<div style="display:flex;align-items:center;gap:6px;font-size:12px;color:#cbd5e1">'
                    f'<div style="width:10px;height:10px;border-radius:50%;background:{color_map.get(l,"#94a3b8")};flex-shrink:0"></div>'
                    f'<span>{l}</span></div>'
                    for l in labels
                )

                st.markdown(f"""
                <div style="display:flex;align-items:center;gap:28px;padding:12px 0">
                  <div style="width:180px;height:180px;border-radius:50%;
                       background:conic-gradient({gradient});position:relative;flex-shrink:0">
                    <div style="position:absolute;top:50%;left:50%;
                         transform:translate(-50%,-50%);width:66%;height:66%;
                         background:#0e1117;border-radius:50%;display:flex;
                         align-items:center;justify-content:center;flex-direction:column">
                      <span style="font-size:22px;font-weight:700;color:#fafafa;line-height:1">{total_for_donut}</span>
                      <span style="font-size:11px;color:#8b9197;margin-top:2px">reports</span>
                    </div>
                  </div>
                  <div style="display:flex;flex-direction:column;gap:6px">{legend_items}</div>
                </div>
                """, unsafe_allow_html=True)

            with bars_col:
                max_count = max(values) if values else 1
                bars_html = ""
                for label, count in zip(labels, values):
                    color   = color_map.get(label, '#94a3b8')
                    pct_bar = round(count / max_count * 100)
                    short   = label if len(label) <= 26 else label[:24] + "…"
                    bars_html += f"""
                    <div class="rca-cat-row">
                      <div class="rca-cat-dot" style="background:{color}"></div>
                      <div class="rca-cat-name">{short}</div>
                      <div class="rca-cat-bar-wrap">
                        <div class="rca-cat-bar" style="width:{pct_bar}%;background:{color}"></div>
                      </div>
                      <div class="rca-cat-count">{count}</div>
                    </div>"""
                st.markdown(f'<div class="rca-cat-bars">{bars_html}</div>', unsafe_allow_html=True)

        # Severity breakdown table
        if cat_counts:
            st.divider()
            st.markdown('<p class="rca-section-title">Severity Breakdown by Category</p>', unsafe_allow_html=True)

            has_sev = (
                not complaints_df.empty
                and "Taxonomy Category" in complaints_df.columns
                and "Severity" in complaints_df.columns
            )
            sev_rows = []
            for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
                if has_sev:
                    cat_df   = complaints_df[complaints_df["Taxonomy Category"] == cat]
                    crit     = int((cat_df["Severity"] == "Critical").sum())
                    non_crit = count - crit
                else:
                    crit, non_crit = 0, count
                sev_rows.append({
                    "Category":    cat,
                    "Total":       count,
                    "Critical":    crit,
                    "Non-Critical": non_crit,
                    "Severity":    "🔴 Critical" if crit > 0 else "🟢 Non-Critical",
                })

            sev_df  = pd.DataFrame(sev_rows)
            max_tot = int(sev_df["Total"].max()) if not sev_df.empty else 1
            st.dataframe(
                sev_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Category":    st.column_config.TextColumn("Category",    width="large"),
                    "Total":       st.column_config.ProgressColumn(
                                       "Total", min_value=0, max_value=max_tot, format="%d", width="medium"
                                   ),
                    "Critical":    st.column_config.NumberColumn("🔴 Critical",    width="small"),
                    "Non-Critical": st.column_config.NumberColumn("🟢 Non-Critical", width="small"),
                    "Severity":    st.column_config.TextColumn("Top Severity", width="medium"),
                },
            )

    # ═══════════════════════ TAB 2: CATEGORY ANALYSIS ════════════════════════
    with tab2:
        if deeper_analysis:
            st.markdown(
                f'<div style="background:#1e2a3a;border:1px solid #2d4a6b;border-left:3px solid #2563eb;'
                f'border-radius:6px;padding:9px 13px;margin-bottom:10px;font-size:12.5px;color:#93c5fd;line-height:1.5">'
                f'<span style="font-size:9.5px;font-weight:700;letter-spacing:.9px;text-transform:uppercase;'
                f'color:#60a5fa;display:block;margin-bottom:2px">Cross-Cutting Theme</span>{deeper_analysis}</div>',
                unsafe_allow_html=True,
            )

        if not rca_sorted:
            st.info("No category analysis data available.")
        else:
            has_sev_col = (
                not complaints_df.empty
                and "Taxonomy Category" in complaints_df.columns
                and "Severity" in complaints_df.columns
            )
            has_sub_col = (
                not complaints_df.empty
                and "Taxonomy Category" in complaints_df.columns
                and "Taxonomy Subcategory" in complaints_df.columns
            )

            LBL     = "font-size:10px;font-weight:700;letter-spacing:.9px;text-transform:uppercase;color:#64748b;margin-bottom:3px"
            RC_BOX  = "background:#12141a;border-radius:0 4px 4px 0;padding:7px 10px;line-height:1.5;font-size:13px;color:#e2e8f0"
            ACT_BOX = "background:linear-gradient(135deg,#0c1f14 0%,#091510 100%);border:1px solid #1c3d28;border-radius:5px;padding:7px 10px"

            for i in range(0, len(rca_sorted), 2):
                pair = rca_sorted[i:i + 2]
                cols = st.columns(2, gap="small")

                for col, entry in zip(cols, pair):
                    category = entry.get("category", "Unknown")
                    count    = cat_counts.get(category, 0)
                    color    = color_map.get(category, "#94a3b8")

                    has_critical = False
                    if has_sev_col:
                        cat_df       = complaints_df[complaints_df["Taxonomy Category"] == category]
                        has_critical = bool((cat_df["Severity"] == "Critical").any())

                    root_cause        = entry.get("root_cause", "—")
                    deeper_root_cause = entry.get("deeper_root_cause", "")
                    issue_breakdown   = entry.get("issue_breakdown", [])
                    next_actions      = entry.get("next_actions", [])

                    sub_counts: dict[str, int] = {}
                    if has_sub_col:
                        sub_counts = (
                            complaints_df[complaints_df["Taxonomy Category"] == category]
                            ["Taxonomy Subcategory"]
                            .value_counts()
                            .head(4)
                            .to_dict()
                        )

                    sev_color = "#f87171" if has_critical else "#4ade80"
                    sev_text  = "Critical" if has_critical else "Non-Critical"

                    # ── subcategory pills (in header, under category name) ─────
                    pills_html = "".join(
                        f'<span class="sub-pill" style="'
                        f'color:{color};'
                        f'background:{_rgba(color, 0.10)};'
                        f'border-color:{_rgba(color, 0.30)};'
                        f'--pill-glow:{_rgba(color, 0.45)}">'
                        f'{s}</span>'
                        for s, _ in sub_counts.items() if s
                    )

                    # ── deeper root cause ─────────────────────────────────────
                    deeper_html = ""
                    if deeper_root_cause:
                        deeper_html = (
                            f'<div style="margin-bottom:6px">'
                            f'<div style="{LBL}">Deeper Root Cause</div>'
                            f'<div style="{RC_BOX};border-left:2px solid #8b5cf6">{deeper_root_cause}</div>'
                            f'</div>'
                        )

                    # ── issue breakdown (full width, bullet = card color) ──────
                    issue_items = "".join(
                        f'<div style="font-size:12.5px;color:#cbd5e1;padding:1px 0;line-height:1.4">'
                        f'<span style="color:{color};margin-right:5px;font-size:15px;line-height:1;vertical-align:middle">·</span>'
                        f'{iss}</div>'
                        for iss in issue_breakdown[:4]
                    ) or f'<div style="font-size:12.5px;color:#475569">—</div>'

                    # ── recommended actions ───────────────────────────────────
                    act_items = "".join(
                        f'<div style="font-size:12.5px;color:#7dd3a8;padding:1px 0;line-height:1.4">'
                        f'<span style="color:#4a9e72;margin-right:4px;font-weight:600">{idx}.</span>{a}</div>'
                        for idx, a in enumerate(next_actions[:4], 1)
                    ) or f'<div style="font-size:12.5px;color:#475569">—</div>'

                    card = f"""<div style="background:#1a1d27;border-top:1px solid #252836;border-right:1px solid #252836;border-bottom:1px solid #252836;border-left:3px solid {color};border-radius:8px;padding:11px 13px">

  <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:2px">
    <div style="font-size:15px;font-weight:700;color:#f1f5f9;line-height:1.25;flex:1;padding-right:12px">{category}</div>
    <div style="text-align:right;flex-shrink:0;line-height:1.3">
      <span style="font-size:19px;font-weight:700;color:#f1f5f9">{count}</span>
      <span style="font-size:11px;color:#64748b"> reports</span><br>
      <span style="font-size:11px;font-weight:600;color:{sev_color}">&#9679; {sev_text}</span>
    </div>
  </div>

  <div style="margin-bottom:5px">{pills_html if pills_html else ""}</div>

  <div style="height:1px;background:#252836;margin-bottom:6px"></div>

  <div style="margin-bottom:6px">
    <div style="{LBL}">Root Cause</div>
    <div style="{RC_BOX};border-left:2px solid {color}">{root_cause}</div>
  </div>

  {deeper_html}<div style="margin-bottom:6px">
    <div style="{LBL}">Issue Breakdown</div>
    {issue_items}
  </div>

  <div>
    <div style="{LBL}">Recommended Actions</div>
    <div style="{ACT_BOX}">{act_items}</div>
  </div>

</div>"""

                    with col:
                        st.markdown(card, unsafe_allow_html=True)

                st.markdown('<div style="margin-bottom:6px"></div>', unsafe_allow_html=True)

            if rca_report:
                st.divider()
                st.download_button(
                    "⬇ Download Full RCA Report (.md)",
                    rca_report.encode("utf-8"),
                    "rca_report.md",
                    "text/markdown",
                    use_container_width=True,
                )

    # ═════════════════════════ TAB 3: REPORT LOG ═════════════════════════════
    with tab3:
        # Column selection for the log view
        log_cols = [c for c in [
            "Translation (EN)", "Original Text", "Language",
            "Taxonomy Category", "Taxonomy Subcategory", "Severity", "Classification", "Confidence",
        ] if c in df_result.columns]
        df_log = df_result[log_cols].copy() if log_cols else df_result.copy()

        # Filter controls
        f1, f2, f3, f4 = st.columns([3, 1, 1, 1])

        search_term = f1.text_input(
            "search", label_visibility="collapsed",
            placeholder="🔍  Search translated reports…",
        )

        cat_opts = ["All Categories"]
        if "Taxonomy Category" in df_result.columns:
            cat_opts += sorted(df_result["Taxonomy Category"].dropna().unique().tolist())
        cat_filter = f2.selectbox("Category", cat_opts, label_visibility="collapsed")

        sub_opts = ["All Subcategories"]
        if "Taxonomy Subcategory" in df_result.columns:
            sub_pool = (
                df_result[df_result["Taxonomy Category"] == cat_filter]
                if cat_filter != "All Categories" else df_result
            )
            sub_opts += sorted(sub_pool["Taxonomy Subcategory"].dropna().unique().tolist())
        sub_filter = f3.selectbox("Subcategory", sub_opts, label_visibility="collapsed")

        sev_filter = f4.selectbox(
            "Severity", ["All Severities", "Critical", "Non-Critical"],
            label_visibility="collapsed",
        )

        # Apply filters
        df_filtered = df_log.copy()
        if search_term and "Translation (EN)" in df_filtered.columns:
            df_filtered = df_filtered[
                df_filtered["Translation (EN)"].str.contains(search_term, case=False, na=False)
            ]
        if cat_filter != "All Categories" and "Taxonomy Category" in df_filtered.columns:
            df_filtered = df_filtered[df_filtered["Taxonomy Category"] == cat_filter]
        if sub_filter != "All Subcategories" and "Taxonomy Subcategory" in df_filtered.columns:
            df_filtered = df_filtered[df_filtered["Taxonomy Subcategory"] == sub_filter]
        if sev_filter != "All Severities" and "Severity" in df_filtered.columns:
            df_filtered = df_filtered[df_filtered["Severity"] == sev_filter]

        st.caption(f"Showing **{len(df_filtered)}** of **{len(df_result)}** reports")
        st.dataframe(df_filtered, use_container_width=True, height=500, hide_index=False)

        st.divider()
        dl1, dl2 = st.columns(2)
        csv_bytes = df_result.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        dl1.download_button(
            "⬇ Download CSV", csv_bytes, "rca_results.csv", "text/csv",
            use_container_width=True,
        )
        excel_buf = io.BytesIO()
        with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
            df_result.to_excel(writer, index=False, sheet_name="RCA Results")
        excel_buf.seek(0)
        dl2.download_button(
            "⬇ Download Excel", excel_buf.getvalue(), "rca_results.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    st.divider()
    if st.button("🔄  Start New Analysis", use_container_width=True):
        del st.session_state["job_result"]
        st.rerun()


# ── If completed result already in session — show it ─────────────────────────
if "job_result" in st.session_state:
    render_results(st.session_state["job_result"])
    st.stop()


# ── Poll in-progress job ──────────────────────────────────────────────────────
if "job_id" in st.session_state:
    job_id = st.session_state.job_id
    total  = st.session_state.get("total_batches", 1)

    try:
        r   = requests.get(f"{BACKEND_URL}/status/{job_id}", timeout=10)
        job = r.json()
    except Exception as exc:
        st.error(f"Lost contact with backend: {exc}")
        del st.session_state["job_id"]
        st.stop()

    if job["status"] == "running":
        phase = job.get("phase", 1)
        done  = job.get("progress", 0)
        if phase == 1:
            pct  = done / total if total else 0
            text = f"Phase 1 — Translating & classifying: batch {done} / {total}"
        elif str(phase) == "1.5":
            pct  = 1.0
            text = "Phase 1.5 — Mapping complaints to taxonomy categories…"
        else:
            pct  = 1.0
            text = "Phase 2 — Running root cause analysis on full dataset…"
        st.progress(pct, text=text)
        time.sleep(POLL_INTERVAL)
        st.rerun()

    elif job["status"] == "error":
        del st.session_state["job_id"]
        st.error(f"Processing failed: {job.get('detail', 'Unknown error')}")

    elif job["status"] == "done":
        del st.session_state["job_id"]
        st.session_state["job_result"] = job
        st.rerun()

    st.stop()


# ── Upload form ───────────────────────────────────────────────────────────────
st.title("RCA Intelligence")
st.markdown("Upload a **CSV or Excel** file. The system translates, classifies, and performs root cause analysis.")
st.divider()

uploaded_file = st.file_uploader("Upload file (CSV / Excel)", type=["csv", "xlsx", "xls"])

if uploaded_file is not None:
    try:
        df_preview = (
            pd.read_csv(uploaded_file)
            if uploaded_file.name.endswith(".csv")
            else pd.read_excel(uploaded_file)
        )
        uploaded_file.seek(0)
    except Exception as exc:
        st.error(f"Could not read file: {exc}")
        st.stop()

    col_l, col_r = st.columns([3, 1])
    with col_l:
        st.subheader("Preview")
        st.dataframe(df_preview.head(10), use_container_width=True, height=260)
    with col_r:
        st.metric("Rows",    len(df_preview))
        st.metric("Columns", len(df_preview.columns))
        selected_col = st.selectbox("Text column", df_preview.columns.tolist())
        non_empty = df_preview[selected_col].dropna()
        non_empty = non_empty[non_empty.astype(str).str.strip() != ""]
        st.caption(f"Non-empty rows: **{len(non_empty)}**")

    st.divider()
    if st.button("Run AI Analysis", type="primary", use_container_width=True):
        try:
            uploaded_file.seek(0)
            r = requests.post(
                f"{BACKEND_URL}/process",
                files={"file": (uploaded_file.name, uploaded_file, "application/octet-stream")},
                data={"column": selected_col},
                timeout=30,
            )
            if r.status_code == 200:
                resp = r.json()
                st.session_state["job_id"]        = resp["job_id"]
                st.session_state["total_batches"] = resp["total_batches"]
                st.rerun()
            else:
                st.error(f"Error {r.status_code}: {r.json().get('detail', r.text)}")
        except requests.exceptions.ConnectionError:
            st.error(f"Cannot connect to backend at `{BACKEND_URL}`.")
        except Exception as exc:
            st.error(f"Unexpected error: {exc}")
else:
    st.info("Upload a CSV or Excel file above to get started.")
