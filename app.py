"""
app.py — Meta Video Push Tool (Streamlit)
------------------------------------------
Paste a Google Sheets URL → previews data → push to Meta.
Access token is loaded from .streamlit/secrets.toml (key: META_ACCESS_TOKEN).

Why Google Sheets instead of CSV?
  Large IDs (campaign_id, adset_id, product_set_id) are 18-digit integers.
  Excel/CSV download converts them to scientific notation (1.2E+17), breaking
  the API. Reading directly from Sheets preserves them as plain text.
"""

import re
import io

import pandas as pd
import requests
import streamlit as st

from meta_api import process_row

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Meta Video Push Tool",
    page_icon="📹",
    layout="wide",
)

# ── Constants ─────────────────────────────────────────────────────────────────
# Columns that must stay as strings — never let pandas cast them to float/int
ID_COLS = ["campaign_id", "adset_id", "product_set_id"]

REQUIRED_COLS = {
    "creator_name", "ad_code", "cta_type", "cta_app_install_link",
    "cta_app_landing_link", "campaign_id", "adset_id", "ad_name", "product_set_id",
}

STATUS_EMOJI = {
    "pending":    "⏳",
    "running":    "🔄",
    "success":    "✅",
    "failed":     "❌",
    "ineligible": "⚠️",
    "skipped":    "⏭️",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_token() -> str:
    try:
        return st.secrets["META_ACCESS_TOKEN"]
    except (KeyError, FileNotFoundError):
        return ""


def extract_sheet_id(url: str) -> tuple[str | None, str | None]:
    """
    Returns (sheet_id, gid) from any Google Sheets URL variant.
    gid may be None (defaults to first sheet).
    """
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url)
    if not match:
        return None, None
    sheet_id = match.group(1)
    gid_match = re.search(r"[#&?]gid=(\d+)", url)
    gid = gid_match.group(1) if gid_match else None
    return sheet_id, gid


def load_sheet(sheet_id: str, gid: str | None) -> pd.DataFrame:
    """
    Downloads the sheet as CSV (public link required) and returns a DataFrame
    with all ID columns forced to string to prevent precision loss.
    """
    export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    if gid:
        export_url += f"&gid={gid}"

    resp = requests.get(export_url, timeout=15)
    resp.raise_for_status()

    dtype_map = {col: str for col in ID_COLS}
    df = pd.read_csv(io.StringIO(resp.text), dtype=dtype_map)

    # Strip whitespace from string columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    return df


def render_status_table(placeholder, statuses: list[dict]):
    rows_display = []
    for s in statuses:
        emoji = STATUS_EMOJI.get(s["status"], "")
        rows_display.append({
            "Ad Name":      s["ad_name"],
            "Status":       f"{emoji} {s['status'].capitalize()}",
            "Published Ad": s.get("published_ad_id") or "—",
            "Creative ID":  s.get("creative_id") or "—",
            "Video ID":     s.get("video_id") or "—",
            "Error":        s.get("error_message") or "",
        })
    placeholder.dataframe(pd.DataFrame(rows_display), use_container_width=True, hide_index=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ Configuration")
    st.caption("Pre-filled with Minutes defaults. Edit as needed.")

    ad_account_id = st.text_input(
        "Ad Account ID",
        value="1549883851784009",
        help="Without the 'act_' prefix",
    )
    facebook_page_id = st.text_input("Facebook Page ID", value="336701269535125")
    ig_account_id    = st.text_input("Instagram Account ID", value="17841467737662719")

    st.divider()

    token = get_token()
    if token:
        st.success("✅ Access token loaded from secrets", icon="🔑")
    else:
        st.warning(
            "⚠️ No token in `.streamlit/secrets.toml`.\n\n"
            "Add `META_ACCESS_TOKEN = \"your_token\"` and restart.",
            icon="🔑",
        )

    st.divider()
    st.caption("**Required sheet columns:**")
    st.code(
        "creator_name · ad_code · cta_type\n"
        "cta_app_install_link · cta_app_landing_link\n"
        "campaign_id · adset_id · ad_name · product_set_id",
        language=None,
    )


# ── Main ──────────────────────────────────────────────────────────────────────
st.title("📹 Meta Video Push Tool")
st.caption("Paste your Google Sheet URL, preview the data, then push to Meta.")

# ── Sheet URL input ───────────────────────────────────────────────────────────
sheet_url = st.text_input(
    "Google Sheet URL",
    placeholder="https://docs.google.com/spreadsheets/d/…",
    help="Sheet must be shared with 'Anyone with the link can view'",
)

col_load, col_hint = st.columns([2, 5])
with col_load:
    load_btn = st.button("📥 Load Sheet", use_container_width=True, disabled=not sheet_url)
with col_hint:
    st.caption("💡 Make sure the sheet is shared as **Anyone with the link → Viewer**")

if not sheet_url:
    st.info("👆 Paste your Google Sheet URL above to get started.")
    st.stop()

if not load_btn and "sheet_df" not in st.session_state:
    st.info("👆 Click **Load Sheet** to fetch the data.")
    st.stop()

# ── Fetch sheet ───────────────────────────────────────────────────────────────
if load_btn:
    sheet_id, gid = extract_sheet_id(sheet_url)
    if not sheet_id:
        st.error("Couldn't extract a Sheet ID from that URL. Please check and try again.")
        st.stop()

    with st.spinner("Fetching sheet…"):
        try:
            df = load_sheet(sheet_id, gid)
            st.session_state["sheet_df"] = df
            st.session_state["sheet_url"] = sheet_url
        except requests.HTTPError as e:
            st.error(
                f"Could not fetch the sheet (HTTP {e.response.status_code}). "
                "Make sure the sheet is shared as **Anyone with the link → Viewer**."
            )
            st.stop()
        except Exception as e:
            st.error(f"Error loading sheet: {e}")
            st.stop()

df = st.session_state["sheet_df"]

# ── Validate columns ──────────────────────────────────────────────────────────
missing = REQUIRED_COLS - set(df.columns)
if missing:
    st.error(f"Missing columns: `{', '.join(sorted(missing))}`")
    st.stop()

# Drop invalid product_set_id rows
df = df[~df["product_set_id"].astype(str).str.strip().isin(["error", "eror", "", "nan"])]

st.success(f"✅ Loaded **{len(df)} rows** from Google Sheets")

with st.expander("Preview input data", expanded=True):
    st.dataframe(df, use_container_width=True)

# ── Push button ───────────────────────────────────────────────────────────────
st.divider()
col_btn, _ = st.columns([2, 5])
with col_btn:
    run = st.button(
        "🚀 Push Videos to Meta",
        type="primary",
        disabled=not token,
        use_container_width=True,
    )

if not run:
    st.stop()

if not token:
    st.error("Access token is missing. See sidebar instructions.")
    st.stop()

config = {
    "access_token":    token,
    "ad_account_id":   ad_account_id,
    "facebook_page_id": facebook_page_id,
    "ig_account_id":   ig_account_id,
}

rows  = df.to_dict("records")
total = len(rows)
results = []

st.divider()
st.subheader(f"Processing {total} rows…")

progress_bar           = st.progress(0, text="Starting…")
status_table_placeholder = st.empty()

live_statuses = [
    {"ad_name": r.get("ad_name", ""), "status": "pending",
     "published_ad_id": None, "creative_id": None,
     "video_id": None, "error_message": ""}
    for r in rows
]
render_status_table(status_table_placeholder, live_statuses)

for i, row in enumerate(rows):
    ad_name = row.get("ad_name", f"Row {i+1}")
    progress_bar.progress(i / total, text=f"Processing {ad_name} ({i+1}/{total})…")

    live_statuses[i]["status"] = "running"
    render_status_table(status_table_placeholder, live_statuses)

    result = process_row(row, config)
    results.append(result)

    live_statuses[i].update({
        "status":          result.get("status", "failed"),
        "published_ad_id": result.get("published_ad_id"),
        "creative_id":     result.get("creative_id"),
        "video_id":        result.get("video_id"),
        "error_message":   result.get("error_message", ""),
    })
    render_status_table(status_table_placeholder, live_statuses)

progress_bar.progress(1.0, text="Done!")

# ── Summary ───────────────────────────────────────────────────────────────────
st.divider()
result_df = pd.DataFrame(results)

success_count    = (result_df["status"] == "success").sum()
failed_count     = (result_df["status"] == "failed").sum()
ineligible_count = (result_df["status"] == "ineligible").sum()

col_a, col_b, col_c = st.columns(3)
col_a.metric("✅ Success",     success_count)
col_b.metric("❌ Failed",      failed_count)
col_c.metric("⚠️ Ineligible",  ineligible_count)

# ── Download results ──────────────────────────────────────────────────────────
output_cols = [
    "creator_name", "ad_name", "adset_id", "campaign_id",
    "status", "error_message",
    "published_ad_id", "creative_id", "video_id",
    "ad_code", "cta_type",
]
download_df = result_df[[c for c in output_cols if c in result_df.columns]]
csv_bytes   = download_df.to_csv(index=False).encode("utf-8")

st.download_button(
    label="⬇️ Download Results CSV",
    data=csv_bytes,
    file_name=f"meta_push_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv",
    type="primary",
)
