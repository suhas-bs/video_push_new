"""
app.py — Meta Video Push Tool (Streamlit)
------------------------------------------
Flow:
  1. Upload CSV  →  validate + preview
  2. Enter Meta access token
  3. Push Videos to Meta  →  live row-by-row status
  4. Download results CSV
"""

import io
import requests
import pandas as pd
import streamlit as st
from meta_api import process_row, get_ig_accounts

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Meta Video Push Tool", page_icon="📹", layout="wide")

# ── Constants ─────────────────────────────────────────────────────────────────
ID_COLS = ["campaign_id", "adset_id", "product_set_id"]   # keep as strings

REQUIRED_COLS = {
    "creator_name", "cta_type",
    "cta_app_install_link", "cta_app_landing_link",
    "campaign_id", "adset_id", "ad_name", "product_set_id",
}
# At least one of these must be present per row
MEDIA_COLS = {"ad_code", "instagram_media_id"}

STATUS_EMOJI = {
    "pending":    "⏳",
    "running":    "🔄",
    "success":    "✅",
    "failed":     "❌",
    "ineligible": "⚠️",
    "skipped":    "⏭️",
}

# ── Sidebar — Account IDs ─────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ Account Config")
    st.caption("Pre-filled with Minutes defaults. Edit as needed.")
    ad_account_id    = st.text_input("Ad Account ID",        value="1549883851784009", help="Without act_ prefix")
    facebook_page_id = st.text_input("Facebook Page ID",     value="336701269535125")
    ig_account_id    = st.text_input("Instagram Account ID", value="17841467737662719")
    st.divider()
    st.caption("**Required CSV columns**")
    st.code("creator_name\ncta_type\ncta_app_install_link\ncta_app_landing_link\ncampaign_id\nadset_id\nad_name\nproduct_set_id\nad_code", language=None)
    st.divider()
    st.caption("**🔑 Strongly recommended**")
    st.code("creator_ig_account_id", language=None)
    st.caption(
        "Each creator's **numeric** Instagram account ID (e.g. `17841467737662719`). "
        "Without this, the tool needs a special-permission token. "
        "Creators can find their ID in Instagram → Settings → Account → Instagram ID."
    )


# ── Main ──────────────────────────────────────────────────────────────────────
st.title("📹 Meta Video Push Tool")

# ────────────────────────────────────────────────────────────────────────────
# ACCOUNT VERIFIER  (run before uploading CSV to confirm IDs are correct)
# ────────────────────────────────────────────────────────────────────────────
with st.expander("🔍 Verify Account Config (run this first)", expanded=False):
    st.caption("Enter your token below and click Verify to confirm which Instagram accounts Meta can see.")
    verify_token = st.text_area("Token for verification", key="verify_token", height=80,
                                placeholder="Paste your Meta access token here…",
                                help="Use text area so you can confirm the full token pasted correctly.")
    verify_token = verify_token.strip()
    if st.button("Verify Accounts"):
        if not verify_token:
            st.warning("Enter a token first.")
        else:
            import urllib3; urllib3.disable_warnings()
            GRAPH = "https://graph.facebook.com"

            # Check token
            me = requests.get(f"{GRAPH}/v23.0/me",
                              params={"access_token": verify_token, "fields": "id,name"},
                              verify=False)
            if me.status_code != 200:
                st.error(f"Token invalid: {me.json()}")
            else:
                st.success(f"Token OK — logged in as: {me.json().get('name')} ({me.json().get('id')})")

                # IG accounts via page
                pg = requests.get(f"{GRAPH}/v23.0/{facebook_page_id}",
                                  params={"access_token": verify_token,
                                          "fields": "name,instagram_business_account"},
                                  verify=False).json()
                st.write("**Facebook Page:**", pg.get("name", "—"))
                ig_from_page = pg.get("instagram_business_account", {}).get("id")
                st.write("**IG account linked to page:**", ig_from_page or "❌ None found")

                # IG accounts via ad account
                aa = requests.get(f"{GRAPH}/v23.0/act_{ad_account_id}",
                                  params={"access_token": verify_token,
                                          "fields": "name,instagram_accounts{id,name,username}"},
                                  verify=False).json()
                st.write("**Ad Account:**", aa.get("name", "—"))
                ig_from_aa = aa.get("instagram_accounts", {}).get("data", [])
                if ig_from_aa:
                    st.write("**IG accounts linked to ad account:**")
                    for a in ig_from_aa:
                        st.code(f"ID: {a.get('id')}  |  @{a.get('username','?')}  |  {a.get('name','')}")
                else:
                    st.write("**IG accounts linked to ad account:** ❌ None found")

                if not ig_from_page and not ig_from_aa:
                    st.error("⛔ No Instagram accounts found. Go to Meta Business Manager → Business Settings → Instagram Accounts and connect your IG account to the page/ad account.")
                else:
                    st.info("Copy the correct IG account ID above and paste it into the 'Instagram Account ID' field in the sidebar.")

# ────────────────────────────────────────────────────────────────────────────
# STEP 1 — Upload CSV
# ────────────────────────────────────────────────────────────────────────────
st.subheader("Step 1 — Upload CSV")

uploaded = st.file_uploader("Upload your creator CSV", type=["csv"], label_visibility="collapsed")

if not uploaded:
    st.info("👆 Upload a CSV file to get started.")
    st.stop()

# Parse — force ID columns to string to prevent 18-digit float conversion
try:
    df = pd.read_csv(uploaded, dtype={c: str for c in ID_COLS})
except Exception as e:
    st.error(f"Could not read CSV: {e}")
    st.stop()

# Strip whitespace
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].str.strip()

# Validate columns
missing = REQUIRED_COLS - set(df.columns)
if missing:
    st.error(f"Missing columns: `{', '.join(sorted(missing))}`")
    st.stop()

if not (MEDIA_COLS & set(df.columns)):
    st.error("CSV must have at least one of: `instagram_media_id` or `ad_code`")
    st.stop()

# Drop rows with bad product_set_id
df = df[~df["product_set_id"].fillna("").str.lower().isin(["error", "eror", "", "nan", "none"])]

if df.empty:
    st.warning("No valid rows found after filtering. Check the CSV.")
    st.stop()

st.success(f"✅ **{len(df)} rows** loaded from `{uploaded.name}`")
with st.expander("Preview data", expanded=True):
    st.dataframe(df, use_container_width=True)


# ────────────────────────────────────────────────────────────────────────────
# STEP 2 — Enter access token
# ────────────────────────────────────────────────────────────────────────────
st.divider()
st.subheader("Step 2 — Enter Meta Access Token")

token = st.text_area(
    "Access Token",
    height=80,
    placeholder="Paste your Meta access token here…",
    label_visibility="collapsed",
)
token = token.strip()

if not token:
    st.info("🔑 Enter your Meta access token to continue.")
    st.stop()

st.success(f"✅ Token received ({len(token)} chars).")


# ────────────────────────────────────────────────────────────────────────────
# STEP 3 — Push
# ────────────────────────────────────────────────────────────────────────────
st.divider()
st.subheader("Step 3 — Push Videos to Meta")

col_btn, _ = st.columns([2, 5])
with col_btn:
    run = st.button("🚀 Push Videos to Meta", type="primary", use_container_width=True)

if not run:
    st.stop()

config = {
    "access_token":       token,
    "ad_account_id":      ad_account_id,
    "facebook_page_id":   facebook_page_id,
    "ig_account_id":      ig_account_id,
    "use_eligibility_api": False,   # set True only if your app has branded_content_advertisable_medias permission
}

rows  = df.to_dict("records")
total = len(rows)
results = []

st.divider()
st.subheader(f"Processing {total} rows…")

progress_bar = st.progress(0, text="Starting…")
tbl_placeholder = st.empty()

live = [
    {"ad_name": r.get("ad_name", ""), "status": "pending",
     "published_ad_id": None, "creative_id": None,
     "video_id": None, "error_message": ""}
    for r in rows
]

def render(placeholder, statuses):
    placeholder.dataframe(
        pd.DataFrame([{
            "Ad Name":      s["ad_name"],
            "Status":       f"{STATUS_EMOJI.get(s['status'], '')} {s['status'].capitalize()}",
            "Published Ad": s.get("published_ad_id") or "—",
            "Creative ID":  s.get("creative_id") or "—",
            "Video ID":     s.get("video_id") or "—",
            "Error":        s.get("error_message") or "",
        } for s in statuses]),
        use_container_width=True, hide_index=True
    )

render(tbl_placeholder, live)

for i, row in enumerate(rows):
    ad_name = row.get("ad_name", f"Row {i+1}")
    progress_bar.progress(i / total, text=f"Processing {ad_name} ({i+1}/{total})…")
    live[i]["status"] = "running"
    render(tbl_placeholder, live)

    result = process_row(row, config)
    results.append(result)

    live[i].update({
        "status":          result.get("status", "failed"),
        "published_ad_id": result.get("published_ad_id"),
        "creative_id":     result.get("creative_id"),
        "video_id":        result.get("video_id"),
        "error_message":   result.get("error_message", ""),
    })
    render(tbl_placeholder, live)

progress_bar.progress(1.0, text="Done!")


# ────────────────────────────────────────────────────────────────────────────
# STEP 4 — Summary + Download
# ────────────────────────────────────────────────────────────────────────────
st.divider()
result_df = pd.DataFrame(results)

c1, c2, c3 = st.columns(3)
c1.metric("✅ Success",     int((result_df["status"] == "success").sum()))
c2.metric("❌ Failed",      int((result_df["status"] == "failed").sum()))
c3.metric("⚠️ Ineligible",  int((result_df["status"] == "ineligible").sum()))

# Results CSV
success_cols = [
    "creator_name", "ad_name", "campaign_id", "adset_id",
    "published_ad_id", "creative_id", "video_id",
    "status", "error_message", "ad_code", "cta_type",
]
out_df    = result_df[[c for c in success_cols if c in result_df.columns]]
csv_bytes = out_df.to_csv(index=False).encode("utf-8")

st.download_button(
    label="⬇️ Download Results CSV",
    data=csv_bytes,
    file_name=f"meta_push_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv",
    type="primary",
)
