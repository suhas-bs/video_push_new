# Meta Video Push Tool

A Streamlit app that reads a creator CSV and pushes Instagram videos to Meta ad sets via the Graph API.

---

## Setup (one-time)

### 1. Clone the repo
```bash
git clone <your-repo-url>
cd <repo-folder>
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Add your Meta access token
```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
```
Open `.streamlit/secrets.toml` and paste your token:
```toml
META_ACCESS_TOKEN = "your_actual_token_here"
```
> **Never commit `secrets.toml` to git.** It's already in `.gitignore`.

---

## Running the app
```bash
streamlit run app.py
```
The app opens at `http://localhost:8501`.

---

## CSV format

Your input file must have these columns (see `sample_input.csv` for reference):

| Column | Description |
|---|---|
| `creator_name` | Creator's display name |
| `ad_code` | Instagram partnership ad code |
| `cta_type` | e.g. `SHOP_NOW` |
| `cta_app_install_link` | Play Store / App Store link |
| `cta_app_landing_link` | Deep link landing URL |
| `campaign_id` | Meta Campaign ID |
| `adset_id` | Meta Ad Set ID |
| `ad_name` | Name to give the new ad |
| `product_set_id` | Catalogue product set ID |

---

## What the app does (per row)

1. **Eligibility check** — verifies the IG media is eligible for a partnership ad  
2. **Upload video** — pushes the IG video into the ad account's video library  
3. **Create ad creative** — builds a branded content creative with your CTA  
4. **Create ad** — creates the ad in `PAUSED` status  

Results (published ad IDs, creative IDs, error messages) are shown live and can be downloaded as CSV.

---

## Account IDs

The sidebar is pre-filled with **Minutes** defaults. For a different account, update the fields in the sidebar — no code change needed.

| Field | Default |
|---|---|
| Ad Account ID | `1549883851784009` |
| Facebook Page ID | `336701269535125` |
| Instagram Account ID | `17841467737662719` |

---

## Deploying to Streamlit Cloud

1. Push this repo to GitHub (make sure `secrets.toml` is **not** committed).
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app** → select your repo / branch / `app.py`.
3. Under **Advanced settings → Secrets**, paste:
   ```toml
   META_ACCESS_TOKEN = "your_token"
   ```
4. Deploy. Share the URL with the team.
