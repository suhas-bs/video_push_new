"""
meta_api.py — Meta Graph API helpers  v8
v8: Exact replication of working notebook flow:
    ad_code → eligibility API → media_id → upload video → create creative → create ad
    Creative uses branded_content + instagram_branded_content (not instagram_actor_id)
"""
import json, re, requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GRAPH   = "https://graph.facebook.com"
VERSION = "v8"


def _clean_id(val):
    s = str(val or "").strip()
    s = re.sub(r'\.0+$', '', s)
    return s or None


def fetch_branded_content_advertisable_medias(access_token, ig_account_id, ad_code):
    """
    Step 1: resolve ad_code → numeric Instagram media ID.
    Returns the first result dict, or {"error": "..."} on failure.
    """
    url = f"{GRAPH}/v22.0/{ig_account_id}/branded_content_advertisable_medias"
    params = {
        "access_token": access_token,
        "fields": "eligibility_errors,owner_id,permalink,id,has_permission_for_partnership_ad",
        "ad_code": ad_code,
    }
    r = requests.get(url, params=params, verify=False)
    if r.status_code == 200:
        data = r.json().get("data", [])
        if data:
            return data[0]
        return {"error": "No data returned from eligibility API"}
    return {"error": f"{r.status_code} — {r.text}"}


def upload_instagram_video(access_token, ad_account_id, source_instagram_media_id, ad_code=None):
    """
    Step 2: upload the creator's media as an ad video asset.
    """
    url = f"{GRAPH}/v22.0/act_{ad_account_id}/advideos"
    params = {
        "source_instagram_media_id": source_instagram_media_id,
        "access_token": access_token,
    }
    if ad_code:
        params["partnership_ad_ad_code"] = ad_code
        params["is_partnership_ad"] = True
    r = requests.post(url, params=params, verify=False)
    d = r.json()
    if r.status_code == 200 and "id" in d:
        return d["id"], None
    err = d.get("error", {})
    return None, f"advideos {r.status_code}: [{err.get('code','?')}] {err.get('message', r.text)}"


def create_ad_creative(access_token, ad_account_id, facebook_page_id, ig_account_id,
                       source_instagram_media_id, ad_code, cta_type,
                       cta_app_install_link, cta_app_landing_link,
                       creative_name="", product_set_id=None):
    """
    Step 3: create the ad creative.

    Structure that works (from confirmed working notebook):
      object_id                  = facebook_page_id
      facebook_branded_content   = {"sponsor_page_id": facebook_page_id}
      instagram_branded_content  = {"sponsor_id": ig_account_id}
      branded_content            = {"instagram_boost_post_access_token": ad_code}
      call_to_action             = {...}
      (optional) degrees_of_freedom_spec + creative_sourcing_spec for product catalogue
    """
    url  = f"{GRAPH}/v23.0/act_{ad_account_id}/adcreatives"
    name = creative_name or "partnership_ad_creative"

    params = {
        "access_token": access_token,
        "name": name,
        "object_id": facebook_page_id,
        "facebook_branded_content":  json.dumps({"sponsor_page_id": facebook_page_id}),
        "instagram_branded_content": json.dumps({"sponsor_id": ig_account_id}),
        "call_to_action": json.dumps({
            "type": cta_type,
            "value": {
                "link":     cta_app_install_link,
                "app_link": cta_app_landing_link,
            },
        }),
    }

    if ad_code:
        params["branded_content"] = json.dumps({"instagram_boost_post_access_token": ad_code})
    elif source_instagram_media_id:
        params["source_instagram_media_id"] = source_instagram_media_id
    else:
        return None, "ad_code or source_instagram_media_id required"

    if product_set_id:
        params["degrees_of_freedom_spec"] = json.dumps({
            "creative_features_spec": {
                "product_extensions": {"enroll_status": "OPT_IN"}
            }
        })
        params["creative_sourcing_spec"] = json.dumps({
            "associated_product_set_id": str(product_set_id)
        })

    r = requests.post(url, params=params, verify=False)
    d = r.json()
    if r.status_code == 200 and "id" in d:
        return d["id"], None
    err = d.get("error", {})
    return None, f"adcreatives {r.status_code}: [{err.get('code','?')}] {err.get('message', r.text)}"


def create_ad(access_token, ad_account_id, ad_name, adset_id, creative_id):
    """Step 4: create the paused ad."""
    url = f"{GRAPH}/v22.0/act_{ad_account_id}/ads"
    params = {
        "access_token": access_token,
        "status":    "PAUSED",
        "name":      ad_name,
        "adset_id":  adset_id,
        "creative":  json.dumps({"creative_id": creative_id}),
    }
    r = requests.post(url, params=params, verify=False)
    d = r.json()
    if r.status_code == 200 and "id" in d:
        return d["id"], None
    err = d.get("error", {})
    return None, f"ads {r.status_code}: [{err.get('code','?')}] {err.get('message', r.text)}"


# kept for compatibility — app.py calls this
def get_ig_accounts(access_token, facebook_page_id, ad_account_id):
    return []


def process_row(row, config):
    result = dict(row)
    result.update({
        "video_id": None, "creative_id": None,
        "published_ad_id": None, "status": "skipped", "error_message": "",
    })

    token   = config["access_token"]
    acct    = _clean_id(config["ad_account_id"])
    fb_page = config["facebook_page_id"]
    ig_acct = config["ig_account_id"]

    ad_code      = str(row.get("ad_code", "")).strip() or None
    cta_type     = str(row.get("cta_type", "SHOP_NOW")).strip()
    install_link = str(row.get("cta_app_install_link", "")).strip()
    landing_link = str(row.get("cta_app_landing_link", "")).strip()
    ad_name      = str(row.get("ad_name", "")).strip()
    adset_id     = _clean_id(row.get("adset_id", ""))

    # product_set_id — keep as string, skip if invalid
    ps_raw = str(row.get("product_set_id", "")).strip()
    product_set_id = ps_raw if ps_raw.lower() not in ("", "nan", "none", "error", "eror") else None

    diag = "[v8] "

    if not ad_code:
        result.update({"status": "skipped", "error_message": "No ad_code provided"})
        return result

    # ── Step 1: eligibility → media ID ────────────────────────────────────────
    elig = fetch_branded_content_advertisable_medias(token, ig_acct, ad_code)
    if "error" in elig:
        result.update({"status": "failed",
                       "error_message": diag + f"Eligibility failed: {elig['error']}"})
        return result

    elig_errors = elig.get("eligibility_errors", [])
    if elig_errors:
        result.update({"status": "ineligible",
                       "error_message": diag + f"Not eligible: {elig_errors}"})
        return result

    media_id = elig.get("id")
    if not media_id:
        result.update({"status": "failed",
                       "error_message": diag + "Eligibility API returned no media ID"})
        return result

    # ── Step 2: upload video ───────────────────────────────────────────────────
    video_id, err = upload_instagram_video(token, acct, media_id, ad_code)
    result["video_id"] = video_id
    if not video_id:
        result.update({"status": "failed",
                       "error_message": diag + (err or "Video upload returned no ID")})
        return result

    # ── Step 3: create creative ────────────────────────────────────────────────
    creative_id, err = create_ad_creative(
        token, acct, fb_page, ig_acct,
        media_id, ad_code, cta_type, install_link, landing_link,
        ad_name, product_set_id,
    )
    result["creative_id"] = creative_id
    if not creative_id:
        result.update({"status": "failed",
                       "error_message": diag + (err or "Creative creation returned no ID")})
        return result

    # ── Step 4: create ad ──────────────────────────────────────────────────────
    published_ad_id, err = create_ad(token, acct, ad_name, adset_id, creative_id)
    result["published_ad_id"] = published_ad_id
    if not published_ad_id:
        result.update({"status": "failed",
                       "error_message": diag + (err or "Ad creation returned no ID")})
        return result

    result["status"] = "success"
    return result
