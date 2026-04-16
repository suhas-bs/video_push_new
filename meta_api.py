"""
meta_api.py
-----------
Low-level helpers that talk to the Meta Graph API.
All functions are stateless — caller passes account IDs and access token.
"""

import json
import requests

# Suppress SSL warnings (same behaviour as the original notebook)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GRAPH_BASE = "https://graph.facebook.com"
AD_VIDEO_API_VERSION = "v22.0"
AD_CREATIVE_API_VERSION = "v23.0"
AD_API_VERSION = "v22.0"
ELIGIBILITY_API_VERSION = "v22.0"


# ---------------------------------------------------------------------------
# 1. Check eligibility
# ---------------------------------------------------------------------------

def fetch_eligibility(
    access_token: str,
    ig_account_id: str | int,
    ad_code: str | None = None,
    permalinks: list | None = None,
) -> dict:
    """
    Returns the first media object from branded_content_advertisable_medias,
    or a dict with an 'error' key on failure.
    """
    url = f"{GRAPH_BASE}/{ELIGIBILITY_API_VERSION}/{ig_account_id}/branded_content_advertisable_medias"
    params = {
        "access_token": access_token,
        "fields": "eligibility_errors,owner_id,permalink,id,has_permission_for_partnership_ad",
    }
    if ad_code:
        params["ad_code"] = ad_code
    elif permalinks:
        params["permalinks"] = json.dumps(permalinks)
    else:
        raise ValueError("Either ad_code or permalinks must be provided.")

    resp = requests.get(url, params=params, verify=False)
    if resp.status_code == 200:
        data = resp.json().get("data", [])
        if data:
            return data[0]
        return {"error": "No data returned from eligibility check."}
    return {"error": f"{resp.status_code} — {resp.text}"}


# ---------------------------------------------------------------------------
# 2. Upload Instagram video to Ad Account
# ---------------------------------------------------------------------------

def upload_instagram_video(
    access_token: str,
    ad_account_id: str | int,
    source_instagram_media_id: str,
    ad_code: str | None = None,
) -> str | None:
    """
    Uploads an IG media to the ad account's video library.
    Returns video_id on success, None on failure.
    """
    url = f"{GRAPH_BASE}/{AD_VIDEO_API_VERSION}/act_{ad_account_id}/advideos"
    params = {
        "source_instagram_media_id": source_instagram_media_id,
        "access_token": access_token,
    }
    if ad_code:
        params["partnership_ad_ad_code"] = ad_code
        params["is_partnership_ad"] = True

    resp = requests.post(url, params=params, verify=False)
    if resp.status_code == 200:
        return resp.json().get("id")
    return None


# ---------------------------------------------------------------------------
# 3. Create Ad Creative
# ---------------------------------------------------------------------------

def create_ad_creative(
    access_token: str,
    ad_account_id: str | int,
    facebook_page_id: str | int,
    ig_account_id: str | int,
    source_instagram_media_id: str | None,
    ad_code: str | None,
    cta_type: str,
    cta_app_install_link: str,
    cta_app_landing_link: str,
    product_set_id: str | int | None = None,
) -> str | None:
    """
    Creates an ad creative and returns its ID, or None on failure.
    Retries once on a 200-but-no-id response (mirrors original notebook logic).
    """
    url = f"{GRAPH_BASE}/{AD_CREATIVE_API_VERSION}/act_{ad_account_id}/adcreatives"
    params = {
        "access_token": access_token,
        "object_id": facebook_page_id,
        "facebook_branded_content": json.dumps({"sponsor_page_id": facebook_page_id}),
        "instagram_branded_content": json.dumps({"sponsor_id": ig_account_id}),
        "call_to_action": json.dumps({
            "type": cta_type,
            "value": {
                "link": cta_app_install_link,
                "app_link": cta_app_landing_link,
            },
        }),
    }

    if ad_code:
        params["branded_content"] = json.dumps({"instagram_boost_post_access_token": ad_code})
    elif source_instagram_media_id:
        params["source_instagram_media_id"] = source_instagram_media_id
    else:
        raise ValueError("Either ad_code or source_instagram_media_id must be provided.")

    if product_set_id:
        params["degrees_of_freedom_spec"] = json.dumps({
            "creative_features_spec": {
                "product_extensions": {"enroll_status": "OPT_IN"}
            }
        })
        params["creative_sourcing_spec"] = json.dumps({
            "associated_product_set_id": str(product_set_id)
        })

    for _attempt in range(2):          # retry once, same as original notebook
        resp = requests.post(url, params=params, verify=False)
        data = resp.json()
        if resp.status_code == 200 and "id" in data:
            return data["id"]

    return None


# ---------------------------------------------------------------------------
# 4. Create Ad
# ---------------------------------------------------------------------------

def create_ad(
    access_token: str,
    ad_account_id: str | int,
    ad_name: str,
    adset_id: str | int,
    creative_id: str,
) -> str | None:
    """
    Creates an ad in PAUSED status. Returns published_ad_id or None.
    """
    url = f"{GRAPH_BASE}/{AD_API_VERSION}/act_{ad_account_id}/ads"
    params = {
        "access_token": access_token,
        "status": "PAUSED",
        "name": ad_name,
        "adset_id": adset_id,
        "creative": json.dumps({"creative_id": creative_id}),
    }
    resp = requests.post(url, params=params, verify=False)
    if resp.status_code == 200:
        return resp.json().get("id")
    return None


# ---------------------------------------------------------------------------
# 5. End-to-end pipeline for a single row
# ---------------------------------------------------------------------------

def process_row(row: dict, config: dict) -> dict:
    """
    Runs the full pipeline for one CSV row.
    config keys: access_token, ad_account_id, facebook_page_id, ig_account_id

    Returns a copy of the row enriched with:
      eligibility, video_id, creative_id, published_ad_id, status, error_message
    """
    result = dict(row)
    result.update({"eligibility": None, "video_id": None,
                   "creative_id": None, "published_ad_id": None,
                   "status": "skipped", "error_message": ""})

    token       = config["access_token"]
    ad_acct     = config["ad_account_id"]
    fb_page     = config["facebook_page_id"]
    ig_acct     = config["ig_account_id"]

    ad_code             = str(row.get("ad_code", "")).strip() or None
    cta_type            = row.get("cta_type", "SHOP_NOW")
    cta_install_link    = row.get("cta_app_install_link", "")
    cta_landing_link    = row.get("cta_app_landing_link", "")
    ad_name             = row.get("ad_name", "")
    adset_id            = row.get("adset_id", "")
    product_set_id      = row.get("product_set_id") or None

    # Step 1 — eligibility
    eligibility = fetch_eligibility(token, ig_acct, ad_code=ad_code)
    result["eligibility"] = eligibility

    if "error" in eligibility:
        result["status"] = "failed"
        result["error_message"] = f"Eligibility check failed: {eligibility['error']}"
        return result

    if eligibility.get("eligibility_errors"):
        result["status"] = "ineligible"
        result["error_message"] = str(eligibility["eligibility_errors"])
        return result

    source_media_id = eligibility.get("id")

    # Step 2 — upload video
    video_id = upload_instagram_video(token, ad_acct, source_media_id, ad_code)
    result["video_id"] = video_id
    if not video_id:
        result["status"] = "failed"
        result["error_message"] = "Video upload returned no ID."
        return result

    # Step 3 — create creative
    creative_id = create_ad_creative(
        token, ad_acct, fb_page, ig_acct,
        source_media_id, ad_code,
        cta_type, cta_install_link, cta_landing_link,
        product_set_id,
    )
    result["creative_id"] = creative_id
    if not creative_id:
        result["status"] = "failed"
        result["error_message"] = "Creative creation returned no ID."
        return result

    # Step 4 — create ad
    published_ad_id = create_ad(token, ad_acct, ad_name, adset_id, creative_id)
    result["published_ad_id"] = published_ad_id
    if not published_ad_id:
        result["status"] = "failed"
        result["error_message"] = "Ad creation returned no ID."
        return result

    result["status"] = "success"
    return result
