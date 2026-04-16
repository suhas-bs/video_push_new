"""
meta_api.py — Meta Graph API helpers
"""
import json, requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
GRAPH = "https://graph.facebook.com"

def fetch_eligibility(access_token, ig_account_id, ad_code=None, permalinks=None):
    url = f"{GRAPH}/v22.0/{ig_account_id}/branded_content_advertisable_medias"
    params = {"access_token": access_token,
              "fields": "eligibility_errors,owner_id,permalink,id,has_permission_for_partnership_ad"}
    if ad_code: params["ad_code"] = ad_code
    elif permalinks: params["permalinks"] = json.dumps(permalinks)
    else: raise ValueError("ad_code or permalinks required")
    r = requests.get(url, params=params, verify=False)
    if r.status_code == 200:
        data = r.json().get("data", [])
        return data[0] if data else {"error": "No data returned"}
    return {"error": f"{r.status_code} — {r.text}"}

def upload_instagram_video(access_token, ad_account_id, source_instagram_media_id, ad_code=None):
    url = f"{GRAPH}/v22.0/act_{ad_account_id}/advideos"
    params = {"source_instagram_media_id": source_instagram_media_id, "access_token": access_token}
    if ad_code:
        params["partnership_ad_ad_code"] = ad_code
        params["is_partnership_ad"] = True
    r = requests.post(url, params=params, verify=False)
    return r.json().get("id") if r.status_code == 200 else None

def create_ad_creative(access_token, ad_account_id, facebook_page_id, ig_account_id,
                       source_instagram_media_id, ad_code, cta_type,
                       cta_app_install_link, cta_app_landing_link, product_set_id=None):
    url = f"{GRAPH}/v23.0/act_{ad_account_id}/adcreatives"
    params = {
        "access_token": access_token,
        "object_id": facebook_page_id,
        "facebook_branded_content": json.dumps({"sponsor_page_id": facebook_page_id}),
        "instagram_branded_content": json.dumps({"sponsor_id": ig_account_id}),
        "call_to_action": json.dumps({"type": cta_type,
            "value": {"link": cta_app_install_link, "app_link": cta_app_landing_link}}),
    }
    if ad_code: params["branded_content"] = json.dumps({"instagram_boost_post_access_token": ad_code})
    elif source_instagram_media_id: params["source_instagram_media_id"] = source_instagram_media_id
    else: raise ValueError("ad_code or source_instagram_media_id required")
    if product_set_id:
        params["degrees_of_freedom_spec"] = json.dumps(
            {"creative_features_spec": {"product_extensions": {"enroll_status": "OPT_IN"}}})
        params["creative_sourcing_spec"] = json.dumps(
            {"associated_product_set_id": str(product_set_id)})
    for _ in range(2):
        r = requests.post(url, params=params, verify=False)
        d = r.json()
        if r.status_code == 200 and "id" in d: return d["id"]
    return None

def create_ad(access_token, ad_account_id, ad_name, adset_id, creative_id):
    url = f"{GRAPH}/v22.0/act_{ad_account_id}/ads"
    params = {"access_token": access_token, "status": "PAUSED", "name": ad_name,
              "adset_id": adset_id, "creative": json.dumps({"creative_id": creative_id})}
    r = requests.post(url, params=params, verify=False)
    return r.json().get("id") if r.status_code == 200 else None

def process_row(row, config):
    result = dict(row)
    result.update({"video_id": None, "creative_id": None,
                   "published_ad_id": None, "status": "skipped", "error_message": ""})
    token, acct, fb_page, ig_acct = (config["access_token"], config["ad_account_id"],
                                      config["facebook_page_id"], config["ig_account_id"])
    ad_code        = str(row.get("ad_code", "")).strip() or None
    cta_type       = row.get("cta_type", "SHOP_NOW")
    install_link   = row.get("cta_app_install_link", "")
    landing_link   = row.get("cta_app_landing_link", "")
    ad_name        = row.get("ad_name", "")
    adset_id       = row.get("adset_id", "")
    product_set_id = row.get("product_set_id") or None

    # ── Media ID / eligibility resolution ────────────────────────────────────
    #
    # Three paths — tried in order:
    #
    # PATH 1 · instagram_media_id in CSV
    #   → skip eligibility API, use the ID directly for upload + creative
    #
    # PATH 2 · ad_code only, no instagram_media_id
    #   → skip eligibility API AND video upload entirely
    #   → create creative directly with ad_code via branded_content param
    #   → avoids branded_content_advertisable_medias permission requirement
    #
    # PATH 3 · legacy / has full permissions
    #   → call eligibility API with ad_code to resolve media_id, then full flow
    #   → only used if config flag "use_eligibility_api" is True

    media_id = str(row.get("instagram_media_id", "")).strip() or None
    use_eligibility = config.get("use_eligibility_api", False)

    if not media_id and not ad_code:
        result.update({"status": "skipped",
                       "error_message": "Neither instagram_media_id nor ad_code provided"})
        return result

    if not media_id and not use_eligibility:
        # PATH 2 — ad_code only, skip straight to creative
        creative_id = create_ad_creative(
            token, acct, fb_page, ig_acct,
            None, ad_code, cta_type, install_link, landing_link, product_set_id
        )
        result["creative_id"] = creative_id
        if not creative_id:
            result.update({"status": "failed",
                           "error_message": "Creative creation returned no ID"}); return result
        published_ad_id = create_ad(token, acct, ad_name, adset_id, creative_id)
        result["published_ad_id"] = published_ad_id
        if not published_ad_id:
            result.update({"status": "failed",
                           "error_message": "Ad creation returned no ID"}); return result
        result["status"] = "success"
        return result

    if not media_id and use_eligibility:
        # PATH 3 — legacy full flow via eligibility API
        elig = fetch_eligibility(token, ig_acct, ad_code=ad_code)
        if "error" in elig:
            result.update({"status": "failed",
                           "error_message": f"Eligibility: {elig['error']}"}); return result
        if elig.get("eligibility_errors"):
            result.update({"status": "ineligible",
                           "error_message": str(elig["eligibility_errors"])}); return result
        media_id = elig.get("id")

    video_id = upload_instagram_video(token, acct, media_id, ad_code)
    result["video_id"] = video_id
    if not video_id:
        result.update({"status": "failed", "error_message": "Video upload returned no ID"}); return result

    creative_id = create_ad_creative(token, acct, fb_page, ig_acct, media_id, ad_code,
                                     cta_type, install_link, landing_link, product_set_id)
    result["creative_id"] = creative_id
    if not creative_id:
        result.update({"status": "failed", "error_message": "Creative creation returned no ID"}); return result

    published_ad_id = create_ad(token, acct, ad_name, adset_id, creative_id)
    result["published_ad_id"] = published_ad_id
    if not published_ad_id:
        result.update({"status": "failed", "error_message": "Ad creation returned no ID"}); return result

    result["status"] = "success"
    return result
