"""
meta_api.py — Meta Graph API helpers  v6
"""
import json, re, requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
GRAPH   = "https://graph.facebook.com"
VERSION = "v6"


def _clean_id(val):
    s = str(val or "").strip()
    s = re.sub(r'\.0+$', '', s)
    return s or None


def get_ig_accounts(access_token, facebook_page_id, ad_account_id):
    """
    Return a deduplicated list of IG account IDs to try as instagram_actor_id.
    Source 1: IG account linked to the Facebook page
    Source 2: IG accounts linked to the ad account
    """
    ig_ids = []

    r = requests.get(
        f"{GRAPH}/v23.0/{facebook_page_id}",
        params={"access_token": access_token,
                "fields": "instagram_business_account"},
        verify=False,
    )
    if r.status_code == 200:
        ig_id = r.json().get("instagram_business_account", {}).get("id")
        if ig_id and ig_id not in ig_ids:
            ig_ids.append(ig_id)

    r2 = requests.get(
        f"{GRAPH}/v23.0/act_{ad_account_id}",
        params={"access_token": access_token,
                "fields": "instagram_accounts{id}"},
        verify=False,
    )
    if r2.status_code == 200:
        for acct in r2.json().get("instagram_accounts", {}).get("data", []):
            ig_id = acct.get("id")
            if ig_id and ig_id not in ig_ids:
                ig_ids.append(ig_id)

    return ig_ids


def fetch_eligibility(access_token, ig_account_id, ad_code=None, permalinks=None):
    url = f"{GRAPH}/v22.0/{ig_account_id}/branded_content_advertisable_medias"
    params = {"access_token": access_token,
              "fields": "eligibility_errors,owner_id,permalink,id,has_permission_for_partnership_ad"}
    if ad_code:      params["ad_code"] = ad_code
    elif permalinks: params["permalinks"] = json.dumps(permalinks)
    else:            raise ValueError("ad_code or permalinks required")
    r = requests.get(url, params=params, verify=False)
    if r.status_code == 200:
        data = r.json().get("data", [])
        return data[0] if data else {"error": "No data returned"}
    return {"error": f"{r.status_code} — {r.text}"}


def upload_instagram_video(access_token, ad_account_id, source_instagram_media_id, ad_code=None):
    url = f"{GRAPH}/v22.0/act_{ad_account_id}/advideos"
    params = {"source_instagram_media_id": source_instagram_media_id,
              "access_token": access_token}
    if ad_code:
        params["partnership_ad_ad_code"] = ad_code
        params["is_partnership_ad"]      = True
    r = requests.post(url, params=params, verify=False)
    d = r.json()
    if r.status_code == 200 and "id" in d:
        return d["id"], None
    err = d.get("error", {})
    return None, f"advideos {r.status_code}: [{err.get('code','?')}] {err.get('message', r.text)}"


def _post_creative(label, url, params):
    r = requests.post(url, params=params, verify=False)
    d = r.json()
    if r.status_code == 200 and "id" in d:
        return d["id"], None
    err = d.get("error", {})
    return None, f"[{label}] {r.status_code}: [{err.get('code','?')}] {err.get('message', r.text)}"


def create_ad_creative(access_token, ad_account_id, facebook_page_id, ig_account_id,
                       ig_candidates, source_instagram_media_id, ad_code, cta_type,
                       cta_app_install_link, cta_app_landing_link, creative_name=""):
    url  = f"{GRAPH}/v23.0/act_{ad_account_id}/adcreatives"
    name = creative_name or "partnership_ad_creative"
    cta  = json.dumps({
        "type":  cta_type,
        "value": {"link": cta_app_install_link, "app_link": cta_app_landing_link},
    })
    cta_obj = {
        "type":  cta_type,
        "value": {"link": cta_app_install_link, "app_link": cta_app_landing_link},
    }

    all_ig = list(dict.fromkeys(ig_candidates + ([ig_account_id] if ig_account_id else [])))
    attempts = []

    if ad_code:
        for ig in all_ig:
            attempts.append((f"A[{ig}]", {
                "access_token":                      access_token,
                "name":                              name,
                "instagram_actor_id":                ig,
                "instagram_boost_post_access_token": ad_code,
                "call_to_action":                    cta,
            }))

        attempts.append(("A-noactor", {
            "access_token":                      access_token,
            "name":                              name,
            "instagram_boost_post_access_token": ad_code,
            "call_to_action":                    cta,
        }))

        for ig in all_ig:
            attempts.append((f"B[{ig}]", {
                "access_token": access_token,
                "name":         name,
                "object_story_spec": json.dumps({
                    "instagram_actor_id": ig,
                    "link_data": {
                        "instagram_boost_post_access_token": ad_code,
                        "call_to_action": cta_obj,
                    },
                }),
            }))

        for ig in all_ig:
            attempts.append((f"C[{ig}]", {
                "access_token":              access_token,
                "name":                      name,
                "object_id":                 facebook_page_id,
                "facebook_branded_content":  json.dumps({"sponsor_page_id": facebook_page_id}),
                "instagram_branded_content": json.dumps({"sponsor_id": ig}),
                "branded_content":           json.dumps({"instagram_boost_post_access_token": ad_code}),
                "call_to_action":            cta,
            }))

    elif source_instagram_media_id:
        for ig in all_ig:
            attempts.append((f"D[{ig}]", {
                "access_token":              access_token,
                "name":                      name,
                "instagram_actor_id":        ig,
                "source_instagram_media_id": source_instagram_media_id,
                "call_to_action":            cta,
            }))
    else:
        raise ValueError("ad_code or source_instagram_media_id required")

    errors = []
    for label, params in attempts:
        cid, err = _post_creative(label, url, params)
        if cid:
            return cid, None
        errors.append(err)

    return None, " | ".join(errors)


def create_ad(access_token, ad_account_id, ad_name, adset_id, creative_id):
    url = f"{GRAPH}/v22.0/act_{ad_account_id}/ads"
    params = {
        "access_token": access_token,
        "status":       "PAUSED",
        "name":         ad_name,
        "adset_id":     adset_id,
        "creative":     json.dumps({"creative_id": creative_id}),
    }
    r = requests.post(url, params=params, verify=False)
    d = r.json()
    if r.status_code == 200 and "id" in d:
        return d["id"], None
    err = d.get("error", {})
    return None, f"ads {r.status_code}: [{err.get('code','?')}] {err.get('message', r.text)}"


def process_row(row, config):
    result = dict(row)
    result.update({"video_id": None, "creative_id": None,
                   "published_ad_id": None, "status": "skipped", "error_message": ""})

    token   = config["access_token"]
    acct    = _clean_id(config["ad_account_id"])
    fb_page = config["facebook_page_id"]
    ig_acct = config["ig_account_id"]

    ig_candidates = get_ig_accounts(token, fb_page, acct)

    ad_code      = str(row.get("ad_code", "")).strip() or None
    cta_type     = str(row.get("cta_type", "SHOP_NOW")).strip()
    install_link = str(row.get("cta_app_install_link", "")).strip()
    landing_link = str(row.get("cta_app_landing_link", "")).strip()
    ad_name      = str(row.get("ad_name", "")).strip()
    adset_id     = _clean_id(row.get("adset_id", ""))
    media_id     = _clean_id(row.get("instagram_media_id", ""))

    use_eligibility = config.get("use_eligibility_api", False)

    if not media_id and not ad_code:
        result.update({"status": "skipped",
                       "error_message": "Neither instagram_media_id nor ad_code provided"})
        return result

    diag = f"[v6 ig_resolved={ig_candidates}] "

    if not media_id and not use_eligibility:
        creative_id, err = create_ad_creative(
            token, acct, fb_page, ig_acct, ig_candidates,
            None, ad_code, cta_type, install_link, landing_link, ad_name,
        )
        result["creative_id"] = creative_id
        if not creative_id:
            result.update({"status": "failed",
                           "error_message": diag + (err or "Creative creation returned no ID")})
            return result

        published_ad_id, err = create_ad(token, acct, ad_name, adset_id, creative_id)
        result["published_ad_id"] = published_ad_id
        if not published_ad_id:
            result.update({"status": "failed",
                           "error_message": diag + (err or "Ad creation returned no ID")})
            return result

        result["status"] = "success"
        return result

    if not media_id and use_eligibility:
        elig = fetch_eligibility(token, ig_acct, ad_code=ad_code)
        if "error" in elig:
            result.update({"status": "failed",
                           "error_message": f"Eligibility: {elig['error']}"})
            return result
        if elig.get("eligibility_errors"):
            result.update({"status": "ineligible",
                           "error_message": str(elig["eligibility_errors"])})
            return result
        media_id = elig.get("id")

    video_id, err = upload_instagram_video(token, acct, media_id, ad_code)
    result["video_id"] = video_id
    if not video_id:
        result.update({"status": "failed",
                       "error_message": err or "Video upload returned no ID"})
        return result

    creative_id, err = create_ad_creative(
        token, acct, fb_page, ig_acct, ig_candidates,
        media_id, ad_code, cta_type, install_link, landing_link, ad_name,
    )
    result["creative_id"] = creative_id
    if not creative_id:
        result.update({"status": "failed",
                       "error_message": err or "Creative creation returned no ID"})
        return result

    published_ad_id, err = create_ad(token, acct, ad_name, adset_id, creative_id)
    result["published_ad_id"] = published_ad_id
    if not published_ad_id:
        result.update({"status": "failed",
                       "error_message": err or "Ad creation returned no ID"})
        return result

    result["status"] = "success"
    return result
