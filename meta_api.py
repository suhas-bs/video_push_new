"""
meta_api.py — Meta Graph API helpers  v5
"""
import json, re, requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
GRAPH   = "https://graph.facebook.com"
VERSION = "v5"   # bump this so you can confirm the right code is live


def _clean_id(val):
    s = str(val or "").strip()
    s = re.sub(r'\.0+$', '', s)
    return s or None


def get_page_ig_account(access_token, facebook_page_id):
    """Return the IG business account ID linked to a FB page."""
    r = requests.get(
        f"{GRAPH}/v23.0/{facebook_page_id}",
        params={"access_token": access_token,
                "fields": "instagram_business_account"},
        verify=False,
    )
    d = r.json()
    if r.status_code == 200:
        return d.get("instagram_business_account", {}).get("id"), None
    err = d.get("error", {})
    return None, f"{r.status_code}: {err.get('message', r.text)}"


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
    r   = requests.post(url, params=params, verify=False)
    d   = r.json()
    if r.status_code == 200 and "id" in d:
        return d["id"], None
    err = d.get("error", {})
    return None, f"[{label}] {r.status_code}: [{err.get('code','?')}] {err.get('message', r.text)}"


def create_ad_creative(access_token, ad_account_id, facebook_page_id, ig_account_id,
                       source_instagram_media_id, ad_code, cta_type,
                       cta_app_install_link, cta_app_landing_link):
    """
    Try every known adcreatives structure for partnership ads.
    Returns (creative_id, None) on first success, or (None, all_errors).
    """
    url = f"{GRAPH}/v23.0/act_{ad_account_id}/adcreatives"
    cta = json.dumps({
        "type":  cta_type,
        "value": {"link": cta_app_install_link, "app_link": cta_app_landing_link},
    })
    cta_obj = {
        "type":  cta_type,
        "value": {"link": cta_app_install_link, "app_link": cta_app_landing_link},
    }

    attempts = []   # (label, params_dict)

    if ad_code:
        # ── A: standard partnership-ad structure ────────────────────────────
        # instagram_actor_id = brand IG account, ad code as top-level field
        attempts.append(("A", {
            "access_token":                      access_token,
            "instagram_actor_id":                ig_account_id,
            "instagram_boost_post_access_token": ad_code,
            "call_to_action":                    cta,
        }))

        # ── A2: same but WITHOUT instagram_actor_id ─────────────────────────
        # Let Meta infer the actor from the ad account / page linkage
        attempts.append(("A2", {
            "access_token":                      access_token,
            "instagram_boost_post_access_token": ad_code,
            "call_to_action":                    cta,
        }))

        # ── B: object_story_spec with ig_actor + ad code inside link_data ──
        attempts.append(("B", {
            "access_token": access_token,
            "object_story_spec": json.dumps({
                "instagram_actor_id": ig_account_id,
                "link_data": {
                    "instagram_boost_post_access_token": ad_code,
                    "call_to_action": cta_obj,
                },
            }),
        }))

        # ── B2: object_story_spec with page_id instead of ig_actor ─────────
        attempts.append(("B2", {
            "access_token": access_token,
            "object_story_spec": json.dumps({
                "page_id":            facebook_page_id,
                "instagram_actor_id": ig_account_id,
                "link_data": {
                    "instagram_boost_post_access_token": ad_code,
                    "call_to_action": cta_obj,
                },
            }),
        }))

        # ── C: legacy nested branded_content with explicit sponsor fields ───
        attempts.append(("C", {
            "access_token":              access_token,
            "object_id":                 facebook_page_id,
            "facebook_branded_content":  json.dumps({"sponsor_page_id": facebook_page_id}),
            "instagram_branded_content": json.dumps({"sponsor_id": ig_account_id}),
            "branded_content":           json.dumps({"instagram_boost_post_access_token": ad_code}),
            "call_to_action":            cta,
        }))

        # ── C2: same without instagram_branded_content ──────────────────────
        attempts.append(("C2", {
            "access_token":             access_token,
            "object_id":                facebook_page_id,
            "facebook_branded_content": json.dumps({"sponsor_page_id": facebook_page_id}),
            "branded_content":          json.dumps({"instagram_boost_post_access_token": ad_code}),
            "call_to_action":           cta,
        }))

    elif source_instagram_media_id:
        attempts.append(("D", {
            "access_token":              access_token,
            "instagram_actor_id":        ig_account_id,
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

    # Auto-resolve the IG account linked to the FB page — more reliable than
    # the sidebar value which can be stale or mis-copied.
    fetched_ig, _ = get_page_ig_account(token, fb_page)
    if fetched_ig:
        ig_acct = fetched_ig

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

    # ── PATH 2: ad_code only → skip eligibility + video upload ───────────────
    if not media_id and not use_eligibility:
        creative_id, err = create_ad_creative(
            token, acct, fb_page, ig_acct,
            None, ad_code, cta_type, install_link, landing_link,
        )
        result["creative_id"] = creative_id
        if not creative_id:
            result.update({"status": "failed",
                           "error_message": f"[code v5] {err}"})
            return result

        published_ad_id, err = create_ad(token, acct, ad_name, adset_id, creative_id)
        result["published_ad_id"] = published_ad_id
        if not published_ad_id:
            result.update({"status": "failed",
                           "error_message": err or "Ad creation returned no ID"})
            return result

        result["status"] = "success"
        return result

    # ── PATH 3: legacy eligibility check ─────────────────────────────────────
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

    # ── PATH 1: media_id available ───────────────────────────────────────────
    video_id, err = upload_instagram_video(token, acct, media_id, ad_code)
    result["video_id"] = video_id
    if not video_id:
        result.update({"status": "failed",
                       "error_message": err or "Video upload returned no ID"})
        return result

    creative_id, err = create_ad_creative(
        token, acct, fb_page, ig_acct, media_id, ad_code,
        cta_type, install_link, landing_link,
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
