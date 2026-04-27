"""
meta_api.py — Meta Graph API helpers  v10
Approach:
  - PRIMARY (no special permissions needed):
      instagram_actor_id (creator's IG) + instagram_boost_post_access_token (ad_code)
      Proved working in v6 for Akhil & Juvella. Requires creator_ig_account_id in CSV.
  - FALLBACK (needs instagram_branded_content_ads permission):
      Full eligibility flow (notebook token path)
  - LAST RESORT:
      branded_content structure without actor (also needs permission, will fail cleanly)
"""
import json, re, requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GRAPH   = "https://graph.facebook.com"
VERSION = "v10"

_PERMISSION_CODES = {3, 10, 200}


def _clean_id(val):
    s = str(val or "").strip()
    s = re.sub(r'\.0+$', '', s)
    return s or None


def _is_permission_error(resp_json):
    return resp_json.get("error", {}).get("code") in _PERMISSION_CODES


def get_ig_accounts(access_token, facebook_page_id, ad_account_id):
    """For the Verify Accounts block."""
    ig_ids = []
    r = requests.get(f"{GRAPH}/v23.0/{facebook_page_id}",
                     params={"access_token": access_token,
                             "fields": "instagram_business_account"},
                     verify=False)
    if r.status_code == 200:
        ig_id = r.json().get("instagram_business_account", {}).get("id")
        if ig_id:
            ig_ids.append(ig_id)
    return ig_ids


def _fetch_eligibility(token, ig_account_id, ad_code):
    """Returns (media_dict | None, error_str | None, is_perm_error)."""
    url = f"{GRAPH}/v22.0/{ig_account_id}/branded_content_advertisable_medias"
    params = {
        "access_token": token,
        "fields": "eligibility_errors,owner_id,permalink,id,has_permission_for_partnership_ad",
        "ad_code": ad_code,
    }
    r = requests.get(url, params=params, verify=False)
    d = r.json()
    if r.status_code == 200:
        data = d.get("data", [])
        if data:
            return data[0], None, False
        return None, "Eligibility API: no data returned", False
    is_perm = _is_permission_error(d)
    err = d.get("error", {})
    return None, f"{r.status_code}: {err.get('message', r.text)}", is_perm


def _upload_video(token, ad_account_id, media_id, ad_code):
    url = f"{GRAPH}/v22.0/act_{ad_account_id}/advideos"
    params = {
        "source_instagram_media_id": media_id,
        "access_token": token,
        "partnership_ad_ad_code": ad_code,
        "is_partnership_ad": True,
    }
    r = requests.post(url, params=params, verify=False)
    d = r.json()
    if r.status_code == 200 and "id" in d:
        return d["id"], None
    err = d.get("error", {})
    return None, f"advideos {r.status_code}: [{err.get('code','?')}] {err.get('message', r.text)}"


def _create_ad(token, ad_account_id, ad_name, adset_id, creative_id):
    url = f"{GRAPH}/v22.0/act_{ad_account_id}/ads"
    params = {
        "access_token": token,
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
    msg = f"ads {r.status_code}: [{err.get('code','?')}] {err.get('message', r.text)}"
    for k in ("error_user_msg", "error_user_title"):
        if err.get(k):
            msg += f" | {err[k]}"
    return None, msg


def _try_creative(label, token, ad_account_id, params):
    """POST to adcreatives. Returns (creative_id | None, error_str)."""
    url = f"{GRAPH}/v23.0/act_{ad_account_id}/adcreatives"
    params["access_token"] = token
    r = requests.post(url, params=params, verify=False)
    d = r.json()
    if r.status_code == 200 and "id" in d:
        return d["id"], None
    err = d.get("error", {})
    msg = f"[{label}] {r.status_code}: [{err.get('code','?')}] {err.get('message', r.text)}"
    # surface extra detail when available
    for k in ("error_user_msg", "error_user_title"):
        if err.get(k):
            msg += f" | {err[k]}"
    return None, msg


def _build_cta_json(cta_type, install_link, landing_link):
    return json.dumps({
        "type": cta_type,
        "value": {"link": install_link, "app_link": landing_link},
    })


def _product_params(product_set_id):
    if not product_set_id:
        return {}
    return {
        "degrees_of_freedom_spec": json.dumps({
            "creative_features_spec": {
                "product_extensions": {"enroll_status": "OPT_IN"}
            }
        }),
        "creative_sourcing_spec": json.dumps({
            "associated_product_set_id": str(product_set_id)
        }),
    }


def process_row(row, config):
    result = dict(row)
    result.update({
        "video_id": None, "creative_id": None,
        "published_ad_id": None, "status": "skipped", "error_message": "",
    })

    token   = config["access_token"]
    acct    = _clean_id(config["ad_account_id"])
    fb_page = config["facebook_page_id"]
    ig_acct = config["ig_account_id"]   # brand IG (Flipkart Minutes)

    ad_code      = str(row.get("ad_code", "")).strip() or None
    cta_type     = str(row.get("cta_type", "SHOP_NOW")).strip()
    install_link = str(row.get("cta_app_install_link", "")).strip()
    landing_link = str(row.get("cta_app_landing_link", "")).strip()
    ad_name      = str(row.get("ad_name", "")).strip()
    adset_id     = _clean_id(row.get("adset_id", ""))
    ps_raw       = str(row.get("product_set_id", "")).strip()
    product_set_id = ps_raw if ps_raw.lower() not in ("", "nan", "none", "error", "eror") else None

    # Creator's own IG account ID — key to making partnership ads work without special permissions
    cr_raw     = str(row.get("creator_ig_account_id", "")).strip()
    creator_ig = cr_raw if cr_raw.lower() not in ("", "nan", "none") else None

    cta = _build_cta_json(cta_type, install_link, landing_link)
    pp  = _product_params(product_set_id)

    if not ad_code:
        result.update({"status": "skipped", "error_message": "No ad_code provided"})
        return result

    all_errors = []

    # ══════════════════════════════════════════════════════════════════════════
    # PATH 1 — creator_ig_account_id present in CSV
    #   Uses: instagram_actor_id (creator) + instagram_boost_post_access_token
    #   No special permissions needed. Proved working in v6.
    # ══════════════════════════════════════════════════════════════════════════
    if creator_ig:
        params = {
            "name":                              ad_name,
            "instagram_actor_id":                creator_ig,
            "instagram_boost_post_access_token": ad_code,
            "call_to_action":                    cta,
            **pp,
        }
        cid, err = _try_creative("creator-actor", token, acct, params)
        if cid:
            result["creative_id"] = cid
            pub_id, err2 = _create_ad(token, acct, ad_name, adset_id, cid)
            result["published_ad_id"] = pub_id
            if pub_id:
                result["status"] = "success"
                return result
            all_errors.append(f"Ad: {err2}")
        else:
            all_errors.append(err)

    # ══════════════════════════════════════════════════════════════════════════
    # PATH 2 — Full eligibility flow (needs instagram_branded_content_ads perm)
    #   Works with the notebook token. Gets media_id → upload video → creative.
    # ══════════════════════════════════════════════════════════════════════════
    media_result, elig_err, is_perm_err = _fetch_eligibility(token, ig_acct, ad_code)

    if media_result is not None:
        elig_errors = media_result.get("eligibility_errors", [])
        if elig_errors:
            result.update({"status": "ineligible",
                           "error_message": f"[PATH-2] Not eligible: {elig_errors}"})
            return result

        media_id   = media_result.get("id")
        owner_ig   = media_result.get("owner_id")  # creator's IG from eligibility

        if media_id:
            video_id, _ = _upload_video(token, acct, media_id, ad_code)
            result["video_id"] = video_id

            # Try with media owner IG if available
            for actor in ([owner_ig] if owner_ig else []) + [ig_acct]:
                params = {
                    "name":                              ad_name,
                    "instagram_actor_id":                actor,
                    "instagram_boost_post_access_token": ad_code,
                    "call_to_action":                    cta,
                    **pp,
                }
                cid, err = _try_creative(f"elig-actor[{actor}]", token, acct, params)
                if cid:
                    result["creative_id"] = cid
                    pub_id, err2 = _create_ad(token, acct, ad_name, adset_id, cid)
                    result["published_ad_id"] = pub_id
                    if pub_id:
                        result["status"] = "success"
                        return result
                    all_errors.append(f"Ad: {err2}")
                    break
                all_errors.append(err)

            # Also try branded_content structure (notebook path)
            params = {
                "name":                         ad_name,
                "object_id":                    fb_page,
                "facebook_branded_content":     json.dumps({"sponsor_page_id": int(fb_page)}),
                "instagram_branded_content":    json.dumps({"sponsor_id": int(ig_acct)}),
                "branded_content":              json.dumps({"instagram_boost_post_access_token": ad_code}),
                "call_to_action":               cta,
                **pp,
            }
            cid, err = _try_creative("elig-branded", token, acct, params)
            if cid:
                result["creative_id"] = cid
                pub_id, err2 = _create_ad(token, acct, ad_name, adset_id, cid)
                result["published_ad_id"] = pub_id
                if pub_id:
                    result["status"] = "success"
                    return result
                all_errors.append(f"Ad: {err2}")
            else:
                all_errors.append(err)

    else:
        all_errors.append(f"Eligibility: {elig_err}")

    # ══════════════════════════════════════════════════════════════════════════
    # PATH 3 — branded_content without eligibility (last resort)
    #   Skip creative creation if we already have a creative_id from PATH 2
    #   (avoids orphaned creatives — the ad creation is the bottleneck, not the creative)
    # ══════════════════════════════════════════════════════════════════════════
    existing_cid = result.get("creative_id")
    if existing_cid:
        # Re-try ad creation with the creative we already made
        pub_id, err2 = _create_ad(token, acct, ad_name, adset_id, existing_cid)
        result["published_ad_id"] = pub_id
        if pub_id:
            result["status"] = "success"
            return result
        all_errors.append(f"Ad[retry-existing-creative]: {err2}")
    else:
        params = {
            "name":                         ad_name,
            "object_id":                    fb_page,
            "facebook_branded_content":     json.dumps({"sponsor_page_id": int(fb_page)}),
            "instagram_branded_content":    json.dumps({"sponsor_id": int(ig_acct)}),
            "branded_content":              json.dumps({"instagram_boost_post_access_token": ad_code}),
            "call_to_action":               cta,
            **pp,
        }
        cid, err = _try_creative("direct-branded", token, acct, params)
        if cid:
            result["creative_id"] = cid
            pub_id, err2 = _create_ad(token, acct, ad_name, adset_id, cid)
            result["published_ad_id"] = pub_id
            if pub_id:
                result["status"] = "success"
                return result
            all_errors.append(f"Ad: {err2}")
        else:
            all_errors.append(err)

    # All paths failed
    hint = ""
    if is_perm_err and not creator_ig:
        hint = (" | ⚠️ FIX: either (A) add 'creator_ig_account_id' column to CSV"
                " with each creator's numeric Instagram account ID,"
                " or (B) use the token from your working notebook.")
    result.update({
        "status": "failed",
        "error_message": " | ".join(all_errors) + hint,
    })
    return result
