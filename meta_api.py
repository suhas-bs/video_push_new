"""
meta_api.py — Meta Graph API helpers  v9
v9: Two-path flow
    Path A (full-permission token): eligibility → upload video → create creative → ad
    Path B (limited token): skip eligibility, create creative directly with
           branded_content + facebook/instagram_branded_content structure → ad
"""
import json, re, requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GRAPH   = "https://graph.facebook.com"
VERSION = "v9"

_PERMISSION_CODES = {3, 10, 200}  # Meta permission-related error codes


def _clean_id(val):
    s = str(val or "").strip()
    s = re.sub(r'\.0+$', '', s)
    return s or None


def _is_permission_error(resp_json):
    code = resp_json.get("error", {}).get("code")
    return code in _PERMISSION_CODES


# kept for the Verify Accounts block in app.py
def get_ig_accounts(access_token, facebook_page_id, ad_account_id):
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
    """Returns (media_dict_or_None, error_str_or_None, is_permission_error)."""
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
        return None, "Eligibility API returned no data", False
    is_perm = _is_permission_error(d)
    err_msg = d.get("error", {}).get("message", r.text)
    return None, f"{r.status_code}: {err_msg}", is_perm


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


def _create_creative(token, ad_account_id, facebook_page_id, ig_account_id,
                     ad_code, cta_type, install_link, landing_link,
                     creative_name, product_set_id,
                     source_instagram_media_id=None):
    """
    Notebook-proven creative structure:
      object_id                 = facebook_page_id
      facebook_branded_content  = {"sponsor_page_id": facebook_page_id}
      instagram_branded_content = {"sponsor_id": ig_account_id}
      branded_content           = {"instagram_boost_post_access_token": ad_code}
      call_to_action            = {...}
    source_instagram_media_id is used as fallback when no ad_code (unlikely but safe).
    """
    url    = f"{GRAPH}/v23.0/act_{ad_account_id}/adcreatives"
    name   = creative_name or "partnership_ad_creative"
    cta    = json.dumps({
        "type": cta_type,
        "value": {"link": install_link, "app_link": landing_link},
    })

    params = {
        "access_token":               token,
        "name":                       name,
        "object_id":                  facebook_page_id,
        "facebook_branded_content":   json.dumps({"sponsor_page_id": facebook_page_id}),
        "instagram_branded_content":  json.dumps({"sponsor_id": ig_account_id}),
        "call_to_action":             cta,
    }

    if ad_code:
        params["branded_content"] = json.dumps({"instagram_boost_post_access_token": ad_code})
    elif source_instagram_media_id:
        params["source_instagram_media_id"] = source_instagram_media_id
    else:
        return None, "No ad_code or media_id for creative"

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


def _create_ad(token, ad_account_id, ad_name, adset_id, creative_id):
    url = f"{GRAPH}/v22.0/act_{ad_account_id}/ads"
    params = {
        "access_token": token,
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
    ps_raw       = str(row.get("product_set_id", "")).strip()
    product_set_id = ps_raw if ps_raw.lower() not in ("", "nan", "none", "error", "eror") else None

    if not ad_code:
        result.update({"status": "skipped", "error_message": "No ad_code provided"})
        return result

    # ── Path A: try eligibility first (needs instagram_branded_content_ads perm) ──
    media_result, elig_err, is_perm_err = _fetch_eligibility(token, ig_acct, ad_code)

    if media_result is not None:
        # Eligibility succeeded — check for content-level errors
        elig_errors = media_result.get("eligibility_errors", [])
        if elig_errors:
            result.update({"status": "ineligible",
                           "error_message": f"[v9-A] Not eligible: {elig_errors}"})
            return result

        media_id = media_result.get("id")
        if media_id:
            # upload video asset
            video_id, err = _upload_video(token, acct, media_id, ad_code)
            result["video_id"] = video_id
            # video upload failure is non-fatal — creative can still work via branded_content
            if not video_id:
                result["error_message"] = f"[v9-A] Video upload skipped: {err}"

            # create creative
            creative_id, err = _create_creative(
                token, acct, fb_page, ig_acct,
                ad_code, cta_type, install_link, landing_link,
                ad_name, product_set_id, source_instagram_media_id=media_id,
            )
            result["creative_id"] = creative_id
            if not creative_id:
                result.update({"status": "failed",
                               "error_message": f"[v9-A] Creative failed: {err}"})
                return result

            pub_id, err = _create_ad(token, acct, ad_name, adset_id, creative_id)
            result["published_ad_id"] = pub_id
            if not pub_id:
                result.update({"status": "failed",
                               "error_message": f"[v9-A] Ad creation failed: {err}"})
                return result

            result["status"] = "success"
            return result

    # ── Path B: eligibility skipped (permission error) or no media_id returned ──
    # Directly create creative with branded_content structure — no media upload needed
    path_b_note = f"[v9-B perm_skip={is_perm_err}]"

    creative_id, err = _create_creative(
        token, acct, fb_page, ig_acct,
        ad_code, cta_type, install_link, landing_link,
        ad_name, product_set_id,
    )
    result["creative_id"] = creative_id
    if not creative_id:
        # Surface both the eligibility error and the creative error
        elig_note = f"Eligibility: {elig_err} | " if elig_err else ""
        result.update({"status": "failed",
                       "error_message": f"{path_b_note} {elig_note}Creative: {err}"})
        return result

    pub_id, err = _create_ad(token, acct, ad_name, adset_id, creative_id)
    result["published_ad_id"] = pub_id
    if not pub_id:
        result.update({"status": "failed",
                       "error_message": f"{path_b_note} Ad: {err}"})
        return result

    result["status"] = "success"
    return result
