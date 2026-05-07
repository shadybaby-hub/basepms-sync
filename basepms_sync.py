import requests
import json
import os
import time
import base64
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ───────────────────────────────────────────────────
API_ROOT        = "https://hfs.api.basepms.com"
API_TOKEN       = os.environ.get("BASEPMS_API_TOKEN", "")
SHEET_ID        = os.environ.get("SHEET_ID", "")
FORCE_PUSH      = os.environ.get("FORCE_PUSH", "false").lower() == "true"
GITHUB_TOKEN    = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO     = "shadybaby-hub/basepms-sync"
GITHUB_BRANCH   = "main"
IMAGES_FOLDER   = "images"
DELAY_SECONDS   = 1.1

ACADEMIC_YEARS = ["2025/2026", "2026/2027"]

MAIN_HEADERS = [
    "brand", "property_name", "city", "room_type", "academic_year",
    "duration_weeks", "price_per_week", "price_formatted", "available",
    "thumbnail", "instalment_name", "start_date", "end_date",
    "base_hub_url", "scraped_at"
]

IMAGE_HEADERS = [
    "brand", "property_name", "city", "room_type",
    "image_id", "image_name", "image_url"
]

BRAND_LOOKUP = {
    "wearehomesforstudents.com":  "Homes for Students",
    "prestigestudentliving.com":  "Prestige Student Living",
    "presitgestudentliving.com":  "Prestige Student Living",
    "urbanstudentlife.com":       "Urban Student Life",
    "universalstudentliving.com": "Universal Student Living",
    "essentialstudentliving.com": "Essential Student Living",
    "evostudent.com":             "Evo Student",
    "arkstudent.com":             "ARK Student",
    "heyday.ie":                  "Heyday",
    "weareoneliving.com":         "One Living",
}

# ── HELPERS ───────────────────────────────────────────────────
def get_brand(email):
    if not email or "@" not in email:
        return ""
    domain = email.split("@")[-1].lower().strip()
    return BRAND_LOOKUP.get(domain, "")

def api_get(path, params=None):
    url = API_ROOT + path
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()

def to_list(resp):
    if not resp:
        return []
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        for key in ["data", "results", "items", "properties", "room_types", "instalments"]:
            if isinstance(resp.get(key), list):
                return resp[key]
        return [resp]
    return []

# ── IMAGE RE-HOSTING ──────────────────────────────────────────
# Tracks filenames already uploaded this run to avoid duplicate API calls
_uploaded_this_run = set()

def get_existing_github_images():
    """Fetch the list of already-uploaded images from the GitHub repo."""
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    existing = set()
    page = 1
    while True:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{IMAGES_FOLDER}?per_page=100&page={page}"
        response = requests.get(url, headers=headers)
        if response.status_code == 404:
            break
        if not response.ok:
            break
        items = response.json()
        if not items:
            break
        existing.update(item["name"] for item in items)
        if len(items) < 100:
            break
        page += 1
    return existing

def upload_image_to_github(image_url, existing_filenames):
    """
    Downloads an image from BasePMS using the Bearer token,
    then uploads it to the GitHub repo's images/ folder.
    Returns the public raw.githubusercontent.com URL, or the
    original URL if anything fails.
    """
    if not GITHUB_TOKEN:
        return image_url

    # Extract filename from URL
    filename = image_url.split("/")[-1].split("?")[0]
    if not filename:
        return image_url

    # Skip if already uploaded (either this run or in a previous run)
    if filename in _uploaded_this_run or filename in existing_filenames:
        public_url = (
            f"https://raw.githubusercontent.com/{GITHUB_REPO}/"
            f"{GITHUB_BRANCH}/{IMAGES_FOLDER}/{filename}"
        )
        return public_url

    # Download image from BasePMS with auth
    try:
        img_response = requests.get(
            image_url,
            headers={"Authorization": f"Bearer {API_TOKEN}"},
            timeout=15
        )
        img_response.raise_for_status()
        image_data = img_response.content
    except Exception as e:
        print(f"    ⚠  Image download failed ({filename}): {e}")
        return image_url

    # Upload to GitHub
    try:
        encoded = base64.b64encode(image_data).decode("utf-8")
        gh_url = (
            f"https://api.github.com/repos/{GITHUB_REPO}/"
            f"contents/{IMAGES_FOLDER}/{filename}"
        )
        payload = {
            "message": f"Add image {filename}",
            "content": encoded,
            "branch": GITHUB_BRANCH
        }
        gh_headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json"
        }
        gh_response = requests.put(gh_url, json=payload, headers=gh_headers, timeout=30)

        if gh_response.status_code in (200, 201):
            _uploaded_this_run.add(filename)
            public_url = (
                f"https://raw.githubusercontent.com/{GITHUB_REPO}/"
                f"{GITHUB_BRANCH}/{IMAGES_FOLDER}/{filename}"
            )
            return public_url
        else:
            print(f"    ⚠  GitHub upload failed ({filename}): {gh_response.status_code}")
            return image_url

    except Exception as e:
        print(f"    ⚠  GitHub upload error ({filename}): {e}")
        return image_url

# ── GOOGLE SHEETS AUTH ────────────────────────────────────────
def get_gspread_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
    creds_dict = json.loads(creds_json)
    scopes     = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds  = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client

def get_or_create_tab(spreadsheet, tab_name):
    try:
        sheet = spreadsheet.worksheet(tab_name)
        sheet.clear()
        print(f"  Cleared existing tab: {tab_name}")
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=tab_name, rows=10000, cols=20)
        print(f"  Created new tab: {tab_name}")
    return sheet

# ── FETCH ALL PROPERTIES ──────────────────────────────────────
def fetch_all_properties():
    properties = []
    page = 1
    while True:
        print(f"  Fetching properties page {page}...", end=" ", flush=True)
        time.sleep(DELAY_SECONDS)
        try:
            resp  = api_get("/api/properties", {"page": page})
            items = to_list(resp)
        except Exception as e:
            print(f"FAILED: {e}")
            break

        if not items:
            print("done.")
            break

        properties.extend(items)
        print(f"{len(items)} fetched (total: {len(properties)})")

        if len(items) < 20:
            break
        page += 1

    return properties

# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("BasePMS → Google Sheets Sync")
    print("=" * 60)

    # ── Determine tab names ───────────────────────────────────
    today = datetime.now().strftime("%Y%m%d")
    if FORCE_PUSH:
        main_tab  = "BasePMS"
        image_tab = "BasePMS Images"
        print(f"\nMode: FORCE PUSH → tabs: '{main_tab}', '{image_tab}'")
    else:
        main_tab  = today
        image_tab = f"{today}_images"
        print(f"\nMode: SCHEDULED → tabs: '{main_tab}', '{image_tab}'")

    # ── Pre-load existing GitHub images ──────────────────────
    print("\nChecking existing images in GitHub repo...")
    existing_filenames = get_existing_github_images()
    print(f"  {len(existing_filenames)} images already uploaded")

    # ── Connect to Google Sheets ──────────────────────────────
    print("\nConnecting to Google Sheets...")
    client      = get_gspread_client()
    spreadsheet = client.open_by_key(SHEET_ID)
    main_sheet  = get_or_create_tab(spreadsheet, main_tab)
    img_sheet   = get_or_create_tab(spreadsheet, image_tab)

    # ── Write headers ─────────────────────────────────────────
    main_sheet.append_row(MAIN_HEADERS)
    img_sheet.append_row(IMAGE_HEADERS)

    # ── Fetch all properties ──────────────────────────────────
    print("\nFetching all properties...")
    all_properties = fetch_all_properties()
    print(f"\nTotal: {len(all_properties)} properties\n")

    scraped_at  = datetime.now().strftime("%d/%m/%Y %H:%M")
    main_rows   = []
    image_rows  = []
    total_main  = 0
    total_imgs  = 0

    # ── Process each property ─────────────────────────────────
    for i, prop in enumerate(all_properties):
        brand = get_brand(prop.get("email", ""))
        seen_room_types = set()

        for ay in ACADEMIC_YEARS:
            time.sleep(DELAY_SECONDS)
            try:
                room_types = api_get(
                    f"/api/properties/{prop['id']}/room_types",
                    {"academicYear": ay}
                )
            except Exception as e:
                print(f"  ⚠  {prop['name']} ({ay}): {e}")
                continue

            rt_list = to_list(room_types)

            for rt in rt_list:
                # ── Resolve thumbnail → public GitHub URL ─────
                raw_thumbnail = rt.get("thumbnail") or prop.get("thumbnail") or ""
                if raw_thumbnail and raw_thumbnail.startswith("https://hfs.api.basepms.com"):
                    public_thumbnail = upload_image_to_github(raw_thumbnail, existing_filenames)
                else:
                    public_thumbnail = raw_thumbnail

                # ── Main data rows (instalments) ──────────────
                for inst in to_list(rt.get("instalments", [])):
                    pricing = inst.get("pricing") or {}
                    main_rows.append([
                        brand,
                        prop.get("name", ""),
                        prop.get("city_name", ""),
                        rt.get("name", ""),
                        inst.get("academic_year", ay),
                        inst.get("contract_length", ""),
                        pricing.get("price", ""),
                        pricing.get("price_formatted", ""),
                        str(pricing.get("available", "")),
                        public_thumbnail,
                        inst.get("name", ""),
                        inst.get("start_date", ""),
                        inst.get("end_date", ""),
                        inst.get("base_hub_url", ""),
                        scraped_at
                    ])

                # ── Image rows (once per room type, not per AY) ──
                rt_key = (prop["id"], rt.get("id"))
                if rt_key not in seen_room_types:
                    seen_room_types.add(rt_key)
                    for img in to_list(rt.get("images", [])):
                        image_rows.append([
                            brand,
                            prop.get("name", ""),
                            prop.get("city_name", ""),
                            rt.get("name", ""),
                            img.get("id", ""),
                            img.get("name", ""),
                            img.get("url", "")
                        ])

        pct = round((i + 1) / len(all_properties) * 100)
        print(f"  [{pct:3d}%] {i+1}/{len(all_properties)} {prop['name']}")

        # ── Flush to Sheets every 50 properties ───────────────
        if len(main_rows) >= 50 or (i == len(all_properties) - 1 and main_rows):
            main_sheet.append_rows(main_rows, value_input_option="USER_ENTERED")
            total_main += len(main_rows)
            main_rows = []

        if len(image_rows) >= 50 or (i == len(all_properties) - 1 and image_rows):
            img_sheet.append_rows(image_rows, value_input_option="USER_ENTERED")
            total_imgs += len(image_rows)
            image_rows = []

    print("\n" + "=" * 60)
    print(f"COMPLETE")
    print(f"  ✓ {total_main} rows → '{main_tab}'")
    print(f"  ✓ {total_imgs} image rows → '{image_tab}'")
    print(f"  ✓ {len(_uploaded_this_run)} new images uploaded to GitHub")
    print("=" * 60)

if __name__ == "__main__":
    main()
