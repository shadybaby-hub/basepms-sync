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
RUN_MODE        = os.environ.get("RUN_MODE", "sync")   # "sync" or "friday"
GITHUB_TOKEN    = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO     = "shadybaby-hub/basepms-sync"
GITHUB_BRANCH   = "main"
IMAGES_FOLDER   = "images"
DELAY_SECONDS   = 1.1
MAX_ARCHIVE_TABS = 10

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

COMPARISON_HEADERS = [
    "brand", "property_name", "city", "room_type", "academic_year", "duration_weeks",
    "price_previous", "price_current",
    "start_date_previous", "start_date_current",
    "end_date_previous", "end_date_current",
    "thumbnail_previous", "thumbnail_current",
    "change_flag"
]

COMPARISON_IMAGE_HEADERS = [
    "brand", "property_name", "city", "room_type",
    "image_url", "status"
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

# Yellow background for changed cells
YELLOW = {"red": 1.0, "green": 0.95, "blue": 0.0}

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
_uploaded_this_run = set()

def get_existing_github_images():
    """Fetch all uploaded image filenames using the Git Trees API."""
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    ref_url = f"https://api.github.com/repos/{GITHUB_REPO}/git/ref/heads/{GITHUB_BRANCH}"
    ref_resp = requests.get(ref_url, headers=headers)
    if not ref_resp.ok:
        return set()
    tree_sha = ref_resp.json()["object"]["sha"]

    tree_url = f"https://api.github.com/repos/{GITHUB_REPO}/git/trees/{tree_sha}?recursive=1"
    tree_resp = requests.get(tree_url, headers=headers)
    if not tree_resp.ok:
        return set()

    files = tree_resp.json().get("tree", [])
    return {
        item["path"].split("/")[-1]
        for item in files
        if item["path"].startswith(f"{IMAGES_FOLDER}/")
    }

def upload_image_to_github(image_url, existing_filenames):
    if not GITHUB_TOKEN:
        return image_url

    filename = image_url.split("/")[-1].split("?")[0]
    if not filename:
        return image_url

    if filename in _uploaded_this_run or filename in existing_filenames:
        return (
            f"https://raw.githubusercontent.com/{GITHUB_REPO}/"
            f"{GITHUB_BRANCH}/{IMAGES_FOLDER}/{filename}"
        )

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
            return (
                f"https://raw.githubusercontent.com/{GITHUB_REPO}/"
                f"{GITHUB_BRANCH}/{IMAGES_FOLDER}/{filename}"
            )
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
    scopes = [
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

def copy_tab(spreadsheet, source_name, dest_name):
    """Copy source tab to dest_name. Overwrites dest if it exists."""
    try:
        source = spreadsheet.worksheet(source_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  ⚠  Source tab '{source_name}' not found — skipping archive")
        return False

    data = source.get_all_values()
    if not data:
        print(f"  ⚠  Source tab '{source_name}' is empty — skipping archive")
        return False

    try:
        dest = spreadsheet.worksheet(dest_name)
        dest.clear()
    except gspread.exceptions.WorksheetNotFound:
        dest = spreadsheet.add_worksheet(title=dest_name, rows=len(data) + 100, cols=20)

    dest.update(data, value_input_option="USER_ENTERED")
    print(f"  Archived '{source_name}' → '{dest_name}' ({len(data)-1} rows)")
    return True

def prune_old_archive_tabs(spreadsheet, prefix, max_tabs):
    """Delete oldest archive tabs beyond max_tabs for a given prefix."""
    all_titles = [ws.title for ws in spreadsheet.worksheets()]
    archive_tabs = sorted([
        t for t in all_titles
        if t.startswith(prefix + "_") and t[len(prefix)+1:].isdigit()
    ])
    while len(archive_tabs) > max_tabs:
        oldest = archive_tabs.pop(0)
        spreadsheet.del_worksheet(spreadsheet.worksheet(oldest))
        print(f"  Pruned old archive tab: {oldest}")

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

# ── SYNC ─────────────────────────────────────────────────────
def run_sync(spreadsheet, main_tab, image_tab):
    print("\nChecking existing images in GitHub repo...")
    existing_filenames = get_existing_github_images()
    print(f"  {len(existing_filenames)} images already uploaded")

    main_sheet = get_or_create_tab(spreadsheet, main_tab)
    img_sheet  = get_or_create_tab(spreadsheet, image_tab)

    main_sheet.append_row(MAIN_HEADERS)
    img_sheet.append_row(IMAGE_HEADERS)

    print("\nFetching all properties...")
    all_properties = fetch_all_properties()
    print(f"\nTotal: {len(all_properties)} properties\n")

    scraped_at = datetime.now().strftime("%d/%m/%Y %H:%M")
    main_rows  = []
    image_rows = []
    total_main = 0
    total_imgs = 0

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
                raw_thumbnail = rt.get("thumbnail") or prop.get("thumbnail") or ""
                if raw_thumbnail and raw_thumbnail.startswith("https://hfs.api.basepms.com"):
                    public_thumbnail = upload_image_to_github(raw_thumbnail, existing_filenames)
                else:
                    public_thumbnail = raw_thumbnail

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

        if len(main_rows) >= 50 or (i == len(all_properties) - 1 and main_rows):
            main_sheet.append_rows(main_rows, value_input_option="USER_ENTERED")
            total_main += len(main_rows)
            main_rows = []

        if len(image_rows) >= 50 or (i == len(all_properties) - 1 and image_rows):
            img_sheet.append_rows(image_rows, value_input_option="USER_ENTERED")
            total_imgs += len(image_rows)
            image_rows = []

    print(f"\n  ✓ {total_main} rows → '{main_tab}'")
    print(f"  ✓ {total_imgs} image rows → '{image_tab}'")
    print(f"  ✓ {len(_uploaded_this_run)} new images uploaded to GitHub")

# ── COMPARE ───────────────────────────────────────────────────
def rows_to_dict(rows, key_cols):
    """Convert list of rows (with header) into dict keyed by tuple of key_cols."""
    if not rows:
        return {}
    headers = rows[0]
    result  = {}
    for row in rows[1:]:
        row    = list(row) + [""] * (len(headers) - len(row))
        record = dict(zip(headers, row))
        key    = tuple(record.get(c, "") for c in key_cols)
        result[key] = record
    return result

def run_compare(spreadsheet, today):
    FRIDAY_MAIN = "BasePMS_Friday"
    FRIDAY_IMGS = "BasePMS_Friday_Images"

    # Find most recent archive tab
    all_titles = [ws.title for ws in spreadsheet.worksheets()]
    archive_tabs = sorted([
        t for t in all_titles
        if t.startswith("BasePMS_Friday_") and t[len("BasePMS_Friday_"):].isdigit()
    ])

    if not archive_tabs:
        print("  ⚠  No archive tab found — skipping comparison (first Friday run?)")
        return

    prev_main_tab = archive_tabs[-1]
    prev_imgs_tab = "BasePMS_Friday_Images_" + prev_main_tab[len("BasePMS_Friday_"):]

    print(f"\n  Comparing '{FRIDAY_MAIN}' vs '{prev_main_tab}'")

    try:
        curr_main_rows = spreadsheet.worksheet(FRIDAY_MAIN).get_all_values()
        prev_main_rows = spreadsheet.worksheet(prev_main_tab).get_all_values()
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"  ⚠  Could not load tabs for comparison: {e}")
        return

    MATCH_KEY = ["brand", "property_name", "city", "room_type", "academic_year", "duration_weeks"]

    curr_dict = rows_to_dict(curr_main_rows, MATCH_KEY)
    prev_dict = rows_to_dict(prev_main_rows, MATCH_KEY)
    all_keys  = sorted(set(list(curr_dict.keys()) + list(prev_dict.keys())))

    comp_rows    = [COMPARISON_HEADERS]
    yellow_cells = []

    for row_idx, key in enumerate(all_keys):
        curr = curr_dict.get(key)
        prev = prev_dict.get(key)

        if curr and not prev:
            flag = "NEW"
            row = [
                curr.get("brand",""), curr.get("property_name",""),
                curr.get("city",""), curr.get("room_type",""),
                curr.get("academic_year",""), curr.get("duration_weeks",""),
                "", curr.get("price_per_week",""),
                "", curr.get("start_date",""),
                "", curr.get("end_date",""),
                "", curr.get("thumbnail",""),
                flag
            ]
        elif prev and not curr:
            flag = "REMOVED"
            row = [
                prev.get("brand",""), prev.get("property_name",""),
                prev.get("city",""), prev.get("room_type",""),
                prev.get("academic_year",""), prev.get("duration_weeks",""),
                prev.get("price_per_week",""), "",
                prev.get("start_date",""), "",
                prev.get("end_date",""), "",
                prev.get("thumbnail",""), "",
                flag
            ]
        else:
            p_price = prev.get("price_per_week","")
            c_price = curr.get("price_per_week","")
            p_start = prev.get("start_date","")
            c_start = curr.get("start_date","")
            p_end   = prev.get("end_date","")
            c_end   = curr.get("end_date","")
            p_thumb = prev.get("thumbnail","")
            c_thumb = curr.get("thumbnail","")

            changes = []
            if p_price != c_price:
                changes.append("PRICE CHANGED")
            if p_start != c_start or p_end != c_end:
                changes.append("DATE CHANGED")
            if p_thumb != c_thumb:
                changes.append("IMAGE CHANGED")

            if not changes:
                flag = "NO CHANGE"
            elif len(changes) == 1:
                flag = changes[0]
            else:
                flag = "MULTIPLE CHANGES"

            row = [
                prev.get("brand",""), prev.get("property_name",""),
                prev.get("city",""), prev.get("room_type",""),
                prev.get("academic_year",""), prev.get("duration_weeks",""),
                p_price, c_price,
                p_start, c_start,
                p_end, c_end,
                p_thumb, c_thumb,
                flag
            ]

            # Cols: price=6,7 | start_date=8,9 | end_date=10,11 | thumbnail=12,13
            if p_price != c_price:
                yellow_cells += [(row_idx, 6), (row_idx, 7)]
            if p_start != c_start:
                yellow_cells += [(row_idx, 8), (row_idx, 9)]
            if p_end != c_end:
                yellow_cells += [(row_idx, 10), (row_idx, 11)]
            if p_thumb != c_thumb:
                yellow_cells += [(row_idx, 12), (row_idx, 13)]

        comp_rows.append(row)

    # Write comparison tab
    comp_tab_name = f"Comparison_{today}"
    comp_sheet    = get_or_create_tab(spreadsheet, comp_tab_name)
    comp_sheet.update(comp_rows, value_input_option="USER_ENTERED")
    print(f"  ✓ {len(comp_rows)-1} rows → '{comp_tab_name}'")

    # Apply yellow highlights
    if yellow_cells:
        sheet_id      = comp_sheet.id
        requests_body = []
        for (r, c) in yellow_cells:
            requests_body.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex":    r + 1,
                        "endRowIndex":      r + 2,
                        "startColumnIndex": c,
                        "endColumnIndex":   c + 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": YELLOW
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })
        spreadsheet.batch_update({"requests": requests_body})
        print(f"  ✓ {len(yellow_cells)} cells highlighted yellow")

    # Image comparison
    try:
        curr_img_rows = spreadsheet.worksheet(FRIDAY_IMGS).get_all_values()
    except gspread.exceptions.WorksheetNotFound:
        curr_img_rows = []

    try:
        prev_img_rows = spreadsheet.worksheet(prev_imgs_tab).get_all_values()
    except gspread.exceptions.WorksheetNotFound:
        prev_img_rows = []

    IMG_KEY = ["brand", "property_name", "city", "room_type"]

    def img_set(rows):
        if not rows:
            return set()
        headers = rows[0]
        result  = set()
        for row in rows[1:]:
            row    = list(row) + [""] * (len(headers) - len(row))
            record = dict(zip(headers, row))
            key    = tuple(record.get(c, "") for c in IMG_KEY)
            url    = record.get("image_url", "")
            result.add((key, url))
        return result

    curr_imgs = img_set(curr_img_rows)
    prev_imgs = img_set(prev_img_rows)

    added   = curr_imgs - prev_imgs
    removed = prev_imgs - curr_imgs
    kept    = curr_imgs & prev_imgs

    img_comp_rows = [COMPARISON_IMAGE_HEADERS]
    for (key, url) in sorted(added):
        img_comp_rows.append(list(key) + [url, "ADDED"])
    for (key, url) in sorted(removed):
        img_comp_rows.append(list(key) + [url, "REMOVED"])
    for (key, url) in sorted(kept):
        img_comp_rows.append(list(key) + [url, "NO CHANGE"])

    img_comp_tab   = f"Comparison_{today}_images"
    img_comp_sheet = get_or_create_tab(spreadsheet, img_comp_tab)
    img_comp_sheet.update(img_comp_rows, value_input_option="USER_ENTERED")
    print(f"  ✓ {len(img_comp_rows)-1} rows → '{img_comp_tab}'")

# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 60)
    today = datetime.now().strftime("%Y%m%d")

    print("\nConnecting to Google Sheets...")
    client      = get_gspread_client()
    spreadsheet = client.open_by_key(SHEET_ID)

    if RUN_MODE == "friday":
        print("BasePMS Friday Sync & Compare")
        print("=" * 60)

        FRIDAY_MAIN = "BasePMS_Friday"
        FRIDAY_IMGS = "BasePMS_Friday_Images"

        # Step 1 — Archive before overwriting
        print("\nArchiving current Friday data...")
        copy_tab(spreadsheet, FRIDAY_MAIN, f"BasePMS_Friday_{today}")
        copy_tab(spreadsheet, FRIDAY_IMGS, f"BasePMS_Friday_Images_{today}")

        # Step 2 — Prune oldest archives
        prune_old_archive_tabs(spreadsheet, "BasePMS_Friday", MAX_ARCHIVE_TABS)
        prune_old_archive_tabs(spreadsheet, "BasePMS_Friday_Images", MAX_ARCHIVE_TABS)

        # Step 3 — Fresh sync
        print("\nFetching fresh Friday data...")
        run_sync(spreadsheet, FRIDAY_MAIN, FRIDAY_IMGS)

        # Step 4 — Compare
        print("\nRunning comparison...")
        run_compare(spreadsheet, today)

    else:
        print("BasePMS → Google Sheets Sync")
        print("=" * 60)
        print(f"\nMode: {'FORCE PUSH' if FORCE_PUSH else 'SCHEDULED'} → tabs: 'BasePMS', 'BasePMS Images'")
        run_sync(spreadsheet, "BasePMS", "BasePMS Images")

    print("\n" + "=" * 60)
    print("COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()
