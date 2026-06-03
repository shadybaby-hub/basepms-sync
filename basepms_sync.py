import requests
import json
import os
import io
import csv
import re
import glob
import time
import base64
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ───────────────────────────────────────────────────
API_ROOT        = "https://hfs.api.basepms.com"
API_TOKEN       = os.environ.get("BASEPMS_API_TOKEN", "")
SHEET_ID        = os.environ.get("SHEET_ID", "")
SHEET_ID_2      = os.environ.get("SHEET_ID_2", "1SAoG7O64TOXjYMfzeyLwnPMbiVx7ge2Yio59aUh3iw0")
ROLLING_DAYS    = 30   # how many days of changes to keep in the secondary sheet
RUN_MODE        = os.environ.get("RUN_MODE", "sync")   # "sync" or "friday"
GITHUB_TOKEN    = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO     = "shadybaby-hub/basepms-sync"
GITHUB_BRANCH   = "main"
IMAGES_FOLDER   = "images"
DATA_FOLDER     = "data"
SNAPSHOT_FOLDER = "data/snapshots"
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

# Header row background for the secondary rolling tabs — #F5A04C
HEADER_BG = {"red": 245 / 255, "green": 160 / 255, "blue": 76 / 255}

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

# ── GITHUB REPO ACCESS ────────────────────────────────────────
def get_repo_file_shas():
    """Return {path: blob_sha} for every file in the repo via the Git Trees API."""
    if not GITHUB_TOKEN:
        return {}
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    ref_url = f"https://api.github.com/repos/{GITHUB_REPO}/git/ref/heads/{GITHUB_BRANCH}"
    ref_resp = requests.get(ref_url, headers=headers)
    if not ref_resp.ok:
        return {}
    tree_sha = ref_resp.json()["object"]["sha"]

    tree_url = f"https://api.github.com/repos/{GITHUB_REPO}/git/trees/{tree_sha}?recursive=1"
    tree_resp = requests.get(tree_url, headers=headers)
    if not tree_resp.ok:
        return {}

    files = tree_resp.json().get("tree", [])
    return {item["path"]: item["sha"] for item in files if item.get("type") == "blob"}

def github_put_file(repo_path, content, message, file_shas=None):
    """Create or update a file in the repo via the Contents API.

    content may be str (encoded as UTF-8) or bytes. Pass file_shas (path→sha
    map from get_repo_file_shas) so existing files are updated rather than
    rejected with a 409.
    """
    if not GITHUB_TOKEN:
        print(f"    ⚠  No GITHUB_TOKEN — cannot write {repo_path}")
        return False

    if isinstance(content, str):
        content = content.encode("utf-8")

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{repo_path}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    payload = {
        "message": message,
        "content": base64.b64encode(content).decode("utf-8"),
        "branch": GITHUB_BRANCH
    }

    sha = (file_shas or {}).get(repo_path)
    if sha:
        payload["sha"] = sha

    resp = requests.put(url, json=payload, headers=headers, timeout=60)
    if resp.status_code in (200, 201):
        print(f"  ✓ Wrote {repo_path}")
        return True
    print(f"  ⚠  GitHub write failed ({repo_path}): {resp.status_code} {resp.text[:200]}")
    return False

def rows_to_csv(rows):
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerows(rows)
    return buf.getvalue()

# ── IMAGE RE-HOSTING ──────────────────────────────────────────
_uploaded_this_run = set()

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

    ok = github_put_file(
        f"{IMAGES_FOLDER}/{filename}",
        image_data,
        f"Add image {filename}"
    )
    if ok:
        _uploaded_this_run.add(filename)
        return (
            f"https://raw.githubusercontent.com/{GITHUB_REPO}/"
            f"{GITHUB_BRANCH}/{IMAGES_FOLDER}/{filename}"
        )
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

def get_tab_keep(spreadsheet, tab_name, cols):
    """Get a tab, or create it (without clearing existing contents)."""
    try:
        return spreadsheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=tab_name, rows=2000, cols=cols)

ACRONYMS = {"url": "URL", "id": "ID"}

def prettify_header(cols):
    """'image_url' -> 'Image URL' : underscores to spaces, title-case each word,
    with known acronyms fully capitalised."""
    def word(w):
        return ACRONYMS.get(w.lower(), w.capitalize())
    return [" ".join(word(w) for w in c.split("_")) for c in cols]

def _col_letter(n):
    """1 -> 'A', 16 -> 'P', etc."""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def style_header_row(ws, ncols):
    """Freeze row 1 and give it the HEADER_BG background with bold white text."""
    ws.format(f"A1:{_col_letter(ncols)}1", {
        "backgroundColor": HEADER_BG,
        "textFormat": {
            "bold": True,
            "foregroundColor": {"red": 1, "green": 1, "blue": 1}
        }
    })
    ws.freeze(rows=1)

def push_rolling_subset(spreadsheet, tab_name, header, dated_new_rows, today_iso):
    """Maintain a single rolling tab: newest rows on top, keep ROLLING_DAYS days.

    `header` includes the leading "date" column. `dated_new_rows` are today's
    rows already prefixed with today's date. Rows from today are replaced (so
    re-running the same day doesn't duplicate), and rows older than the cutoff
    are dropped. Date strings are ISO (YYYY-MM-DD) so lexical compare == date
    compare.
    """
    ws       = get_tab_keep(spreadsheet, tab_name, len(header))
    existing = ws.get_all_values()

    if existing and existing[0][:len(header)] == header:
        old_rows = existing[1:]
    else:
        old_rows = existing

    cutoff = (datetime.now() - timedelta(days=ROLLING_DAYS)).strftime("%Y-%m-%d")
    kept = [
        r for r in old_rows
        if r and r[0] and r[0] != today_iso and r[0] >= cutoff
    ]

    final = [header] + dated_new_rows + kept
    ws.clear()
    ws.resize(rows=max(len(final), 1), cols=len(header))
    ws.update(final, value_input_option="USER_ENTERED")
    style_header_row(ws, len(header))
    print(f"  ✓ {len(dated_new_rows)} new + {len(kept)} kept → '{tab_name}' (rolling {ROLLING_DAYS}d)")

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

# ── COLLECT DATA ──────────────────────────────────────────────
def collect_data(existing_image_filenames):
    """Fetch all properties/room-types/pricing/images from BasePMS.

    Returns (main_rows, image_rows), each a list of lists with the header row
    at index 0 — ready to be serialised as CSV.
    """
    print("\nFetching all properties...")
    all_properties = fetch_all_properties()
    print(f"\nTotal: {len(all_properties)} properties\n")

    scraped_at = datetime.now().strftime("%d/%m/%Y %H:%M")
    main_rows  = [MAIN_HEADERS]
    image_rows = [IMAGE_HEADERS]

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
                    public_thumbnail = upload_image_to_github(raw_thumbnail, existing_image_filenames)
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

    print(f"\n  ✓ {len(main_rows)-1} data rows collected")
    print(f"  ✓ {len(image_rows)-1} image rows collected")
    print(f"  ✓ {len(_uploaded_this_run)} new images uploaded to GitHub")
    return main_rows, image_rows

# ── CSV OUTPUT ────────────────────────────────────────────────
def write_latest_csv(main_rows, image_rows, file_shas):
    print("\nWriting latest CSVs to GitHub...")
    github_put_file(f"{DATA_FOLDER}/basepms_latest.csv",
                    rows_to_csv(main_rows),
                    "Update basepms_latest.csv", file_shas)
    github_put_file(f"{DATA_FOLDER}/basepms_images_latest.csv",
                    rows_to_csv(image_rows),
                    "Update basepms_images_latest.csv", file_shas)

def write_snapshot_csv(main_rows, image_rows, today, file_shas):
    print("\nWriting dated snapshot CSVs to GitHub...")
    github_put_file(f"{SNAPSHOT_FOLDER}/basepms_{today}.csv",
                    rows_to_csv(main_rows),
                    f"Snapshot basepms {today}", file_shas)
    github_put_file(f"{SNAPSHOT_FOLDER}/basepms_images_{today}.csv",
                    rows_to_csv(image_rows),
                    f"Snapshot basepms images {today}", file_shas)

# ── COMPARE ───────────────────────────────────────────────────
def read_csv_file(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return [row for row in csv.reader(f)]

def normalize_rows(rows):
    """Round-trip in-memory rows through CSV so values are strings that match
    exactly what is written to disk (the previous snapshot is read back as
    strings, so the current data must be compared in the same form)."""
    return [row for row in csv.reader(io.StringIO(rows_to_csv(rows)))]

def find_previous_snapshot_date(today):
    """Most recent dated main snapshot in the repo checkout, excluding today's."""
    dates = []
    for path in glob.glob(os.path.join(SNAPSHOT_FOLDER, "basepms_*.csv")):
        name = os.path.basename(path)
        if name.startswith("basepms_images_"):
            continue
        m = re.match(r"basepms_(\d{8})\.csv$", name)
        if m and m.group(1) != today:
            dates.append(m.group(1))
    return max(dates) if dates else None

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

def run_compare(spreadsheet, today, curr_main_rows, curr_image_rows, spreadsheet2=None):
    prev_date = find_previous_snapshot_date(today)
    if not prev_date:
        print("  ⚠  No previous snapshot found — skipping comparison (first run?)")
        return

    prev_main_rows  = read_csv_file(os.path.join(SNAPSHOT_FOLDER, f"basepms_{prev_date}.csv"))
    prev_image_rows = read_csv_file(os.path.join(SNAPSHOT_FOLDER, f"basepms_images_{prev_date}.csv"))

    # Match the on-disk string form so typed values don't read as spurious changes
    curr_main_rows  = normalize_rows(curr_main_rows)
    curr_image_rows = normalize_rows(curr_image_rows)

    print(f"\n  Comparing snapshot {today} vs {prev_date}")

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

    curr_imgs = img_set(curr_image_rows)
    prev_imgs = img_set(prev_image_rows)

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

    # ── Secondary sheet: rolling 30-day log of changes only ───────
    # 'Rooms'       = comparison rows where change_flag (col O) != NO CHANGE
    # 'Room Images' = image rows where status (col F) != NO CHANGE
    if spreadsheet2 is not None:
        today_iso  = datetime.now().strftime("%Y-%m-%d")
        rooms_new  = [[today_iso] + r for r in comp_rows[1:]     if r[14] != "NO CHANGE"]
        images_new = [[today_iso] + r for r in img_comp_rows[1:] if r[5]  != "NO CHANGE"]
        try:
            push_rolling_subset(spreadsheet2, "Rooms",
                                prettify_header(["date"] + COMPARISON_HEADERS), rooms_new, today_iso)
            push_rolling_subset(spreadsheet2, "Room Images",
                                prettify_header(["date"] + COMPARISON_IMAGE_HEADERS), images_new, today_iso)
        except Exception as e:
            print(f"  ⚠  Secondary sheet push failed (is it shared with the "
                  f"service account?): {e}")

# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 60)
    today = datetime.now().strftime("%Y%m%d")

    print("\nReading existing repo file list from GitHub...")
    file_shas = get_repo_file_shas()
    existing_image_filenames = {
        p.split("/")[-1] for p in file_shas if p.startswith(f"{IMAGES_FOLDER}/")
    }
    print(f"  {len(existing_image_filenames)} images already uploaded")

    if RUN_MODE == "friday":
        print("BasePMS Friday Sync & Compare")
        print("=" * 60)

        print("\nConnecting to Google Sheets...")
        client      = get_gspread_client()
        spreadsheet = client.open_by_key(SHEET_ID)

        spreadsheet2 = None
        if SHEET_ID_2:
            try:
                spreadsheet2 = client.open_by_key(SHEET_ID_2)
                print(f"  Secondary sheet connected: {spreadsheet2.title}")
            except Exception as e:
                print(f"  ⚠  Could not open secondary sheet {SHEET_ID_2}: {e}")

        # Step 1 — Fetch fresh data
        main_rows, image_rows = collect_data(existing_image_filenames)

        # Step 2 — Persist full data to GitHub as CSV (latest + dated snapshot)
        write_latest_csv(main_rows, image_rows, file_shas)
        write_snapshot_csv(main_rows, image_rows, today, file_shas)

        # Step 3 — Compare this snapshot against the previous one → Google Sheets
        #          (and push the changes-only rolling log to the secondary sheet)
        print("\nRunning comparison...")
        run_compare(spreadsheet, today, main_rows, image_rows, spreadsheet2)

    else:
        print("BasePMS → GitHub CSV Sync")
        print("=" * 60)
        print("\nMode: SYNC → data/basepms_latest.csv, data/basepms_images_latest.csv")

        main_rows, image_rows = collect_data(existing_image_filenames)
        write_latest_csv(main_rows, image_rows, file_shas)

    print("\n" + "=" * 60)
    print("COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()
