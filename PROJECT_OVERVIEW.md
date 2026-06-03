# basepms-sync — Project Overview

> **Reminder:** Update this file whenever you make changes to the project. Ask Claude to re-explain and rewrite it if needed.

---

## What This Project Does

Pulls all student accommodation listings (properties, room types, pricing) from the **BasePMS API** and writes them into a **Google Sheet**. Every Friday it also archives the previous week's data and generates a **week-over-week comparison report** highlighting what changed.

Think of it as an automated weekly price audit pipeline for student accommodation brands.

---

## The One File That Matters

```
basepms_sync.py      — the entire sync pipeline, ~610 lines
```

Everything else is supporting infrastructure:
- `.github/workflows/` — GitHub Actions that run the script on a schedule
- `images/` — re-hosted property images committed to this repo

---

## Two Run Modes

The script behaviour is controlled by the `RUN_MODE` environment variable.

### Mode 1: `sync` (runs Mon–Fri at 23:00 UTC)

Triggered by `.github/workflows/sync.yml`.

1. Fetches all properties from BasePMS (paginated)
2. For each property, fetches room types + pricing for **2025/2026** and **2026/2027**
3. Re-hosts any BasePMS-origin images to this GitHub repo (`images/` folder) so they don't expire
4. Writes everything to two Google Sheet tabs:
   - `BasePMS` — one row per room type instalment (pricing, dates, thumbnail, etc.)
   - `BasePMS Images` — one row per image per room type

### Mode 2: `friday` (runs Mon–Fri at 07:42 BST / 06:42 UTC)

Triggered by `.github/workflows/friday_sync.yml`. Does everything in Mode 1, **plus**:

1. **Archives** the current `BasePMS_Friday` and `BasePMS_Friday_Images` tabs by copying them to dated tabs (e.g. `BasePMS_Friday_20250530`)
2. **Prunes** old archives — keeps only the last 10 Fridays
3. **Runs a fresh sync** into `BasePMS_Friday` and `BasePMS_Friday_Images`
4. **Generates a comparison report** (see below)

---

## The Comparison Report

Produced by `run_compare()`. Written into two new Google Sheet tabs named after today's date:

### `Comparison_YYYYMMDD`

Every room type from both this week and last week appears as a row. The match key is:
`brand + property_name + city + room_type + academic_year + duration_weeks`

Each row has columns for previous and current values of price, start date, end date, and thumbnail, plus a **change_flag**:

| Flag | Meaning |
|---|---|
| `NO CHANGE` | Nothing changed |
| `PRICE CHANGED` | Price is different |
| `DATE CHANGED` | Start or end date is different |
| `IMAGE CHANGED` | Thumbnail URL is different |
| `MULTIPLE CHANGES` | More than one of the above |
| `NEW` | Listing only exists this week |
| `REMOVED` | Listing existed last week, gone now |

Changed cells are **highlighted yellow** in the sheet.

### `Comparison_YYYYMMDD_images`

Image-level diff. Each row is a `(property, room_type, image_url)` combination with a status of `ADDED`, `REMOVED`, or `NO CHANGE`.

---

## Google Sheet Tab Structure

| Tab name | Written by | Contents |
|---|---|---|
| `BasePMS` | Daily sync | Current live data — all properties/rooms/pricing |
| `BasePMS Images` | Daily sync | All image URLs per room type |
| `BasePMS_Friday` | Friday sync | This Friday's snapshot (gets overwritten weekly) |
| `BasePMS_Friday_Images` | Friday sync | This Friday's images snapshot |
| `BasePMS_Friday_YYYYMMDD` | Friday sync | Archived copy of previous Friday's main data |
| `BasePMS_Friday_Images_YYYYMMDD` | Friday sync | Archived copy of previous Friday's images |
| `Comparison_YYYYMMDD` | Friday sync | Week-over-week pricing/date/image diff |
| `Comparison_YYYYMMDD_images` | Friday sync | Week-over-week image diff |

---

## Image Re-Hosting

BasePMS image URLs are behind their own auth-gated API (`https://hfs.api.basepms.com/...`). If you put those URLs directly in a Google Sheet, the images will be broken for anyone without a BasePMS token.

To fix this, `upload_image_to_github()` downloads each image using the API token and re-uploads it to the `images/` folder in **this GitHub repo** as a public `raw.githubusercontent.com` URL. The script skips images already uploaded (checks via the GitHub Git Trees API).

---

## Brand Mapping

BasePMS properties have an `email` field (the owner's email). The domain maps to a brand name via `BRAND_LOOKUP` in the config section:

```
wearehomesforstudents.com  → Homes for Students
prestigestudentliving.com  → Prestige Student Living
urbanstudentlife.com       → Urban Student Life
... etc.
```

If the domain isn't in the lookup, `brand` is left blank.

---

## Environment Variables / Secrets

Set as GitHub Actions secrets. All required.

| Variable | What it is |
|---|---|
| `BASEPMS_API_TOKEN` | Bearer token for the BasePMS API |
| `GOOGLE_CREDENTIALS` | Full JSON of a Google service account (with Sheets + Drive access) |
| `SHEET_ID` | The Google Sheet ID (from the URL) |
| `GITHUB_TOKEN` | Auto-provided by GitHub Actions — used to commit images to this repo |

`FORCE_PUSH` and `RUN_MODE` are set directly in the workflow YAML, not as secrets.

---

## Key Functions Quick Reference

| Function | What it does |
|---|---|
| `main()` | Entry point — checks `RUN_MODE` and branches to sync or friday path |
| `run_sync(spreadsheet, main_tab, image_tab)` | Fetches all data and writes to two sheet tabs |
| `run_compare(spreadsheet, today)` | Builds the week-over-week diff and writes comparison tabs |
| `fetch_all_properties()` | Paginates the `/api/properties` endpoint |
| `upload_image_to_github(url, existing)` | Downloads a BasePMS image and commits it to this repo |
| `get_existing_github_images()` | Gets all already-uploaded image filenames via Git Trees API |
| `copy_tab(spreadsheet, src, dest)` | Copies one sheet tab to another (used for archiving) |
| `prune_old_archive_tabs(spreadsheet, prefix, max)` | Deletes oldest archive tabs beyond the keep limit |
| `rows_to_dict(rows, key_cols)` | Turns sheet rows into a dict keyed by a composite key (used in comparison) |
| `get_brand(email)` | Maps a property's email domain to a brand name |
| `to_list(resp)` | Normalises various BasePMS API response shapes into a plain list |

---

## Rate Limiting

The script sleeps `1.1 seconds` between every API call (`DELAY_SECONDS = 1.1`). This is intentional — BasePMS enforces a rate limit. Do not remove it.

Rows are flushed to the sheet in batches of 50 to avoid Google Sheets API quota issues.

---

## How to Run Manually

Via GitHub UI: go to **Actions → BasePMS Sync → Run workflow** (or the Friday workflow).

Locally (requires all env vars set):
```bash
pip install -r requirements.txt
RUN_MODE=sync python basepms_sync.py
RUN_MODE=friday python basepms_sync.py
```

## Corresponding Google Sheet: https://docs.google.com/spreadsheets/d/14Qx-9nZlACYfQoJF7EDtZBPib7LfaoeQeixMjEGpxVY/edit?gid=973262602#gid=973262602