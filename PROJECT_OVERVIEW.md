# basepms-sync — Project Overview

> **Reminder:** Update this file whenever you make changes to the project. Ask Claude to re-explain and rewrite it if needed.

---

## What This Project Does

Pulls all student accommodation listings (properties, room types, pricing) from the **BasePMS API**. The **full dataset is stored as CSV files in this GitHub repo** (under `data/`). Each run also keeps a dated snapshot, publishes the **full dataset to Google Sheets** (`BasePMS` + `BasePMS Images` tabs), and generates a **run-over-run comparison report** highlighting what changed.

Think of it as an automated price-audit pipeline: GitHub holds the raw data history, Google Sheets shows the current data plus the diff. Every published tab has the run date stamped in cell A1 (format `2026-06-05 11:37 UTC`); headers start on row 2.

---

## The One File That Matters

```
basepms_sync.py      — the entire sync pipeline
```

Everything else is supporting infrastructure:
- `.github/workflows/` — GitHub Actions that run the script on a schedule
- `data/` — full data exports as CSV (`*_latest.csv` + dated snapshots under `data/snapshots/`)
- `images/` — re-hosted property images committed to this repo

---

## The Run (Mon–Fri at 06:42 UTC)

There is a single pipeline, triggered by `.github/workflows/weekday_sync.yml` (scheduled weekdays; can also be run manually via `workflow_dispatch` on that workflow). Each run:

1. Fetches all properties from BasePMS (paginated)
2. For each property, fetches room types + pricing for **2025/2026** and **2026/2027**
3. Re-hosts any BasePMS-origin images to this GitHub repo (`images/` folder) so they don't expire
4. Writes the full dataset to two CSV files in this repo (via the GitHub Contents API):
   - `data/basepms_latest.csv` — one row per room type instalment (pricing, dates, thumbnail, etc.)
   - `data/basepms_images_latest.csv` — one row per image per room type
5. Writes a **dated snapshot** of the full dataset:
   - `data/snapshots/basepms_YYYYMMDD.csv`
   - `data/snapshots/basepms_images_YYYYMMDD.csv`
6. **Publishes the full dataset to Google Sheets**: main data → the `BasePMS` tab, images → the `BasePMS Images` tab
7. **Compares** today's snapshot against the most recent previous snapshot in the repo and **publishes the comparison report to Google Sheets** (see below)

Snapshots are **kept forever** — nothing is pruned. History lives in the repo / git log.

> The old standalone manual-sync workflow (`manual_sync.yml` / `RUN_MODE=sync`) and the per-row `scraped_at` CSV column have been removed — the run date in cell A1 of each tab replaces `scraped_at`.

---

## The Comparison Report (Google Sheets)

Produced by `run_compare()`. Written into two Google Sheet tabs named after today's date.

The "current" side is the data just fetched in this run; the "previous" side is read from the most recent dated snapshot CSV in `data/snapshots/` (excluding today's). On the very first run, with no prior snapshot, the comparison is skipped.

### `Comparison_YYYYMMDD`

Every room type from both this run and the previous snapshot appears as a row. The match key is:
`brand + property_name + city + room_type + academic_year + duration_weeks`

Each row has columns for previous and current values of price, start date, end date, and thumbnail, plus a **change_flag**:

| Flag | Meaning |
|---|---|
| `NO CHANGE` | Nothing changed |
| `PRICE CHANGED` | Price is different |
| `DATE CHANGED` | Start or end date is different |
| `IMAGE CHANGED` | Thumbnail URL is different |
| `MULTIPLE CHANGES` | More than one of the above |
| `NEW` | Listing only exists this run |
| `REMOVED` | Listing existed in the previous snapshot, gone now |

Changed cells are **highlighted yellow** in the sheet.

### `Comparison_YYYYMMDD_images`

Image-level diff. Each row is a `(property, room_type, image_url)` combination with a status of `ADDED`, `REMOVED`, or `NO CHANGE`.

---

## Where the Data Lives

| Location | Contents |
|---|---|
| `data/basepms_latest.csv` (GitHub) | Current live data — all properties/rooms/pricing |
| `data/basepms_images_latest.csv` (GitHub) | All image URLs per room type |
| `data/snapshots/basepms_YYYYMMDD.csv` (GitHub) | Dated snapshot of the full data (kept forever) |
| `data/snapshots/basepms_images_YYYYMMDD.csv` (GitHub) | Dated snapshot of the images |
| `BasePMS` (Sheet) | Full current dataset (same as `basepms_latest.csv`) |
| `BasePMS Images` (Sheet) | Full current image list (same as `basepms_images_latest.csv`) |
| `Comparison_YYYYMMDD` (Sheet) | Run-over-run pricing/date/image diff |
| `Comparison_YYYYMMDD_images` (Sheet) | Run-over-run image diff |

All four Sheet tabs are written every run, each stamped with the run date in cell A1.

> CSVs are written via the GitHub Contents API using `GITHUB_TOKEN`. The comparison reads the previous snapshot from the **checked-out copy** of the repo, so the `weekday_sync.yml` workflow must keep the `actions/checkout` step.

---

## Image Re-Hosting

BasePMS image URLs are behind their own auth-gated API (`https://hfs.api.basepms.com/...`). If you put those URLs directly in the data, the images will be broken for anyone without a BasePMS token.

To fix this, `upload_image_to_github()` downloads each image using the API token and re-uploads it to the `images/` folder in **this GitHub repo** as a public `raw.githubusercontent.com` URL. Already-uploaded images are skipped (checked via the repo file list fetched with the Git Trees API).

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

Set as GitHub Actions secrets.

| Variable | What it is |
|---|---|
| `BASEPMS_API_TOKEN` | Bearer token for the BasePMS API |
| `GITHUB_TOKEN` | Used to write CSVs and images to this repo via the Contents API (auto-provided by GitHub Actions; needs `contents: write`) |
| `GOOGLE_CREDENTIALS` | Full JSON of a Google service account (Sheets + Drive) — for publishing to the Sheet |
| `SHEET_ID` | The Google Sheet ID (from the URL) |

---

## Key Functions Quick Reference

| Function | What it does |
|---|---|
| `main()` | Entry point — runs the full pipeline (fetch → CSVs → snapshot → Sheets → comparison) |
| `publish_tab(spreadsheet, tab, rows, run_stamp)` | Writes rows to a Sheet tab with the run date in A1 (headers row 2) |
| `collect_data(existing_images)` | Fetches all data, returns `(main_rows, image_rows)` ready for CSV |
| `write_latest_csv(...)` | Writes `data/basepms_latest.csv` + images CSV to the repo |
| `write_snapshot_csv(..., today)` | Writes the dated snapshot CSVs to `data/snapshots/` |
| `run_compare(spreadsheet, today, main_rows, image_rows, run_stamp)` | Diffs current data vs the previous snapshot and writes the comparison tabs to the Google Sheet |
| `find_previous_snapshot_date(today)` | Finds the most recent dated snapshot in the checkout (excluding today's) |
| `fetch_all_properties()` | Paginates the `/api/properties` endpoint |
| `github_put_file(path, content, msg, shas)` | Creates/updates any file in the repo via the Contents API |
| `get_repo_file_shas()` | Returns `{path: blob_sha}` for every file in the repo (Git Trees API) |
| `upload_image_to_github(url, existing)` | Downloads a BasePMS image and commits it to this repo |
| `rows_to_csv(rows)` / `read_csv_file(path)` | CSV serialise / parse helpers |
| `normalize_rows(rows)` | Round-trips in-memory rows through CSV so typed values compare as strings (avoids false-positive diffs) |
| `rows_to_dict(rows, key_cols)` | Turns rows into a dict keyed by a composite key (used in comparison) |
| `get_brand(email)` | Maps a property's email domain to a brand name |
| `to_list(resp)` | Normalises various BasePMS API response shapes into a plain list |

---

## Rate Limiting

The script sleeps `1.1 seconds` between every API call (`DELAY_SECONDS = 1.1`). This is intentional — BasePMS enforces a rate limit. Do not remove it.

---

## How to Run Manually

Via GitHub UI: go to **Actions → BasePMS Weekday Sync & Compare → Run workflow**.

Locally (requires all four env vars set):
```bash
pip install -r requirements.txt
python basepms_sync.py    # data + snapshot + Sheets publish + comparison
```

## Corresponding Google Sheet: https://docs.google.com/spreadsheets/d/14Qx-9nZlACYfQoJF7EDtZBPib7LfaoeQeixMjEGpxVY/edit?gid=973262602#gid=973262602
