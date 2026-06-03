# basepms-sync — Project Overview

> **Reminder:** Update this file whenever you make changes to the project. Ask Claude to re-explain and rewrite it if needed.

---

## What This Project Does

Pulls all student accommodation listings (properties, room types, pricing) from the **BasePMS API**. The **full dataset is stored as CSV files in this GitHub repo** (under `data/`). Each run also keeps a dated snapshot, and the script generates a **week-over-week comparison report** highlighting what changed — the comparison is the **only** thing published to Google Sheets.

Think of it as an automated price-audit pipeline: GitHub holds the raw data history, Google Sheets shows only the diff.

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

## Two Run Modes

The script behaviour is controlled by the `RUN_MODE` environment variable.

### Mode 1: `sync` (manual only)

Triggered by `.github/workflows/manual_sync.yml` (manual `workflow_dispatch` only — no schedule).

1. Fetches all properties from BasePMS (paginated)
2. For each property, fetches room types + pricing for **2025/2026** and **2026/2027**
3. Re-hosts any BasePMS-origin images to this GitHub repo (`images/` folder) so they don't expire
4. Writes the full dataset to two CSV files in this repo (via the GitHub Contents API):
   - `data/basepms_latest.csv` — one row per room type instalment (pricing, dates, thumbnail, etc.)
   - `data/basepms_images_latest.csv` — one row per image per room type

No Google Sheets are touched in this mode.

### Mode 2: `friday` (runs Mon–Fri at 06:42 UTC)

Triggered by `.github/workflows/weekday_sync.yml`. Does everything in Mode 1, **plus**:

1. Writes a **dated snapshot** of the full dataset:
   - `data/snapshots/basepms_YYYYMMDD.csv`
   - `data/snapshots/basepms_images_YYYYMMDD.csv`
2. **Compares** today's snapshot against the most recent previous snapshot in the repo
3. **Publishes the comparison report to Google Sheets** (see below)

Snapshots are **kept forever** — nothing is pruned. History lives in the repo / git log.

---

## The Comparison Report (Google Sheets — the only thing published there)

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

## Secondary Sheet — Changes-Only Rolling Log

In `friday` mode, after writing the per-date `Comparison_*` tabs to the primary sheet, the script also pushes a **changes-only** view into a **second Google Sheet** (`SHEET_ID_2`). This is handled by `push_rolling_subset()`.

Two single, ever-present tabs (not dated):

| Tab | Source | Rows kept |
|---|---|---|
| `Rooms` | `Comparison_YYYYMMDD` | every row **except** `change_flag == NO CHANGE` |
| `Room Images` | `Comparison_YYYYMMDD_images` | every row **except** `status == NO CHANGE` |

Behaviour:
- Each row gets a leading **`date`** column (ISO `YYYY-MM-DD`).
- **Newest day on top** — today's changed rows are prepended above the existing rows.
- **Rolling 30 days** (`ROLLING_DAYS`) — rows with a `date` older than 30 days are pruned on each run.
- **Idempotent per day** — re-running on the same date replaces that day's block instead of duplicating it.
- **Header styling** — row 1 is frozen with an `#F5A04C` background, and the header labels are prettified (underscores → spaces, title-cased), e.g. `change_flag` → `Change Flag`. Reapplied every run.

> The service account (from `GOOGLE_CREDENTIALS`) **must be shared as an Editor** on the secondary sheet, or this push fails (the failure is caught and logged; it does not abort the primary comparison). The sheet ID has a default baked into the script but can be overridden with the `SHEET_ID_2` env var.

---

## Where the Data Lives

| Location | Written by | Contents |
|---|---|---|
| `data/basepms_latest.csv` (GitHub) | Every run | Current live data — all properties/rooms/pricing |
| `data/basepms_images_latest.csv` (GitHub) | Every run | All image URLs per room type |
| `data/snapshots/basepms_YYYYMMDD.csv` (GitHub) | Friday mode | Dated snapshot of the full data (kept forever) |
| `data/snapshots/basepms_images_YYYYMMDD.csv` (GitHub) | Friday mode | Dated snapshot of the images |
| `Comparison_YYYYMMDD` (primary Sheet) | Friday mode | Run-over-run pricing/date/image diff |
| `Comparison_YYYYMMDD_images` (primary Sheet) | Friday mode | Run-over-run image diff |
| `Rooms` (secondary Sheet `SHEET_ID_2`) | Friday mode | Rolling 30-day log of changed rooms (no `NO CHANGE`) |
| `Room Images` (secondary Sheet `SHEET_ID_2`) | Friday mode | Rolling 30-day log of changed images (no `NO CHANGE`) |

| Location | Written by | Contents |
|---|---|---|
| `data/basepms_latest.csv` (GitHub) | Every run | Current live data — all properties/rooms/pricing |
| `data/basepms_images_latest.csv` (GitHub) | Every run | All image URLs per room type |
| `data/snapshots/basepms_YYYYMMDD.csv` (GitHub) | Friday mode | Dated snapshot of the full data (kept forever) |
| `data/snapshots/basepms_images_YYYYMMDD.csv` (GitHub) | Friday mode | Dated snapshot of the images |
| `Comparison_YYYYMMDD` (Google Sheet) | Friday mode | Run-over-run pricing/date/image diff |
| `Comparison_YYYYMMDD_images` (Google Sheet) | Friday mode | Run-over-run image diff |

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

| Variable | Used by | What it is |
|---|---|---|
| `BASEPMS_API_TOKEN` | both modes | Bearer token for the BasePMS API |
| `GITHUB_TOKEN` | both modes | Used to write CSVs and images to this repo via the Contents API (auto-provided by GitHub Actions; needs `contents: write`) |
| `GOOGLE_CREDENTIALS` | `friday` only | Full JSON of a Google service account (Sheets + Drive) — for the comparison report |
| `SHEET_ID` | `friday` only | The primary Google Sheet ID (from the URL) |
| `SHEET_ID_2` | `friday` only (optional) | The secondary Google Sheet ID for the rolling changes-only log. Has a default baked into the script; override only to point elsewhere. The service account must be an Editor on it. |

`RUN_MODE` is set directly in the workflow YAML.

---

## Key Functions Quick Reference

| Function | What it does |
|---|---|
| `main()` | Entry point — fetches the repo file list, then branches on `RUN_MODE` |
| `collect_data(existing_images)` | Fetches all data, returns `(main_rows, image_rows)` ready for CSV |
| `write_latest_csv(...)` | Writes `data/basepms_latest.csv` + images CSV to the repo |
| `write_snapshot_csv(..., today)` | Writes the dated snapshot CSVs to `data/snapshots/` |
| `run_compare(spreadsheet, today, main_rows, image_rows, spreadsheet2)` | Diffs current data vs the previous snapshot, writes comparison tabs to the primary Sheet, and pushes the changes-only rolling log to the secondary Sheet |
| `push_rolling_subset(sheet, tab, header, dated_rows, today)` | Maintains a single rolling tab: newest on top, replaces today's block, prunes rows older than `ROLLING_DAYS` |
| `get_tab_keep(sheet, tab, cols)` | Gets a tab or creates it **without** clearing (used for the rolling tabs) |
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

Via GitHub UI: go to **Actions → BasePMS Manual Sync → Run workflow** (or the **BasePMS Weekday Sync & Compare** workflow).

Locally (requires the env vars set — `RUN_MODE=friday` also needs `GOOGLE_CREDENTIALS` and `SHEET_ID`):
```bash
pip install -r requirements.txt
RUN_MODE=sync   python basepms_sync.py    # full data → data/*.csv
RUN_MODE=friday python basepms_sync.py    # data + snapshot + comparison → Sheets
```

## Corresponding Google Sheet: https://docs.google.com/spreadsheets/d/14Qx-9nZlACYfQoJF7EDtZBPib7LfaoeQeixMjEGpxVY/edit?gid=973262602#gid=973262602
