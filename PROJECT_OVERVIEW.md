# basepms-sync — Project Overview

> **Reminder:** Update this file whenever you make changes to the project. Ask Claude to re-explain and rewrite it if needed.

---

## 🛑 ON HOLD — until further notice (decided 2026-08-08)

**Do not run, re-schedule, or "fix" this project without asking first.** The user has
parked it. Leave the workflows on `workflow_dispatch`-only (the weekday schedule was
removed in `98e9ab9`) — **putting it back on a schedule would actively destroy data**,
because a scheduled run now commits a ~72-row snapshot over the good June data.

### What is actually wrong (measured 2026-08-08, supersedes the 2026-07-01 note below)

The pipeline **did not stop** — it silently collapsed to ~2% of the portfolio and kept
committing green runs:

| Snapshot | Rows | Properties |
|---|---|---|
| `basepms_20260603` … `20260609` | ~3,300 → 2,650 | **223** |
| `basepms_20260706` … `20260717` | **78** | **5** |
| `basepms_20260720` (latest) | **72** | **5** |

It collapsed around **2026-06-09/10** and never recovered. The July runs passed because
72 rows clears the zero-row safety guard in `collect_data()` — so the guard did **not**
protect us here; it only catches a total wipeout, not a 98% one. Nothing alerted.

**Two clues to start from:**
1. The 5 surviving properties — Aire, Hassell's Bridge, Hilbre Gardens, Keele House,
   Leighton Hall — are **all Urban Student Life**, i.e. one brand out of six.
2. `brand` is blank on **72 of 72** rows, where it was populated on 2026-06-09.

That points at the **`BASEPMS_API_TOKEN` losing its multi-brand scope** (or the account
behind it being narrowed), *not* at a code bug. First check when this is picked up:
does the token still return all 223 properties from `/api/properties`?

> ❌ **The old academic-year hypothesis is DISPROVEN.** The 2026-07-01 note guessed that
> `ACADEMIC_YEARS = ["2025/2026", "2026/2027"]` had gone stale after a rollover. It has
> not — the current data still contains **both** `2025 / 2026` and `2026 / 2027` rows.
> The years are fine; the property access is not. Don't spend time there.

### Knock-on effect elsewhere (why this matters beyond this repo)

`rooms-data` renders BASE thumbnails in its **"Room Types + Thumbnails"** audit tab via
`=IMAGE()` pointing at this repo's public `images/` mirror (basepms's own image URLs are
auth-gated and render broken). Because this project is frozen, **that mirror is stuck at
the 2026-07-20 snapshot**, so **184 of 1,596** thumbnails don't render (86.5% coverage)
and coverage will decay as basepms swaps images. Running the sync in its current state
would **not** help — it would touch only the 5 USL properties. Fixing that gap requires
fixing this project first.

_Superseded note, kept for history — 2026-07-01: "no fresh data since 2026-06-10, empty
`room_types` responses, hits the zero-row guard; leading hypothesis stale `ACADEMIC_YEARS`;
diagnostic `.github/workflows/api_probe.yml` sweeps 2024/2025–2027/2028." The empty-response
symptom did change (data returns, just almost none) and the hypothesis is disproven, but
`api_probe.yml` is still there as a **temporary diagnostic** — remove it once root-caused._

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

## Where the Data Lives

| Location | Written by | Contents |
|---|---|---|
| `data/basepms_latest.csv` (GitHub) | Every run | Current live data — all properties/rooms/pricing |
| `data/basepms_images_latest.csv` (GitHub) | Every run | All image URLs per room type |
| `data/snapshots/basepms_YYYYMMDD.csv` (GitHub) | Friday mode | Dated snapshot of the full data (kept forever) |
| `data/snapshots/basepms_images_YYYYMMDD.csv` (GitHub) | Friday mode | Dated snapshot of the images |
| `Comparison_YYYYMMDD` (Sheet) | Friday mode | Run-over-run pricing/date/image diff |
| `Comparison_YYYYMMDD_images` (Sheet) | Friday mode | Run-over-run image diff |

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
| `SHEET_ID` | `friday` only | The Google Sheet ID (from the URL) |

`RUN_MODE` is set directly in the workflow YAML.

---

## Key Functions Quick Reference

| Function | What it does |
|---|---|
| `main()` | Entry point — fetches the repo file list, then branches on `RUN_MODE` |
| `collect_data(existing_images)` | Fetches all data, returns `(main_rows, image_rows)` ready for CSV |
| `write_latest_csv(...)` | Writes `data/basepms_latest.csv` + images CSV to the repo |
| `write_snapshot_csv(..., today)` | Writes the dated snapshot CSVs to `data/snapshots/` |
| `run_compare(spreadsheet, today, main_rows, image_rows)` | Diffs current data vs the previous snapshot and writes the comparison tabs to the Google Sheet |
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
