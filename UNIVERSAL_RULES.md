# Universal rules — basepms-sync

This project is ON HOLD. This file exists so the cross-project rules are findable from here anyway.

## Universal rules — read before changing anything

Rules that apply across **every** shadybaby-hub project live in one place, not in
each repo. BASE Data Tracker `1gnSbSo2yEc4kSTwEDeyaz8XK4ASucM0Bb5JaChqXf_8`:

| Tab | What it holds | Who edits it |
|---|---|---|
| `Universal Rules` | 28 principles, each with the incident that earned it | **code** — `price-audit/publish_universal_rules.py` |
| `Property Exceptions` | parked / leaving / not-live-yet / not-selling | **Callen, by hand** — and it is **read live by code** in price-audit and hfs-scraper |

Do **not** copy the rules into this file. A second copy drifts, which is the exact
failure the tab exists to end — the same reason `parked.py` is now a thin
re-export instead of its own list.

> **Corrected 2026-08-16 — the rolling start date is DISPLAY ONLY.** Our websites
> show a contract rolled to the next Monday with a shrinking remaining term. The
> **JSON API does not**: it returns the original academic year, room type and full
> duration, so scraped `start_date`, `length` and `end_date` are **stable** and
> safe as join keys. Tribeka's page says "6 Bed Ensuite, 2 Weeks, 24 Aug → 4 Sep";
> the API says `25/26 - 6 Bed Ensuite - 34 Weeks`. Only a value a **human** read
> off a page — or an agent who copied one — can be rolled.

