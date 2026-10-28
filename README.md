# FINKI Hub / Courses Scraper

Export FCSE Courses profiles to CSV. Scrape both Moodle instances, or scrape only
the new instance while preserving old-instance data from a previous export.

## Installation

Install `uv` and Python 3.13.x, then install the locked dependencies:

```text
uv sync --locked
```

Alternatively, install the runtime dependencies with
`python -m pip install -r requirements.txt` and run `python -m app` instead of
`uv run python -m app` in the examples below.

## Quick Start: New Instance with Historical Data

Keep a backup of your previous **combined export** outside `output/`, then run:

```text
uv run python -m app --instances new --historical "D:/backups/profiles-historical.csv" --cas -m 18000
```

Enter your FINKI CAS username and password when prompted. This example requests
new-instance IDs 1 through 18000; choose the range you need.

The result is written to `output/profiles.csv`. The historical source is never
overwritten, and the final dataset retains:

- All historical profiles, including old-only profiles and those outside the
  requested scrape range.
- All old-instance fields from the historical export.
- Fresh new-instance fields for successfully scraped profiles, plus newly found
  profiles.

Fresh nonblank names and emails take precedence, with historical values as
fallback. Courses are combined with fresh courses first, duplicates removed, and
course counts recalculated.

The historical file must have the combined export's complete schema and valid,
unique instance IDs. Raw checkpoints cannot be used as historical exports. The
source and output must be different files.

**Historical values are not necessarily current.** Previous combined exports do
not store separate old names, emails, or course lists, so those original values
cannot be reconstructed exactly. Historical shared values and courses are kept;
profiles not returned by the new scrape remain in the dataset. The source path
and content hash are logged for provenance.

## Scraping Both Instances

When both services are available, scrape them together:

```text
uv run python -m app --instances both --cas -m 18000
```

`both` is the default. Each selected instance must authenticate and pass a profile
check before scraping. There is no automatic fallback if an instance is down.
`--historical` is only accepted with `--instances new`.

## Authentication

### CAS

Use `--cas` to log in to the selected instances with one credential pair.

- Username: `--cas-username`, then `COURSES_CAS_USERNAME`, then a terminal prompt.
- Password: `COURSES_CAS_PASSWORD`, then a hidden terminal prompt.

Passwords are never accepted as command-line arguments. MFA and CAPTCHA challenges
are not supported.

### Manual Cookies

Omit `--cas` to supply Moodle session cookies instead. For each selected instance,
the source precedence is:

1. `-c1` for new Moodle or `-c2` for old Moodle.
2. `COURSES_COOKIE_NEW` or `COURSES_COOKIE_OLD`.
3. A hidden terminal prompt.

Prefer environment variables or prompts to keep cookies out of command history.
Cookies must be nonblank and contain no control characters, whitespace,
semicolons, or commas. New-only mode needs only the new cookie and rejects `-c2`.

## Arguments

| Argument | Purpose |
| --- | --- |
| `-h` | Show help. |
| `--instances {both,new}` | Select live instances; defaults to `both`. |
| `--historical PATH` | Previous combined CSV; required for `--instances new`. |
| `--cas` | Authenticate using FINKI CAS. |
| `--cas-username USERNAME` | Set the CAS username. |
| `-c1 COOKIE`, `-c2 COOKIE` | Supply new- and old-instance cookies. |
| `-m MAX_ID` | Request IDs from 1 through this maximum, inclusive. |
| `-i ID [ID ...]` | Request specific profile IDs instead of a range. |
| `-t THREADS` | Worker count; defaults to 10 per selected instance. |
| `-o FILENAME` | Output filename under `output/`; defaults to `profiles.csv`. |

Choose exactly one of `-m` or `-i`. IDs and thread counts must be positive.
`-o` must be a plain filename, not a path.

## Output and Recovery

Profiles are matched by Moodle user ID, never by names or email addresses. Old
IDs **16339 and above** are excluded because account IDs diverge after migration.

The combined CSV includes shared `Name`, `Mail`, `Courses`, and `Courses Count`
columns, instance-specific fields ending in `_old` and `_new`, and each instance's
ID and profile link. Formula-like spreadsheet cells are escaped in the final
export; raw checkpoints retain their original values.

Progress is checkpointed periodically and on interruption. Resume with the same
requested ID set. New-only checkpoints are isolated under
`output/.checkpoints-new-only/<snapshot-content-hash>/`; use the same historical
snapshot contents to resume. They do not replace or clear two-instance checkpoints
or mark historical profiles as freshly scraped.

Invalid checkpoints fail rather than being silently ignored. Failed or empty
scrapes preserve the previous output and recovery checkpoints. Checkpoints are
cleared only after a successful final export.

## License

This project is licensed under the terms of the MIT license.
