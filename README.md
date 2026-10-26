# FINKI Hub / Courses Scraper

Script for scraping all profiles from FCSE Courses (both instances) into CSV format.

## TL;DR

1. Install `uv`
2. Run `uv run python -m app --cas -m 17000`
3. Enter your FINKI CAS username and password when prompted

If the old instance is unavailable, use the [new-only mode with a historical
snapshot](#scraping-only-the-new-instance) instead.

## Installation

Python 3.13.x is required. For a reproducible install from `uv.lock`, run:

`uv sync --locked`

Alternatively, install the bounded runtime dependencies with pip:

`python -m pip install -r requirements.txt`

## Running

`python -m app <arguments>`

Arguments:

1. `-h` - shows help message
2. `-c1` - set `MoodleSession` cookie for the new Courses instance at `https://courses.finki.ukim.mk`
3. `-c2` - set `MoodleSession` cookie for the old Courses instance at `https://oldcourses.finki.ukim.mk`
4. `--cas` - authenticate the selected Courses instances with one FINKI CAS credential pair
5. `--cas-username` - set the CAS username; the password is never accepted as a command-line argument
6. `-o` - output file name (default: profiles.csv)
7. `-t` - number of threads to use (default: 10)
8. `-i` - profile IDs to be scraped
9. `-m` - upper limit of profile IDs to be scraped
10. `--instances` - scrape `both` instances (default), or only `new` using a historical snapshot
11. `--historical` - previous combined CSV to preserve when using `--instances new`

Either `-i` or `-m` is required. Thread counts, maximum IDs, and every explicit
profile ID must be positive. The `-o` value must be a plain filename; output is
always written under `output/`.

There are two authentication modes. CAS mode performs separate logins for the selected
Courses services and preserves each host's `MoodleSession` and `SRVNAME`
cookies. The username source precedence is `--cas-username`,
`COURSES_CAS_USERNAME`, then a terminal prompt. The password source precedence is
`COURSES_CAS_PASSWORD`, then a hidden terminal prompt:

```text
COURSES_CAS_USERNAME=<USERNAME>
COURSES_CAS_PASSWORD=<PASSWORD>
```

Do not put the password on the command line. CAS mode does not support MFA or
CAPTCHA challenges.

Manual cookie mode remains available. For each instance, the cookie source
precedence is:

1. Explicit `-c1` or `-c2` flag
2. `COURSES_COOKIE_NEW` or `COURSES_COOKIE_OLD` environment variable
3. Hidden terminal prompt

Omit cookie flags to keep secrets out of command history. For example, set the
environment variables before running:

```text
COURSES_COOKIE_NEW=<NEW_COOKIE>
COURSES_COOKIE_OLD=<OLD_COOKIE>
```

If neither the corresponding flag nor environment variable is present, the
script prompts without echoing the cookie. Cookie values must be nonblank and
cannot contain control characters, whitespace, semicolons, or commas.

Before scraping, each selected instance must return an authenticated profile without a
redirect. During scraping, transport failures abort an instance when they exceed
half of at least three observed outcomes. Smaller batches abort when most requests
fail at the transport boundary. HTTP responses that contain no exportable profile
remain ordinary empty results.

Progress is checkpointed after 100 completed profiles, every 30 seconds while
new progress is pending, and on interruption. Re-running with the same profile
ID set resumes both instances independently. Invalid or mismatched checkpoint
data fails closed instead of being silently discarded.

CAS example:

`python -m app --cas -m 16500`

Manual-cookie example:

`python -m app -m 16500`

## Scraping Only the New Instance

When the old instance is unavailable or no longer changes, keep a backup of a
previous **combined export** outside the normal output location. Then scrape the
new instance and combine its results with that historical snapshot:

```text
uv run python -m app --instances new --historical "D:/backups/profiles-historical.csv" --cas -m 18000
```

The historical file is required in this mode and is never overwritten. It must
have the combined export's complete schema and valid, unique instance IDs; raw
checkpoint CSVs are not interchangeable with combined exports. The source and
destination must be different files. `--historical` is not used with the default
`--instances both` mode.

Only the new instance is authenticated, preflighted, and scraped. Manual-cookie
mode needs only the new cookie; `-c2` is rejected in new-only mode. There is no
silent outage fallback: authentication, transport, and parsing failures retain
their normal failure behavior.

The resulting `output/profiles.csv` retains **all historical profiles**, including
old-only profiles and profiles outside the selected scrape IDs. Successful fresh
profiles refresh the matching new-instance fields. Historical old-instance
fields are preserved. Fresh nonblank names and emails take precedence, with
historical values as fallback. Courses are a deduplicated union with fresh
courses first, and course counts are recalculated.

The previous combined export does not contain separate old names, emails, or
course lists. Their original values cannot be recovered exactly. Consequently,
the new combined output retains historical shared values and courses rather than
claiming that every retained value was observed in the latest scrape. Profiles
not returned by the latest scrape remain historical records, not evidence of
current visibility or enrollment. The source path and its content hash are logged
for provenance; the established CSV columns remain unchanged.

New-only checkpoints are isolated under
`output/.checkpoints-new-only/<snapshot-content-hash>/`. Resume with the same
snapshot contents and requested ID set. These checkpoints contain only fresh-run
progress, not historical completion claims, and do not replace or clear the
two-instance checkpoints. A failed or empty fresh scrape does not replace the
combined output with the historical snapshot alone.

## Live CAS Smoke Test

GitHub Actions authenticates and preflights the new Courses instance after pushes
to `main` and on manual dispatches from `main`. The workflow expects the
organization or repository Actions secrets `CAS_USERNAME` and `CAS_PASSWORD`,
matching the secret names used by `finki-hub/finki-auth`.

The smoke test does not scrape profiles or write output files. Runs without the
two secrets fail before Pytest starts; ordinary test runs skip the live tests.
The old-instance smoke case remains available for explicit local execution if
that service returns; it is not required by the default workflow.

## Output

The output CSV matches migrated profiles by their shared Moodle user ID. The old
instance is scraped only for IDs below **16339**; old IDs **16339 and above** are
excluded because account IDs diverge after migration. The same filter applies to
old rows loaded from existing checkpoints. New-instance IDs remain unrestricted
within your requested range. Live comparisons found that IDs 16339–16400 belonged
to different people across the two instances, making 16339 the first observed
divergence. ID 16336 matched; IDs 16337–16338 were inaccessible on both instances.

The CSV contains:

- `Name`, `Mail`, and `Courses` combined for matching migrated IDs
- `_old` and `_new` fields for instance-specific values such as `Description_old`
  and `Description_new`
- `ID_old` and `ID_new` for the instance-local Moodle user IDs
- `Profile_old` and `Profile_new` for links using the corresponding instance ID

Hidden, missing, changed, or duplicated emails do not prevent same-ID matches.
Profiles with different IDs remain separate even if their emails match. Names and
emails are never identity keys. New nonblank names and emails take precedence,
with old values as fallback; courses are combined with new courses first. Moodle
access-denied pages are skipped instead of being exported as profiles named `User`.
Formula-like spreadsheet cells are apostrophe-prefixed in the final CSV only;
raw checkpoint values remain unchanged. Empty runs never replace an existing
output or delete recovery checkpoints.

## License

This project is licensed under the terms of the MIT license.
