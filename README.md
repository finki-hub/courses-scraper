# FINKI Hub / Courses Scraper

Script for scraping all profiles from FCSE Courses (both instances) into CSV format.

## TL;DR

1. Install `uv`
2. Run `uv run python -m app --cas -m 17000`
3. Enter your FINKI CAS username and password when prompted

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
4. `--cas` - authenticate both Courses instances with one FINKI CAS credential pair
5. `--cas-username` - set the CAS username; the password is never accepted as a command-line argument
6. `-o` - output file name (default: profiles.csv)
7. `-t` - number of threads to use (default: 10)
8. `-i` - profile IDs to be scraped
9. `-m` - upper limit of profile IDs to be scraped

Either `-i` or `-m` is required. Thread counts, maximum IDs, and every explicit
profile ID must be positive. The `-o` value must be a plain filename; output is
always written under `output/`.

There are two authentication modes. CAS mode performs separate logins for the new
and old Courses services and preserves each host's `MoodleSession` and `SRVNAME`
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

Before scraping, each instance must return an authenticated profile without a
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

## Output

The output CSV matches profiles by a normalized email address only when that address
is unique in both instances. Moodle user IDs are local to each instance and are not
used as a cross-instance identity.

The CSV contains:

- `Name`, `Mail`, and `Courses` combined for unique-email matches
- `_old` and `_new` fields for instance-specific values such as `Description_old`
  and `Description_new`
- `ID_old` and `ID_new` for the instance-local Moodle user IDs
- `Profile_old` and `Profile_new` for links using the corresponding instance ID

Profiles with a missing, malformed, one-sided, or duplicated email remain separate
rows. Names are never used as identity keys because they are not guaranteed unique.
Formula-like spreadsheet cells are apostrophe-prefixed in the final CSV only;
raw checkpoint values remain unchanged. Empty runs never replace an existing
output or delete recovery checkpoints.

## License

This project is licensed under the terms of the MIT license.
