# FINKI Hub / Courses Scraper

Script for scraping all profiles from FCSE Courses (both instances) into CSV format.

## TL;DR

1. Install `uv`
2. Get your Courses `MOODLESESSION` cookies for both instances
3. Run `uv run python -m app -m 17000 -c1 <NEW_COOKIE> -c2 <OLD_COOKIE>`

## Installation

Python 3.13 or higher is required and `uv` is optional.

`python -m pip install -r requirements.txt`

## Running

`python -m app <arguments>`

Arguments:

1. `-h` - shows help message
2. `-c1` - set `MoodleSession` cookie for the new Courses instance at `https://courses.finki.ukim.mk` (required)
3. `-c2` - set `MoodleSession` cookie for the old Courses instance at `https://oldcourses.finki.ukim.mk` (required)
4. `-o` - output file name (default: profiles.csv)
5. `-t` - number of threads to use (default: 10)
6. `-i` - profile IDs to be scraped
7. `-m` - upper limit of profile IDs to be scraped

The arguments `-c1`, `-c2`, and either one of `-i` or `-m` are required.

For example:

`python -m app -m 16500 -c1 f82jike0jehnbvitk87et14fku -c2 a93klnp1kfiocdwml98fu25glv`

## Output

The output CSV matches profiles by a normalized email address only when that address
is unique in both instances. Moodle user IDs are local to each instance and are not
used as a cross-instance identity.

The CSV contains:

- `Name`, `Mail`, and `Courses` combined for confirmed matches
- `_old` and `_new` fields for instance-specific values such as `Description_old`
  and `Description_new`
- `ID_old` and `ID_new` for the instance-local Moodle user IDs
- `Profile_old` and `Profile_new` for links using the corresponding instance ID

Profiles with a missing, malformed, one-sided, or duplicated email remain separate
rows. Names are never used as identity keys because they are not guaranteed unique.

## License

This project is licensed under the terms of the MIT license.
