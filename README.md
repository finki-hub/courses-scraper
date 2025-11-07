# Courses Scraper

Script for scraping all profiles from FCSE Courses (both instances) into CSV format.

## TL;DR

1. Install `uv`
2. Get your Courses `MOODLESESSION` cookies for both instances
3. Run `uv run python -m app -m 17000 -c1 <NEW_COOKIE> -c2 <OLD_COOKIE>`

## Installation

Python 3.12 or higher is required and `uv` is optional.

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

The output CSV file will contain all profile fields from both instances with suffixes:

- `_old` for fields from the old instance (e.g., `Name_old`, `Description_old`)
- `_new` for fields from the new instance (e.g., `Name_new`, `Description_new`)
- `ID` remains unsuffixed as it's the common key

## License

This project is licensed under the terms of the MIT license.
