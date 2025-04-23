# Courses Scraper

Script for scraping all profiles from FCSE Courses into CSV format.

## TL;DR

1. Install `uv`
2. Get your Courses `MOODLESESSION` cookie
3. Run `uv run python -m app -m 17000 -c <COOKIE>`

## Installation

Python 3.12 or higher is required and `uv` is optional.

`python -m pip install -r requirements.txt`

## Running

`python -m app <arguments>`

Arguments:

1. `-h` - shows help message
2. `-c` - set `MoodleSession` cookie content
3. `-o` - output file name
4. `-i` - profile IDs to be scraped
5. `-m` - upper limit of profile IDs to be scraped

The arguments `-c` and either one of `-i` or `-m` are required.

For example:

`python -m app -m 16500 -c f82jike0jehnbvitk87et14fku`

## License

This project is licensed under the terms of the MIT license.
