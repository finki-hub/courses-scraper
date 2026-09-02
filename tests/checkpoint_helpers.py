from __future__ import annotations

from pathlib import Path

import app.__main__ as scraper
from app.constants import base_urls, selectors_new, selectors_old
from app.http import InstanceHttpConfig


def make_config(tmp_path: Path) -> scraper.ScrapeConfig:
    return scraper.ScrapeConfig(
        http_new=InstanceHttpConfig(
            base_url=base_urls["new"],
            cookie="new-cookie",
            selectors=selectors_new,
            threads=1,
        ),
        http_old=InstanceHttpConfig(
            base_url=base_urls["old"],
            cookie="old-cookie",
            selectors=selectors_old,
            threads=1,
        ),
        checkpoint_new=tmp_path / "checkpoint_new.csv",
        checkpoint_old=tmp_path / "checkpoint_old.csv",
    )
