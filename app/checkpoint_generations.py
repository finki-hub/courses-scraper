import re
from collections.abc import Iterator
from pathlib import Path
from typing import Final

_GENERATION_FILE: Final = re.compile(
    r"checkpoint_(?:new|old)\.([0-9a-f]{32})\.csv",
)


def generation_files(directory: Path) -> Iterator[tuple[Path, str]]:
    for candidate in directory.iterdir():
        match = _GENERATION_FILE.fullmatch(candidate.name)
        if match is not None:
            yield candidate, match.group(1)


def remove_superseded(directory: Path, current_generation: str) -> None:
    for candidate, generation in generation_files(directory):
        if generation != current_generation:
            candidate.unlink()


def remove_all(directory: Path) -> None:
    for candidate, _ in generation_files(directory):
        candidate.unlink()
