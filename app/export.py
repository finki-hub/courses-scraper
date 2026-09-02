import re
from pathlib import Path
from typing import Final

import pandas as pd

from app.csv_io import write_csv_atomically

__all__ = ["EmptyExportError", "export_profiles"]

_FORMULA_PREFIX: Final = re.compile(r"\s*[=+\-@]")


class EmptyExportError(Exception):
    def __str__(self) -> str:
        return "refusing to export an empty profile dataset"


def _neutralize_formula[Cell](value: Cell) -> Cell | str:
    if isinstance(value, str) and _FORMULA_PREFIX.match(value):
        return f"'{value}"
    return value


def export_profiles(frame: pd.DataFrame, destination: Path) -> None:
    if frame.empty:
        raise EmptyExportError
    sanitized = frame.copy(deep=True).map(_neutralize_formula)
    write_csv_atomically(sanitized, destination)
