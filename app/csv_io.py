from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd


def write_csv_atomically(df: pd.DataFrame, destination: Path) -> None:
    with NamedTemporaryFile(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
        delete=False,
    ) as temporary_file:
        temporary_path = Path(temporary_file.name)

    try:
        df.to_csv(temporary_path, index=False)
        temporary_path.replace(destination)
    finally:
        temporary_path.unlink(missing_ok=True)
