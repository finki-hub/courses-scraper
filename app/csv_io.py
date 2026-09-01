from pathlib import Path
from shutil import copymode
from tempfile import TemporaryDirectory

import pandas as pd


def write_csv_atomically(df: pd.DataFrame, destination: Path) -> None:
    target = destination.resolve() if destination.is_symlink() else destination

    with TemporaryDirectory(
        dir=target.parent,
        prefix=f".{destination.name}.",
    ) as temporary_directory:
        temporary_path = Path(temporary_directory) / destination.name
        df.to_csv(temporary_path, index=False)
        if target.exists():
            copymode(target, temporary_path)
        temporary_path.replace(target)
