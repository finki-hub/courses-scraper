import os
from pathlib import Path
from shutil import copymode
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pandas as pd


def _fsync_directory(path: Path) -> None:
    if os.name != "posix":
        return
    directory_descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def write_csv_atomically(df: pd.DataFrame, destination: Path) -> None:
    target = destination.resolve() if destination.is_symlink() else destination

    with TemporaryDirectory(
        dir=target.parent,
        prefix=".csv-",
    ) as temporary_directory:
        temporary_path = Path(temporary_directory) / (
            "data" + "".join(destination.suffixes)
        )
        if destination.name.lower().endswith(".zip"):
            archive_name = (
                destination.stem if destination.suffix == ".zip" else destination.name
            )
            df.to_csv(
                temporary_path,
                index=False,
                compression={"method": "zip", "archive_name": archive_name},
            )
        elif destination.name.lower().endswith(
            (".tar", ".tar.gz", ".tar.bz2", ".tar.xz")
        ):
            # Match pandas' default: only an uncompressed .tar loses its suffix.
            archive_name = (
                destination.stem if destination.suffix == ".tar" else destination.name
            )
            df.to_csv(
                temporary_path,
                index=False,
                compression={"method": "tar", "archive_name": archive_name},
            )
        else:
            df.to_csv(temporary_path, index=False)
        with temporary_path.open("r+b") as temporary_file:
            os.fsync(temporary_file.fileno())
        if target.exists():
            copymode(target, temporary_path)
        temporary_path.replace(target)
        _fsync_directory(target.parent)


def write_bytes_atomically(destination: Path, payload: bytes) -> None:
    with NamedTemporaryFile(
        mode="wb",
        dir=destination.parent,
        prefix=f".{destination.name}.",
        delete=False,
    ) as temporary_file:
        temporary_path = Path(temporary_file.name)
        try:
            temporary_file.write(payload)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        except OSError:
            temporary_path.unlink(missing_ok=True)
            raise
    try:
        temporary_path.replace(destination)
        _fsync_directory(destination.parent)
    except OSError:
        temporary_path.unlink(missing_ok=True)
        raise
