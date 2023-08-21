import argparse
import sqlite3
import sys
from collections.abc import Sequence
from pathlib import Path
from sqlite3.dbapi2 import Connection
from typing import Optional

import httpx


def missingpuididentifier(file: Path) -> None:
    """Print a list of every puid in the given files.db that isn't handled in our reference-files.

    Args:
        file (Path): Path to the files.db
    """
    response_convert = httpx.get(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_convert.json"  # noqa
    )
    response_convert_unarchiver = httpx.get(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_extract.json"  # noqa
    )
    response_convert_symphovert = httpx.get(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_convert_symphovert.json"  # noqa
    )
    response_reidentify = httpx.get(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_reidentify.json"  # noqa
    )
    response_ignore = httpx.get(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_ignore.json"  # noqa
    )
    response_custom = httpx.get(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/custom_signatures.json"  # noqa
    )
    response_manual_conversion = httpx.get(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/manual_convert.json",
    )

    # Accumulate fileformats that we can handle
    handled_formats: dict = response_convert.json()
    convert_unarchiver_dict: dict = response_convert_unarchiver.json()
    handled_formats.update(convert_unarchiver_dict)
    convert_symphovert_dict: dict = response_convert_symphovert.json()
    handled_formats.update(convert_symphovert_dict)
    convert_reidentify_dict: dict = response_reidentify.json()
    handled_formats.update(convert_reidentify_dict)
    custom_formats_dict: dict = response_custom.json()
    handled_formats.update({v["puid"]: v for v in custom_formats_dict})
    manual_conversion_dict: dict = response_manual_conversion.json()
    handled_formats.update(manual_conversion_dict)

    # Fileformats that we ignore
    ignored_formats: dict = response_ignore.json()

    handled_files: int = 0
    unidentified_files: int = 0
    ignored_files: int = 0
    unhandled_files: list[tuple[str, int, str]] = []
    manual_conversion_files: list[tuple[str, int, str]] = []
    convert_unarchiver_files: list[tuple[str, int, str]] = []
    convert_symphovert_files: list[tuple[str, int, str]] = []

    # Query _Signaturecount-view from files.db. Print puid,
    # signature and count for all puids not handled in the .json-files
    try:
        con: Connection = sqlite3.connect(f"file:{file}?mode=ro", uri=True)
    except sqlite3.DatabaseError as e:
        sys.exit(f"Error when connection to database: {e}")
    else:
        query: str = "SELECT puid, signature, count FROM _SignatureCount ORDER BY count DESC"
        for puid, sig, count in con.execute(query):
            if puid is None:
                unidentified_files += count
            elif puid in handled_formats:
                handled_files += count
            elif puid in ignored_formats:
                ignored_files += count
            elif puid in manual_conversion_dict:
                manual_conversion_files.append((puid, count, sig))
            elif puid in convert_unarchiver_dict:
                convert_unarchiver_files.append((puid, count, sig))
            elif puid in convert_symphovert_dict:
                convert_symphovert_files.append((puid, count, sig))
            else:
                unhandled_files.append((puid, count, sig))

        print(
            f"There {'were' if len(unhandled_files) != 1 else 'was'} {len(unhandled_files)} "
            + "unhandled file-formats"
            + (":" if unhandled_files else ".")
        )
        if unhandled_files:
            print(f"{'PUID':<16} | {'Count':<10} | Type")
            print("\n".join(f"{p:<16} | {c:<10} | {s}" for p, c, s in unhandled_files), end="\n\n")

        print(
            f"There {'were' if len(manual_conversion_files) != 1 else 'was'} {len(manual_conversion_files)} "
            + "file-formats marked for manual conversion"
            + (":" if manual_conversion_files else ".")
        )
        if manual_conversion_files:
            print(f"{'PUID':<16} | {'Count':<10} | Type")
            print(
                "\n".join(f"{p:<16} | {c:<10} {s}" for p, c, s in manual_conversion_files),
                end="\n\n",
            )

        print(
            f"There {'were' if len(convert_unarchiver_files) != 1 else 'was'} {len(convert_unarchiver_files)} "
            + f"file-formats marked for extraction"
            + (":" if convert_unarchiver_files else ".")
        )
        if convert_unarchiver_files:
            print(f"{'PUID':<16} | {'Count':<10} | Type")
            print(
                "\n".join(f"{p:<16} | {c:<10} | {s}" for p, c, s in convert_unarchiver_files),
                end="\n\n",
            )

        print(
            f"There {'were' if len(convert_symphovert_files) != 1 else 'was'} {len(convert_symphovert_files)} "
            + "file-formats marked for conversion with Symphony"
            + (":" if convert_symphovert_files else ".")
        )
        if convert_symphovert_files:
            print(f"{'PUID':<16} | {'Count':<10} | Type")
            print(
                "\n".join(f"{p:<16} | {c:<10} | {s}" for p, c, s in convert_symphovert_files),
                end="\n\n",
            )

        print(f"There {'were' if ignored_files != 1 else 'was'} {ignored_files} ignored files.")
        print(
            f"There {'were' if unidentified_files != 1 else 'was'} {unidentified_files} unidentified files."
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "file",
        help="Path to the files.db generated by digiarch that's "
        "to be compared with our current convertool json files",
        type=Path,
    )
    args: argparse.Namespace = parser.parse_args(argv)
    missingpuididentifier(args.file)

    return 0


if __name__ == "__main__":
    SystemExit(main())
