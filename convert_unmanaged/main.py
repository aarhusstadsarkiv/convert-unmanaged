import argparse
import sys
from pathlib import Path
import sqlite3
from sqlite3.dbapi2 import Connection
from typing import Optional, Sequence

import httpx


def missingpuididentifier(file: Path) -> None:
    """Prints a list of every puid in the given files.db that isn't
    currently handled in our json reference-files

    Args:
        file: Path to the files.db
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
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/manual_convert.json"
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

    unidentified_files: int = 0
    unhandled_files: list[str] = []
    manual_conversion_files: list[str] = []

    # Query _Signaturecount-view from files.db. Print puid,
    # signature and count for all puids not handled in the .json-files
    try:
        con: Connection = sqlite3.connect(f"file:{file}?mode=ro", uri=True)
    except sqlite3.DatabaseError as e:
        sys.exit(f"Error when connection to database: {e}")
    else:
        query: str = "SELECT puid, signature, count FROM _SignatureCount"
        for puid, sig, count in con.execute(query):
            if puid in manual_conversion_dict:
                manual_conversion_files.append(
                f"Puid: {str(puid).ljust(16)} Count: {str(count).ljust(10)}"
                f"Type: {sig}"
                )                
            if puid in handled_formats or puid in ignored_formats:
                continue
            if puid is None:
                unidentified_files = count
                continue

            unhandled_files.append(
                f"Puid: {str(puid).ljust(16)} Count: {str(count).ljust(10)}"
                f"Type: {sig}"
            )

        if unhandled_files:
            print("Currently unhandled fileformats in the supplied files.db:")
            for s in unhandled_files:
                print(s)
        else:
            print("No unhandled fileformats in the supplied files.db")

        if unidentified_files:
            print(f"There was {unidentified_files} unidentified files")
        else:
            print("No unidentified files")
            
        if manual_conversion_files:
            print("The following file-formats are marked for manual conversion")
            for f in manual_conversion_files:
                print(f)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "file",
        help="Path to the files.db generated by digiarch that's "
        + "to be compared with our current convertool json files",
        type=str,
    )
    args: argparse.Namespace = parser.parse_args(argv)
    missingpuididentifier(args.file)

    return 0


if __name__ == "__main__":
    SystemExit(main())
