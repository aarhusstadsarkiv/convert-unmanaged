import argparse
import sqlite3
import sys
from collections.abc import Callable, Sequence
from json import loads
from pathlib import Path
from shutil import copy
from sqlite3.dbapi2 import Connection
from typing import Optional, Union
from urllib.request import urlopen


def argtype_examples(minimum: int, maximum: int) -> Callable[[Union[str, int]], int]:
    def inner(arg: Union[str, int]) -> int:
        try:
            f = int(arg)

            if f < minimum or f > maximum:
                raise argparse.ArgumentTypeError(
                    f"Argument must be in the range [{minimum}; {maximum}]",
                )

            return f
        except ValueError:
            raise argparse.ArgumentTypeError("Must be an integer number")

    return inner


# noinspection SqlNoDataSourceInspection,SqlResolve
def missingpuididentifier(file: Path, examples: int, examples_dir: Path) -> None:
    """Print a list of every puid in the given files.db that isn't handled in our reference-files.

    Args:
        file (Path): Path to the files.db
        examples (int): How many examples to extract unhandled files
        examples_dir (Path): Output directory for examples
    """
    response_convert = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/add-version-to-files/to_convert.json"  # noqa
    )
    response_convert_unarchiver = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/add-version-to-files/to_extract.json"  # noqa
    )
    response_convert_symphovert = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/add-version-to-files/to_convert_symphovert.json"  # noqa
    )
    response_reidentify = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/add-version-to-files/to_reidentify.json"  # noqa
    )
    response_ignore = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/add-version-to-files/to_ignore.json"  # noqa
    )
    response_custom = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/add-version-to-files/custom_signatures.json"  # noqa
    )
    response_manual_conversion = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/add-version-to-files/manual_convert.json",
    )

    # Accumulate fileformats that we can handle
    handled_formats: dict = loads(response_convert.read())
    convert_unarchiver_dict: dict = loads(response_convert_unarchiver.read())
    handled_formats.update(convert_unarchiver_dict.get("data"))  # type: ignore
    convert_symphovert_dict: dict = loads(response_convert_symphovert.read())
    handled_formats.update(convert_symphovert_dict.get("data"))  # type: ignore
    convert_reidentify_dict: dict = loads(response_reidentify.read())
    handled_formats.update(convert_reidentify_dict.get("data"))  # type: ignore
    custom_formats_dict: dict = loads(response_custom.read())
    handled_formats.update({v["puid"]: v for v in custom_formats_dict.get("data")})  # type: ignore
    manual_conversion_dict: dict = loads(response_manual_conversion.read())
    handled_formats.update(manual_conversion_dict.get("data"))  # type: ignore

    versions: dict = {
        "to_convert": handled_formats.get("version"),
        "to_extract": convert_unarchiver_dict.get("version"),
        "to_convert_symphovert": convert_symphovert_dict.get("version"),
        "to_reidentify": convert_reidentify_dict.get("version"),
        "custom_signatures": custom_formats_dict.get("version"),
        "manual_convert": manual_conversion_dict.get("version"),
    }

    print(f"Running convert unmanaged with the following version of ref. files: {versions}")

    # Fileformats that we ignore
    ignored_formats: dict = loads(response_ignore.read())

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
        examples_dir = (examples_dir or (file.parent / "examples")).resolve()
        examples_dir.mkdir(parents=True, exist_ok=True)
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
            "unhandled file-formats" + (":" if unhandled_files else "."),
        )
        if unhandled_files:
            print(f"{'PUID':<16} | {'Count':<10} | Type")
            print("\n".join(f"{p:<16} | {c:<10} | {s}" for p, c, s in unhandled_files), end="\n\n")

        if unhandled_files and examples > 0:
            for puid, *_ in unhandled_files:
                files: list[tuple[str, str]] = con.execute(
                    "SELECT uuid, relative_path FROM Files WHERE puid = ? "
                    "ORDER BY random() LIMIT ?",
                    [puid, examples],
                ).fetchall()

                output_dir = examples_dir.joinpath(puid.replace("/", "_"))
                output_dir.mkdir(parents=True, exist_ok=True)

                for uuid, relative_path in files:
                    copy(
                        file.parent.parent / relative_path,
                        output_dir / f"{uuid}{Path(relative_path).suffix}",
                    )

        print(
            f"There {'were' if len(manual_conversion_files) != 1 else 'was'} "
            f"{len(manual_conversion_files)} "
            "file-formats marked for manual conversion" + (":" if manual_conversion_files else "."),
        )
        if manual_conversion_files:
            print(f"{'PUID':<16} | {'Count':<10} | Type")
            print(
                "\n".join(f"{p:<16} | {c:<10} {s}" for p, c, s in manual_conversion_files),
                end="\n\n",
            )

        print(
            f"There {'were' if len(convert_unarchiver_files) != 1 else 'was'} "
            f"{len(convert_unarchiver_files)} "
            "file-formats marked for extraction" + (":" if convert_unarchiver_files else "."),
        )
        if convert_unarchiver_files:
            print(f"{'PUID':<16} | {'Count':<10} | Type")
            print(
                "\n".join(f"{p:<16} | {c:<10} | {s}" for p, c, s in convert_unarchiver_files),
                end="\n\n",
            )

        print(
            f"There {'were' if len(convert_symphovert_files) != 1 else 'was'} "
            f"{len(convert_symphovert_files)} "
            "file-formats marked for conversion with Symphony"
            + (":" if convert_symphovert_files else "."),
        )
        if convert_symphovert_files:
            print(f"{'PUID':<16} | {'Count':<10} | Type")
            print(
                "\n".join(f"{p:<16} | {c:<10} | {s}" for p, c, s in convert_symphovert_files),
                end="\n\n",
            )

        print(f"There {'were' if ignored_files != 1 else 'was'} {ignored_files} ignored files.")
        print(
            f"There {'were' if unidentified_files != 1 else 'was'} "
            f"{unidentified_files} unidentified files.",
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "file",
        help="Path to the files.db generated by digiarch that's "
        "to be compared with our current convertool json files",
        type=Path,
    )
    parser.add_argument(
        "--examples",
        type=argtype_examples(0, 10),
        required=False,
        default=0,
        help="Extract N examples of unhandled files",
    )
    parser.add_argument(
        "--examples-dir",
        type=Path,
        required=False,
        default=None,
        help="Set output directory for example files",
    )
    args: argparse.Namespace = parser.parse_args(argv)
    missingpuididentifier(args.file.resolve(), args.examples, args.examples_dir)

    return 0


if __name__ == "__main__":
    SystemExit(main())
