import argparse
import sys
from collections.abc import Callable
from collections.abc import Sequence
from json import loads
from pathlib import Path
from shutil import copy
from sqlite3 import DatabaseError
from typing import Optional
from typing import Union
from urllib.request import urlopen

from acacore.database import FileDB  # type: ignore


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
    response_version = urlopen(
        "https://api.github.com/repos/aarhusstadsarkiv/reference-files/commits/main"  # noqa
    )
    response_convert = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_convert.json"  # noqa
    )
    response_convert_unarchiver = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_extract.json"  # noqa
    )
    response_convert_symphovert = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_convert_symphovert.json"  # noqa
    )
    response_reidentify = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_reidentify.json"  # noqa
    )
    response_ignore = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_ignore.json"  # noqa
    )
    response_custom = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/custom_signatures.json"  # noqa
    )
    response_manual_conversion = urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/manual_convert.json",
    )

    # Accumulate fileformats that we can handle
    version: str = loads(response_version.read())["sha"]
    handled_formats: dict = loads(response_convert.read())
    convert_unarchiver_dict: dict = loads(response_convert_unarchiver.read())
    handled_formats.update(convert_unarchiver_dict)
    convert_symphovert_dict: dict = loads(response_convert_symphovert.read())
    handled_formats.update(convert_symphovert_dict)
    convert_reidentify_dict: dict = loads(response_reidentify.read())
    handled_formats.update(convert_reidentify_dict)
    custom_formats_dict: dict = loads(response_custom.read())
    handled_formats.update({v["puid"]: v for v in custom_formats_dict})
    manual_conversion_dict: dict = loads(response_manual_conversion.read())
    handled_formats.update(manual_conversion_dict)

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
        db: FileDB = FileDB(f"file:{file}?mode=ro", uri=True)
        examples_dir = (examples_dir or (file.parent / "examples")).resolve()
        examples_dir.mkdir(parents=True, exist_ok=True)
    except DatabaseError as e:
        sys.exit(f"Error when connection to database: {e}")
    else:
        print(f"Using references version {version}.", end="\n\n")

        total_files = len(db.files)

        for sc in db.signature_count.select().fetchall():
            if sc.puid is None:
                unidentified_files += sc.count
            elif sc.puid in handled_formats:
                handled_files += sc.count
            elif sc.puid in ignored_formats:
                ignored_files += sc.count
            elif sc.puid in manual_conversion_dict:
                manual_conversion_files.append((sc.puid, sc.count, sc.signature))
            elif sc.puid in convert_unarchiver_dict:
                convert_unarchiver_files.append((sc.puid, sc.count, sc.signature))
            elif sc.puid in convert_symphovert_dict:
                convert_symphovert_files.append((sc.puid, sc.count, sc.signature))
            else:
                unhandled_files.append((sc.puid, sc.count, sc.signature))

        print(f"There {'were' if total_files != 1 else 'was'} {total_files} total files.")

        print(
            f"There {'were' if len(unhandled_files) != 1 else 'was'} {len(unhandled_files)} "
            "unhandled file-formats" + (":" if unhandled_files else "."),
        )
        if unhandled_files:
            print(f"{'PUID':<16} | {'Count':<10} | Type")
            print("\n".join(f"{p:<16} | {c:<10} | {s}" for p, c, s in unhandled_files), end="\n\n")

        if unhandled_files and examples > 0:
            print(
                f"Saving examples for {len(unhandled_files)} unhandled file formats... ",
                end="",
                flush=True,
            )

            for puid, *_ in unhandled_files:
                files: list[tuple[str, str]] = db.execute(
                    f"select UUID, RELATIVE_PATH "
                    f"from FILES "
                    f"where PUID = ? "
                    f"order by RANDOM() limit {examples}",
                    [puid],
                ).fetchall()

                output_dir = examples_dir.joinpath(puid.replace("/", "_"))
                output_dir.mkdir(parents=True, exist_ok=True)

                for uuid, relative_path in files:
                    copy(
                        file.parent.parent / relative_path,
                        output_dir / f"{uuid}{Path(relative_path).suffix}",
                    )

            print("Done", end="\n\n")

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

        print(f"There {'were' if handled_files != 1 else 'was'} {handled_files} handled files.")


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
