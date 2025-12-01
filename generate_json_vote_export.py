import argparse
from pathlib import Path
from typing import List

from json_vote_builder import generate_vote_export_from_json


def _default_archives(json_dir: Path) -> List[Path]:
    if not json_dir.exists():
        raise FileNotFoundError(f"JSON data directory does not exist: {json_dir}")
    archives = sorted(json_dir.glob("*.zip"))
    if not archives:
        raise FileNotFoundError(f"No JSON ZIP archives found in {json_dir}")
    return archives


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a vote export from LegiScan JSON data.")
    parser.add_argument(
        "name",
        help="Exact legislator name to export (must match the JSON dataset).",
    )
    parser.add_argument(
        "--archives",
        nargs="+",
        type=Path,
        help="One or more LegiScan JSON ZIP archives. Defaults to all archives in --json-dir.",
    )
    parser.add_argument(
        "--json-dir",
        type=Path,
        default=Path("JSON DATA"),
        help="Directory containing LegiScan JSON ZIP files (used when --archives is omitted).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("json_vote_export.xlsx"),
        help="Path to the generated Excel workbook.",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    archives = args.archives or _default_archives(args.json_dir)
    row_count = generate_vote_export_from_json(archives, args.name, args.output)
    print(f"Wrote {row_count} rows to {args.output}")


if __name__ == "__main__":
    main()
