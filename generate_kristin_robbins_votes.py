import csv
import datetime as dt
from collections import defaultdict
from pathlib import Path
from typing import Dict, IO, Iterable, List, Optional, Sequence, Union

from openpyxl import Workbook

BASE_DIR = Path(__file__).resolve().parent / "MN_2019-2026_Lesislature"
TARGET_NAME = "Kristin Robbins"
OUTPUT_FILE = Path("kristin_robbins_vote_export.xlsx")
REQUIRED_SESSION_FILES = ("people.csv", "bills.csv", "rollcalls.csv", "votes.csv")

PARTY_LABELS = {
    "D": "Democrat",
    "R": "Republican",
}
VOTE_BUCKETS = ("For", "Against", "Absent", "Not")
WORKBOOK_HEADERS = [
    "Chamber",
    "Session",
    "Bill Number",
    "Bill ID",
    "Bill Motion",
    "URL",
    "Bill Title",
    "Bill Description",
    "Roll Details",
    "Committee ID",
    "Committee",
    "Last Action Date",
    "Last Action",
    "Status",
    "Status Description",
    "Status Date",
    "Roll Call ID",
    "Person",
    "Person Party",
    "Vote",
    "Vote Bucket",
    "Date",
    "Result",
    "Democrat_For",
    "Democrat_Against",
    "Democrat_Absent",
    "Democrat_Not",
    "Democrat_Total",
    "Republican_For",
    "Republican_Against",
    "Republican_Absent",
    "Republican_Not",
    "Republican_Total",
    "Other_For",
    "Other_Against",
    "Other_Absent",
    "Other_Not",
    "Other_Total",
    "Total_For",
    "Total_Against",
    "Total_Absent",
    "Total_Not",
    "Total_Total",
]

PathLike = Union[str, Path]
BaseDirsInput = Union[PathLike, Sequence[PathLike]]


def excel_serial(date_str: str) -> int:
    """Convert YYYY-MM-DD into Excel serial number (day count)."""
    date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    origin = dt.date(1899, 12, 30)  # Excel day-zero on Windows
    return (date - origin).days


def classify_vote(vote_desc: str) -> str:
    """Map LegiScan vote descriptions into output buckets."""
    if not vote_desc:
        return "Not"
    desc = vote_desc.strip().lower()
    if desc.startswith("yea") or desc in {"yes", "aye"}:
        return "For"
    if desc.startswith("nay") or desc in {"no"}:
        return "Against"
    if "absent" in desc or desc.startswith("excused"):
        return "Absent"
    if desc in {"nv", "not voting", "present", "paired", "pass", "p"}:
        return "Not"
    return "Not"


def read_csv(path: Path) -> Iterable[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        yield from reader


def load_people(path: Path) -> Dict[int, Dict[str, str]]:
    people: Dict[int, Dict[str, str]] = {}
    for row in read_csv(path):
        try:
            pid = int(row["people_id"])
        except ValueError:
            continue
        people[pid] = row
    return people


def ensure_target_id(people: Dict[int, Dict[str, str]], name: str) -> Optional[int]:
    for pid, row in people.items():
        if row.get("name", "").strip() == name:
            return pid
    return None


def party_label(people_row: Dict[str, str]) -> str:
    party = (people_row or {}).get("party", "")
    return PARTY_LABELS.get(party, "Other")


def load_bills(path: Path) -> Dict[int, Dict[str, str]]:
    bills: Dict[int, Dict[str, str]] = {}
    for row in read_csv(path):
        try:
            bid = int(row["bill_id"])
        except ValueError:
            continue
        bills[bid] = row
    return bills


def load_rollcalls(path: Path) -> Dict[int, Dict[str, str]]:
    rollcalls: Dict[int, Dict[str, str]] = {}
    for row in read_csv(path):
        try:
            rcid = int(row["roll_call_id"])
        except ValueError:
            continue
        rollcalls[rcid] = row
    return rollcalls


def find_session_csv_dirs(base_dir: Path) -> List[Path]:
    base_path = Path(base_dir)
    if not base_path.exists():
        raise FileNotFoundError(f"No LegiScan data directory found at {base_path}")

    csv_dirs = [
        path
        for path in base_path.rglob("csv")
        if path.is_dir()
        and all((path / filename).exists() for filename in REQUIRED_SESSION_FILES)
    ]
    return sorted(csv_dirs)


def _normalize_base_dirs(base_dirs: BaseDirsInput) -> List[Path]:
    if isinstance(base_dirs, (str, Path)):
        return [Path(base_dirs)]
    return [Path(path) for path in base_dirs]


def gather_session_csv_dirs(base_dirs: BaseDirsInput) -> List[Path]:
    directories: List[Path] = []
    for base_dir in _normalize_base_dirs(base_dirs):
        directories.extend(find_session_csv_dirs(base_dir))
    seen = set()
    unique_dirs: List[Path] = []
    for path in directories:
        resolved = Path(path).resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_dirs.append(path)
    return sorted(unique_dirs)


def detect_state_code(csv_dir: Path) -> Optional[str]:
    bills_path = csv_dir / "bills.csv"
    if not bills_path.exists():
        return None
    for row in read_csv(bills_path):
        state = (row.get("state") or "").strip()
        if state:
            return state.upper()
    return None


def ensure_single_state(csv_dirs: Iterable[Path]) -> Optional[str]:
    states = {
        state for state in (detect_state_code(csv_dir) for csv_dir in csv_dirs) if state
    }
    if len(states) > 1:
        states_list = ", ".join(sorted(states))
        raise ValueError(f"Multiple states detected in dataset: {states_list}")
    return next(iter(states), None)


def determine_dataset_state(base_dirs: BaseDirsInput) -> Optional[str]:
    csv_dirs = gather_session_csv_dirs(base_dirs)
    return ensure_single_state(csv_dirs)


def collect_legislator_names(base_dirs: BaseDirsInput) -> List[str]:
    """Return sorted unique legislator names present in the dataset."""
    csv_dirs = gather_session_csv_dirs(base_dirs)
    ensure_single_state(csv_dirs)
    names = set()
    for csv_dir in csv_dirs:
        people_path = csv_dir / "people.csv"
        for row in read_csv(people_path):
            name = (row.get("name") or "").strip()
            if name:
                names.add(name)
    return sorted(names)


def aggregate_votes(csv_dir: Path, target_id: int, people: Dict[int, Dict[str, str]]):
    counts: Dict[int, Dict[str, Dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: {bucket: 0 for bucket in VOTE_BUCKETS})
    )
    target_votes: Dict[int, Dict[str, str]] = {}

    votes_path = csv_dir / "votes.csv"
    for row in read_csv(votes_path):
        try:
            rcid = int(row["roll_call_id"])
            pid = int(row["people_id"])
        except ValueError:
            continue
        party = party_label(people.get(pid))
        bucket = classify_vote(row.get("vote_desc", ""))
        counts[rcid][party][bucket] += 1
        counts[rcid]["Total"][bucket] += 1
        if pid == target_id:
            target_votes[rcid] = row
    return counts, target_votes


def collect_person_vote_map(base_dirs: BaseDirsInput, person_name: str) -> Dict[int, Dict[str, str]]:
    """Return mapping of roll_call_id to vote details for the given person."""
    votes: Dict[int, Dict[str, str]] = {}

    csv_dirs = gather_session_csv_dirs(base_dirs)
    ensure_single_state(csv_dirs)

    for csv_dir in csv_dirs:
        people = load_people(csv_dir / "people.csv")
        person_id = ensure_target_id(people, person_name)
        if person_id is None:
            continue

        votes_path = csv_dir / "votes.csv"
        for row in read_csv(votes_path):
            try:
                rcid = int(row["roll_call_id"])
                pid = int(row["people_id"])
            except (KeyError, ValueError, TypeError):
                continue
            if pid != person_id:
                continue
            vote_desc = row.get("vote_desc", "")
            votes[rcid] = {
                "vote_desc": vote_desc,
                "vote_bucket": classify_vote(vote_desc),
            }
    return votes


def collect_vote_rows(base_dirs: BaseDirsInput, target_name: str) -> List[List]:
    rows: List[List] = []
    found_target = False

    csv_dirs = gather_session_csv_dirs(base_dirs)
    ensure_single_state(csv_dirs)

    for csv_dir in csv_dirs:
        people = load_people(csv_dir / "people.csv")
        session_target_id = ensure_target_id(people, target_name)
        if session_target_id is None:
            continue
        found_target = True
        target_party = party_label(people.get(session_target_id))

        bills = load_bills(csv_dir / "bills.csv")
        rollcalls = load_rollcalls(csv_dir / "rollcalls.csv")
        counts, target_votes = aggregate_votes(csv_dir, session_target_id, people)

        for rcid, vote in target_votes.items():
            roll = rollcalls.get(rcid)
            if not roll:
                continue
            bill_id_value = roll.get("bill_id")
            try:
                bill_lookup_id = int(bill_id_value)
            except (TypeError, ValueError):
                continue
            bill = bills.get(bill_lookup_id)
            if not bill:
                continue
            session_id = bill.get("session_id", "")
            bill_number = bill.get("bill_number", "")
            bill_id_raw = bill.get("bill_id", bill_id_value)
            bill_id = str(bill_id_raw).strip() if bill_id_raw is not None else ""
            if not bill_id:
                bill_id = str(bill_id_value or "").strip()
            if not bill_id:
                bill_id = f"{session_id}-{bill_number}".strip("-")

            bill_title = (bill.get("title") or "").strip()
            bill_desc = (bill.get("description") or "").strip()
            bill_motion = bill_desc or bill_title or str(bill.get("bill_number", "")).strip()
            bill_url = bill.get("state_link") or bill.get("url") or ""
            vote_desc = vote.get("vote_desc", "")
            vote_bucket = classify_vote(vote_desc)
            vote_date_value = (roll.get("date") or "").strip()
            date_serial = ""
            if vote_date_value:
                try:
                    date_serial = dt.datetime.strptime(vote_date_value, "%Y-%m-%d").strftime("%m/%d/%Y")
                except ValueError:
                    date_serial = vote_date_value
            status_code = (bill.get("status") or "").strip()
            status_desc = (bill.get("status_desc") or "").strip()
            status_date = (bill.get("status_date") or "").strip()
            result = 1 if status_code == "4" or status_desc.lower() == "passed" else 0

            chamber = roll.get("chamber", "")

            roll_desc_raw = (roll.get("description", "") or "").strip()
            try:
                yea = int(roll.get("yea", 0))
            except (TypeError, ValueError):
                yea = roll.get("yea", "") or "0"
            try:
                nay = int(roll.get("nay", 0))
            except (TypeError, ValueError):
                nay = roll.get("nay", "") or "0"
            roll_suffix = f" ({yea}-Y {nay}-N)"
            roll_desc = f"{roll_desc_raw} {roll_suffix}".strip() if roll_desc_raw else roll_suffix

            committee_id = str(roll.get("committee_id") or "").strip()
            committee_name = (roll.get("committee") or "").strip()
            last_action_date = (bill.get("last_action_date") or "").strip()
            last_action = (bill.get("last_action") or "").strip()

            party_counts = counts.get(rcid, {})

            def bucket_value(party_label: str, bucket: str) -> int:
                return party_counts.get(party_label, {}).get(bucket, 0)

            row = [
                chamber,
                session_id,
                bill_number,
                bill_id,
                bill_motion,
                bill_url,
                bill_title,
                bill_desc,
                roll_desc,
                committee_id,
                committee_name,
                last_action_date,
                last_action,
                status_code,
                status_desc,
                status_date,
                rcid,
                target_name,
                target_party,
                vote_desc,
                vote_bucket,
                date_serial,
                result,
            ]

            for party in ("Democrat", "Republican", "Other", "Total"):
                for bucket in VOTE_BUCKETS:
                    row.append(bucket_value(party, bucket))
                row.append(
                    sum(bucket_value(party, bucket) for bucket in VOTE_BUCKETS)
                )

            rows.append(row)

    if not found_target:
        raise ValueError(f"No vote records found for {target_name}.")

    date_idx = WORKBOOK_HEADERS.index("Date")
    session_idx = WORKBOOK_HEADERS.index("Session")
    bill_idx = WORKBOOK_HEADERS.index("Bill Number")
    rows.sort(key=lambda r: (r[date_idx], r[session_idx], r[bill_idx]))
    return rows


def write_workbook(
    rows: List[List], target_name: str, output: Union[Path, IO[bytes]]
):
    wb = Workbook()
    ws = wb.active
    ws.title = target_name
    ws.append(WORKBOOK_HEADERS)
    for row in rows:
        ws.append(row)
    save_target: Union[str, IO[bytes]]
    if isinstance(output, Path):
        save_target = str(output)
    else:
        save_target = output
    wb.save(save_target)


def generate_vote_export(base_dirs: BaseDirsInput, target_name: str, output_path: Path) -> int:
    rows = collect_vote_rows(base_dirs, target_name)
    if not rows:
        raise ValueError(f"No vote records found for {target_name}.")
    write_workbook(rows, target_name, output_path)
    return len(rows)


def main():
    try:
        row_count = generate_vote_export(BASE_DIR, TARGET_NAME, OUTPUT_FILE)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    print(f"Wrote {row_count} rows to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
