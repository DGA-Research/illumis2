import datetime as dt
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import pandas as pd

from generate_kristin_robbins_votes import (
    VOTE_BUCKETS,
    WORKBOOK_HEADERS,
    classify_vote,
    write_workbook,
)
from json_legiscan_loader import (
    determine_json_state,
    extract_archives,
    gather_json_session_dirs,
    iter_roll_calls,
    load_bill_map,
    load_people_map,
)

STATUS_LABELS = {
    0: "Unknown",
    1: "Introduced",
    2: "Engrossed",
    3: "Enrolled",
    4: "Passed",
    5: "Vetoed",
    6: "Failed",
    7: "Override",
    8: "Chaptered",
    9: "Dead",
}

PARTY_ORDER = ("Democrat", "Republican", "Other", "Total")


def _format_us_date(date_str: str | None) -> str:
    if not date_str:
        return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(date_str, fmt).strftime("%m/%d/%Y")
        except ValueError:
            continue
    return date_str


def _status_description(status_code: int | None) -> str:
    if status_code is None:
        return ""
    return STATUS_LABELS.get(int(status_code), str(status_code))


def _normalize_party(code: str | None) -> str:
    token = (code or "").strip().upper()
    if token in {"D", "DEM", "DFL"}:
        return "Democrat"
    if token in {"R", "REP", "GOP"}:
        return "Republican"
    return "Other"


def _format_roll_details(roll_call: dict) -> str:
    desc = (roll_call.get("desc") or "").strip()
    yea = roll_call.get("yea") or 0
    nay = roll_call.get("nay") or 0
    suffix = f" ({yea}-Y {nay}-N)"
    return f"{desc}{suffix}" if desc else suffix.strip()


def _build_count_template() -> Dict[str, Dict[str, int]]:
    return {party: {bucket: 0 for bucket in VOTE_BUCKETS} for party in PARTY_ORDER}


def _tally_votes(votes: Iterable[dict], people_map: Dict[int, dict]) -> Dict[str, Dict[str, int]]:
    counts = _build_count_template()
    for record in votes or []:
        people_id = record.get("people_id")
        vote_text = record.get("vote_text", "")
        bucket = classify_vote(vote_text)
        if bucket not in VOTE_BUCKETS:
            bucket = "Not"
        try:
            person = people_map.get(int(people_id)) or {}
        except (TypeError, ValueError):
            person = {}
        party_label = _normalize_party(person.get("party"))
        counts[party_label][bucket] += 1
        counts["Total"][bucket] += 1

    for party in PARTY_ORDER:
        counts[party]["Total"] = sum(counts[party][bucket] for bucket in VOTE_BUCKETS)
    return counts


def _latest_history_entry(history: List[dict]) -> Tuple[str, str]:
    if not history:
        return "", ""
    latest = history[-1]
    return (
        (latest.get("action") or "").strip(),
        _format_us_date(latest.get("date")),
    )


def extract_crossfile_fields(bill: dict) -> Tuple[str, str]:
    entries = bill.get("sasts") or []
    if not isinstance(entries, list):
        return "", ""
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        entry_type = (entry.get("type") or "").strip().lower()
        type_id = entry.get("type_id")
        if entry_type == "crossfiled" or (isinstance(type_id, int) and type_id == 5):
            crossfile_id = entry.get("sast_bill_id") or entry.get("bill_id") or ""
            crossfile_number = entry.get("sast_bill_number") or entry.get("bill_number") or ""
            crossfile_id = str(crossfile_id).strip() if crossfile_id is not None else ""
            crossfile_number = str(crossfile_number).strip() if crossfile_number is not None else ""
            return crossfile_id, crossfile_number
    return "", ""


def _build_row(
    roll_call: dict,
    vote_record: dict,
    person: dict,
    bill: dict,
    counts: Dict[str, Dict[str, int]],
) -> List:
    bill_id = bill.get("bill_id")
    session_meta = bill.get("session") or {}
    session_label = session_meta.get("session_name") or bill.get("session_id") or ""
    committee_info = bill.get("committee") or {}
    last_action, last_action_date = _latest_history_entry(bill.get("history") or [])
    status_code = bill.get("status")
    status_desc = _status_description(status_code)
    status_date = _format_us_date(bill.get("status_date"))

    roll_details = _format_roll_details(roll_call)
    chamber = (roll_call.get("chamber") or "").title()
    vote_text = vote_record.get("vote_text", "")
    vote_bucket = classify_vote(vote_text)
    roll_date = _format_us_date(roll_call.get("date"))
    result = 1 if roll_call.get("passed") else 0

    bill_title = bill.get("title") or ""
    bill_description = bill.get("description") or ""
    bill_motion = bill_description or bill_title or bill.get("bill_number") or ""
    crossfile_id, crossfile_number = extract_crossfile_fields(bill)

    row_values: Dict[str, object] = {
        "Chamber": chamber,
        "Session": session_label,
        "Bill Number": bill.get("bill_number") or "",
        "Bill ID": bill_id or "",
        "Cross-file Bill ID": crossfile_id,
        "Cross-file Bill Number": crossfile_number,
        "Bill Motion": bill_motion,
        "URL": bill.get("state_link") or bill.get("url") or "",
        "Bill Title": bill_title,
        "Bill Description": bill_description,
        "Roll Details": roll_details,
        "Committee ID": committee_info.get("committee_id") or "",
        "Committee": committee_info.get("name") or "",
        "Last Action Date": last_action_date,
        "Last Action": last_action,
        "Status": status_code or "",
        "Status Description": status_desc,
        "Status Date": status_date,
        "Roll Call ID": roll_call.get("roll_call_id") or "",
        "Person": person.get("name") or "",
        "Person Party": _normalize_party(person.get("party")),
        "Vote": vote_text,
        "Vote Bucket": vote_bucket,
        "Date": roll_date,
        "Result": result,
    }

    for party in PARTY_ORDER:
        for bucket in VOTE_BUCKETS:
            row_values[f"{party}_{bucket}"] = counts[party].get(bucket, 0)
        row_values[f"{party}_Total"] = counts[party].get("Total", 0)

    return [row_values.get(header, "") for header in WORKBOOK_HEADERS]


def collect_vote_rows_from_json(session_dirs: Sequence[Path], target_name: str) -> List[List]:
    normalized_target = target_name.strip()
    rows: List[List] = []
    found_target = False

    for session_dir in session_dirs:
        people_map = load_people_map(session_dir)
        matching_people = [
            person for person in people_map.values()
            if (person.get("name") or "").strip() == normalized_target
        ]
        if not matching_people:
            continue

        found_target = True
        bill_map = load_bill_map(session_dir)

        for roll_call in iter_roll_calls(session_dir):
            vote_lookup: Dict[int, dict] = {}
            for record in roll_call.get("votes") or []:
                people_id = record.get("people_id")
                if people_id is None:
                    continue
                try:
                    vote_lookup[int(people_id)] = record
                except (TypeError, ValueError):
                    continue

            bill_id = roll_call.get("bill_id")
            bill = bill_map.get(int(bill_id)) if bill_id is not None else None
            if not bill:
                continue

            counts = _tally_votes(roll_call.get("votes") or [], people_map)

            for person in matching_people:
                try:
                    person_id = int(person.get("people_id"))
                except (TypeError, ValueError):
                    continue
                vote_record = vote_lookup.get(person_id)
                if not vote_record:
                    continue
                rows.append(_build_row(roll_call, vote_record, person, bill, counts))

    if not found_target or not rows:
        raise ValueError(f"No vote records found for {target_name}.")

    date_idx = WORKBOOK_HEADERS.index("Date")
    session_idx = WORKBOOK_HEADERS.index("Session")
    bill_idx = WORKBOOK_HEADERS.index("Bill Number")
    rows.sort(key=lambda r: (r[date_idx], r[session_idx], r[bill_idx]))
    return rows


def build_summary_dataframe_from_json(session_dirs: Sequence[Path], legislator_name: str):
    rows = collect_vote_rows_from_json(session_dirs, legislator_name)
    df = pd.DataFrame(rows, columns=WORKBOOK_HEADERS)
    df["Date_dt"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Year"] = df["Date_dt"].dt.year.astype("Int64")
    return df


def generate_vote_export_from_json(
    archive_paths: Sequence[Path],
    legislator_name: str,
    output_path: Path,
) -> int:
    extracted = extract_archives(archive_paths)
    try:
        base_dirs = [item.base_path for item in extracted]
        session_dirs = gather_json_session_dirs(base_dirs)
        determine_json_state(session_dirs)
        rows = collect_vote_rows_from_json(session_dirs, legislator_name)
        write_workbook(rows, legislator_name, output_path)
        return len(rows)
    finally:
        for item in extracted:
            item.cleanup()
