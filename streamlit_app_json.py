import io
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st

from generate_kristin_robbins_votes import WORKBOOK_HEADERS, write_workbook
from json_legiscan_loader import (
    collect_legislator_names_json,
    determine_json_state,
    extract_archives,
    gather_json_session_dirs,
)
from json_vote_builder import collect_vote_rows_from_json

JSON_DATA_DIR = Path(__file__).resolve().parent / "JSON DATA"
SESSION_CACHE_KEY = "json_vote_summary"
ALL_STATES_LABEL = "All States"
STATE_CHOICES = [
    ("Alabama", "AL"),
    ("Alaska", "AK"),
    ("Arizona", "AZ"),
    ("Arkansas", "AR"),
    ("California", "CA"),
    ("Colorado", "CO"),
    ("Connecticut", "CT"),
    ("Delaware", "DE"),
    ("Florida", "FL"),
    ("Georgia", "GA"),
    ("Hawaii", "HI"),
    ("Idaho", "ID"),
    ("Illinois", "IL"),
    ("Indiana", "IN"),
    ("Iowa", "IA"),
    ("Kansas", "KS"),
    ("Kentucky", "KY"),
    ("Louisiana", "LA"),
    ("Maine", "ME"),
    ("Maryland", "MD"),
    ("Massachusetts", "MA"),
    ("Michigan", "MI"),
    ("Minnesota", "MN"),
    ("Mississippi", "MS"),
    ("Missouri", "MO"),
    ("Montana", "MT"),
    ("Nebraska", "NE"),
    ("Nevada", "NV"),
    ("New Hampshire", "NH"),
    ("New Jersey", "NJ"),
    ("New Mexico", "NM"),
    ("New York", "NY"),
    ("North Carolina", "NC"),
    ("North Dakota", "ND"),
    ("Ohio", "OH"),
    ("Oklahoma", "OK"),
    ("Oregon", "OR"),
    ("Pennsylvania", "PA"),
    ("Rhode Island", "RI"),
    ("South Carolina", "SC"),
    ("South Dakota", "SD"),
    ("Tennessee", "TN"),
    ("Texas", "TX"),
    ("Utah", "UT"),
    ("Vermont", "VT"),
    ("Virginia", "VA"),
    ("Washington", "WA"),
    ("West Virginia", "WV"),
    ("Wisconsin", "WI"),
    ("Wyoming", "WY"),
]
STATE_NAME_TO_CODE = {name: code for name, code in STATE_CHOICES}
STATE_CODE_TO_NAME = {code: name for name, code in STATE_CHOICES}


def _resolve_archives(selected_names: List[str]) -> List[Path]:
    lookup = {path.name: path for path in JSON_DATA_DIR.glob("*.zip")}
    missing = [name for name in selected_names if name not in lookup]
    if missing:
        raise FileNotFoundError(f"Missing archive(s): {', '.join(missing)}")
    return [lookup[name] for name in selected_names]


def _load_legislator_names(selected_paths: List[Path]) -> Tuple[List[str], Optional[str]]:
    extracted = extract_archives(selected_paths)
    try:
        base_dirs = [item.base_path for item in extracted]
        session_dirs = gather_json_session_dirs(base_dirs)
        dataset_state = determine_json_state(session_dirs)
        names = collect_legislator_names_json(session_dirs)
        return names, dataset_state
    finally:
        for item in extracted:
            item.cleanup()


def _build_vote_rows(selected_paths: List[Path], legislator_name: str) -> List[List]:
    extracted = extract_archives(selected_paths)
    try:
        base_dirs = [item.base_path for item in extracted]
        session_dirs = gather_json_session_dirs(base_dirs)
        return collect_vote_rows_from_json(session_dirs, legislator_name)
    finally:
        for item in extracted:
            item.cleanup()


def _prepare_dataframe(rows: List[List]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=WORKBOOK_HEADERS)
    df["Date_dt"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Year"] = df["Date_dt"].dt.year.astype("Int64")
    return df


def _render_state_filter() -> Tuple[List[str], List[str]]:
    st.sidebar.header("Dataset Selection")
    state_labels = st.sidebar.multiselect(
        "State(s)",
        options=[name for name, _ in STATE_CHOICES],
        default=[],
        help="Choose one or more states to load their JSON archives.",
    )
    state_codes = [STATE_NAME_TO_CODE[label] for label in state_labels]
    return state_labels, state_codes


def _collect_archives_for_states(state_codes: List[str]) -> List[str]:
    available_archives = sorted(JSON_DATA_DIR.glob("*.zip"))
    if not available_archives:
        st.error(f"No JSON ZIP archives found in {JSON_DATA_DIR}.")
        st.stop()

    if not state_codes:
        return []

    selected: List[str] = []
    for path in available_archives:
        code = path.name[:2].upper()
        if code in state_codes:
            selected.append(path.name)
    return selected


def main() -> None:
    st.set_page_config(page_title="LegiScan JSON Vote Explorer", layout="wide")
    st.title("LegiScan JSON Vote Explorer (JSON Beta)")
    st.caption("Load LegiScan JSON archives by state, pick a legislator, and download their vote history.")

    state_labels, state_codes = _render_state_filter()
    if not state_codes:
        st.info("Select at least one state to continue.")
        st.stop()

    selected_archive_names = _collect_archives_for_states(state_codes)
    if not selected_archive_names:
        readable_states = ", ".join(state_labels)
        st.warning(f"No JSON archives found for: {readable_states}.")
        st.stop()

    archives_snapshot = tuple(sorted(selected_archive_names))

    try:
        selected_paths = _resolve_archives(selected_archive_names)
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.stop()

    with st.spinner("Discovering legislators..."):
        try:
            legislator_names, dataset_state = _load_legislator_names(selected_paths)
        except Exception as exc:
            st.error(f"Failed to read archives: {exc}")
            st.stop()

    if not legislator_names:
        st.warning("No legislators found in the selected archives.")
        st.stop()

    if state_labels:
        state_display = ", ".join(state_labels)
    else:
        state_display = (
            STATE_CODE_TO_NAME.get(dataset_state, dataset_state)
            if dataset_state
            else ALL_STATES_LABEL
        )
    st.caption(f"Archive selection: {state_display}")

    selected_legislator = st.selectbox("Legislator", legislator_names)
    generate_summary = st.button("Generate summary", type="primary")

    if generate_summary:
        with st.spinner(f"Compiling votes for {selected_legislator}..."):
            try:
                rows = _build_vote_rows(selected_paths, selected_legislator)
            except Exception as exc:
                st.error(f"Failed to build vote summary: {exc}")
                st.stop()
        summary_df = _prepare_dataframe(rows)
        st.session_state[SESSION_CACHE_KEY] = {
            "rows": rows,
            "df": summary_df,
            "legislator": selected_legislator,
            "archives": archives_snapshot,
        }

    cached = st.session_state.get(SESSION_CACHE_KEY)
    if cached:
        if cached.get("archives") != archives_snapshot or cached.get("legislator") not in legislator_names:
            cached = None

    if not cached:
        st.info("Click **Generate summary** to build the vote dataset.")
        st.stop()

    rows = cached["rows"]
    summary_df = cached["df"]
    legislator = cached["legislator"]

    st.success(f"Compiled {len(summary_df)} votes for {legislator}.")

    display_columns = [
        "Date",
        "Session",
        "Bill Number",
        "Bill Motion",
        "Chamber",
        "Vote",
        "Vote Bucket",
        "Result",
    ]
    st.dataframe(summary_df[display_columns], use_container_width=True, height=400)

    excel_buffer = io.BytesIO()
    write_workbook(rows, legislator, excel_buffer)
    st.download_button(
        label="Download Excel workbook",
        data=excel_buffer.getvalue(),
        file_name=f"{legislator.replace(' ', '_')}_JSON_Votes.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    main()
