import io
from contextlib import ExitStack
from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st

from generate_kristin_robbins_votes import WORKBOOK_HEADERS, write_workbook
from json_legiscan_loader import (
    collect_legislator_names_json,
    extract_archives,
    gather_json_session_dirs,
)
from json_vote_builder import collect_vote_rows_from_json

JSON_DATA_DIR = Path(__file__).resolve().parent / "JSON DATA"
SESSION_CACHE_KEY = "json_vote_summary"


def _resolve_archives(selected_names: List[str]) -> List[Path]:
    lookup = {path.name: path for path in JSON_DATA_DIR.glob("*.zip")}
    missing = [name for name in selected_names if name not in lookup]
    if missing:
        raise FileNotFoundError(f"Missing archive(s): {', '.join(missing)}")
    return [lookup[name] for name in selected_names]


def _load_legislator_names(selected_paths: List[Path]) -> List[str]:
    extracted = extract_archives(selected_paths)
    try:
        base_dirs = [item.base_path for item in extracted]
        session_dirs = gather_json_session_dirs(base_dirs)
        return collect_legislator_names_json(session_dirs)
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


def _render_archive_picker():
    available_archives = sorted(JSON_DATA_DIR.glob("*.zip"))
    if not available_archives:
        st.error(f"No JSON ZIP archives found in {JSON_DATA_DIR}.")
        st.stop()
    default_selection = [available_archives[0].name]
    return st.multiselect(
        "JSON archives",
        options=[path.name for path in available_archives],
        default=default_selection,
        help="Archives are loaded from the local 'JSON DATA' folder.",
    )


def main() -> None:
    st.set_page_config(page_title="LegiScan JSON Vote Explorer", layout="wide")
    st.title("LegiScan JSON Vote Explorer (JSON Beta)")
    st.caption("Load LegiScan JSON archives, pick a legislator, and download their vote history.")

    selected_archive_names = _render_archive_picker()
    if not selected_archive_names:
        st.info("Select at least one JSON archive to continue.")
        st.stop()

    try:
        selected_paths = _resolve_archives(selected_archive_names)
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.stop()

    with st.spinner("Discovering legislators..."):
        try:
            legislator_names = _load_legislator_names(selected_paths)
        except Exception as exc:
            st.error(f"Failed to read archives: {exc}")
            st.stop()

    if not legislator_names:
        st.warning("No legislators found in the selected archives.")
        st.stop()

    selected_legislator = st.selectbox("Legislator", legislator_names)
    generate_summary = st.button("Generate summary", type="primary")

    if generate_summary:
        with st.spinner(f"Compiling votes for {selected_legislator}..."):
            try:
                rows = _build_vote_rows(selected_paths, selected_legislator)
            except Exception as exc:
                st.error(str(exc))
                st.stop()
        summary_df = _prepare_dataframe(rows)
        st.session_state[SESSION_CACHE_KEY] = {
            "rows": rows,
            "df": summary_df,
            "legislator": selected_legislator,
        }

    cached = st.session_state.get(SESSION_CACHE_KEY)
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
