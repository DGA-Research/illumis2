import io
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st
from openpyxl import Workbook

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
FOCUS_PARTY_LOOKUP = {
    "Legislator's Party": None,
    "Democrat": "Democrat",
    "Republican": "Republican",
    "Independent": "Other",
}
FILTER_OPTIONS = [
    "All Votes",
    "Votes Against Party",
    "Minority Votes",
    "Deciding Votes",
    "Skipped Votes",
    "Search By Term",
]
WORKBOOK_VIEWS = [
    "All Votes",
    "Votes Against Party",
    "Minority Votes",
    "Deciding Votes",
    "Skipped Votes",
]


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


def _sanitize_sheet_title(title: str, used_titles: set[str]) -> str:
    cleaned = "".join(ch if ch not in '[]:*?/\\' else "_" for ch in title).strip() or "Sheet"
    cleaned = cleaned[:31]
    base = cleaned
    counter = 1
    while cleaned in used_titles:
        suffix = f"_{counter}"
        if len(base) + len(suffix) > 31:
            cleaned = base[: 31 - len(suffix)] + suffix
        else:
            cleaned = base + suffix
        counter += 1
    used_titles.add(cleaned)
    return cleaned


def write_multi_sheet_workbook(
    sheet_specs: List[Tuple[str, List[str], List[List]]],
    output: io.BytesIO,
) -> None:
    wb = Workbook()
    used_titles: set[str] = set()
    first_sheet = True
    for sheet_name, headers, rows in sheet_specs:
        title = _sanitize_sheet_title(sheet_name, used_titles)
        if first_sheet:
            ws = wb.active
            ws.title = title
            first_sheet = False
        else:
            ws = wb.create_sheet(title=title)
        ws.append(headers)
        for row in rows:
            ws.append(row)
    wb.save(output)


def safe_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _apply_deciding_vote_filter(df: pd.DataFrame, max_vote_diff: int) -> pd.Series:
    total_for = df["Total_For"].apply(safe_int)
    total_against = df["Total_Against"].apply(safe_int)
    vote_diff = (total_for - total_against).abs()
    winning_bucket = pd.Series("Tie", index=df.index, dtype="object")
    winning_bucket = winning_bucket.mask(total_for > total_against, "For")
    winning_bucket = winning_bucket.mask(total_against > total_for, "Against")
    df["Vote Difference"] = vote_diff
    df["Winning Bucket"] = winning_bucket
    return (
        (vote_diff <= max_vote_diff)
        & winning_bucket.isin(["For", "Against"])
        & (df["Vote Bucket"] == winning_bucket)
    )


def apply_filters_json(
    summary_df: pd.DataFrame,
    *,
    filter_mode: str,
    search_term: str = "",
    year_selection: Optional[List[int]] = None,
    party_focus_option: str = "Legislator's Party",
    minority_percent: int = 20,
    min_group_votes: int = 0,
    max_vote_diff: int = 5,
) -> Tuple[pd.DataFrame, int]:
    df = summary_df.copy()

    if year_selection:
        df = df[df["Year"].isin(year_selection)].copy()

    if search_term:
        description_mask = df["Bill Description"].astype(str).str.contains(
            search_term, case=False, na=False
        )
        df = df[description_mask].copy()

    if df.empty:
        raise ValueError("No vote records found for the selected criteria.")

    if filter_mode == "Search By Term" and not search_term:
        raise ValueError("Enter a search term to use the 'Search By Term' vote type.")

    if filter_mode == "Skipped Votes":
        vote_text = df["Vote"].astype(str).str.strip().str.lower()
        skip_mask = ~(
            vote_text.str.startswith("yea")
            | vote_text.str.startswith("nay")
            | vote_text.str.startswith("aye")
        )
        df = df[skip_mask].copy()
        if df.empty:
            raise ValueError("No skipped votes found for the selected criteria.")

    df["Roll Call ID"] = pd.to_numeric(df["Roll Call ID"], errors="coerce").astype("Int64")

    def calc_metrics(row: pd.Series):
        bucket = row["Vote Bucket"]
        party = row["Person Party"]
        metrics = {
            "party_bucket_votes": None,
            "party_total_votes": None,
            "party_share": None,
            "chamber_bucket_votes": None,
            "chamber_total_votes": None,
            "chamber_share": None,
        }

        if party:
            party_bucket_col = f"{party}_{bucket}"
            party_total_col = f"{party}_Total"
            party_bucket = safe_int(row.get(party_bucket_col))
            party_total = safe_int(row.get(party_total_col))
            metrics["party_bucket_votes"] = party_bucket
            metrics["party_total_votes"] = party_total
            metrics["party_share"] = party_bucket / party_total if party_total else None

        chamber_bucket = safe_int(row.get(f"Total_{bucket}"))
        chamber_total = safe_int(row.get("Total_Total"))
        metrics["chamber_bucket_votes"] = chamber_bucket
        metrics["chamber_total_votes"] = chamber_total
        metrics["chamber_share"] = chamber_bucket / chamber_total if chamber_total else None
        return pd.Series(metrics)

    metrics_df = df.apply(calc_metrics, axis=1)
    df = pd.concat([df, metrics_df], axis=1)

    df["focus_party_label"] = df["Person Party"]
    df["focus_party_bucket_votes"] = df["party_bucket_votes"]
    df["focus_party_total_votes"] = df["party_total_votes"]
    df["focus_party_share"] = df["party_share"]

    focus_party_key = FOCUS_PARTY_LOOKUP.get(party_focus_option)
    if filter_mode == "Votes Against Party" and focus_party_key:
        focus_display_label = (
            "Independent" if focus_party_key == "Other" else party_focus_option
        )

        def calc_focus_metrics(row: pd.Series):
            bucket = row["Vote Bucket"]
            bucket_votes = safe_int(row.get(f"{focus_party_key}_{bucket}"))
            total_votes = safe_int(row.get(f"{focus_party_key}_Total"))
            share = bucket_votes / total_votes if total_votes else None
            return pd.Series(
                {
                    "focus_party_label": focus_display_label,
                    "focus_party_bucket_votes": bucket_votes,
                    "focus_party_total_votes": total_votes,
                    "focus_party_share": share,
                }
            )

        focus_metrics = df.apply(calc_focus_metrics, axis=1)
        df[
            [
                "focus_party_label",
                "focus_party_bucket_votes",
                "focus_party_total_votes",
                "focus_party_share",
            ]
        ] = focus_metrics

    deciding_condition = None
    if filter_mode == "Deciding Votes":
        deciding_condition = _apply_deciding_vote_filter(df, max_vote_diff)

    apply_party_filter = filter_mode in {"Votes Against Party", "Minority Votes"}
    apply_chamber_filter = filter_mode == "Minority Votes"
    threshold_ratio = (
        minority_percent / 100.0 if (apply_party_filter or apply_chamber_filter) else None
    )
    min_votes = min_group_votes if (apply_party_filter or apply_chamber_filter) else 0

    filters = []
    if apply_party_filter:
        party_condition = (
            df["focus_party_share"].notna()
            & (df["focus_party_total_votes"] >= min_votes)
            & (df["focus_party_share"] <= threshold_ratio)
        )
        filters.append(party_condition)
    if apply_chamber_filter:
        chamber_condition = (
            df["chamber_share"].notna()
            & (df["chamber_total_votes"] >= min_votes)
            & (df["chamber_share"] <= threshold_ratio)
        )
        filters.append(chamber_condition)
    if filter_mode == "Deciding Votes" and deciding_condition is not None:
        filters.append(deciding_condition)

    pre_filter_count = len(df)

    if filters:
        mask = filters[0]
        for condition in filters[1:]:
            mask &= condition
        filtered_df = df[mask].copy()
    else:
        filtered_df = df.copy()

    dedupe_keys = [col for col in ["Roll Call ID", "Person"] if col in filtered_df.columns]
    if dedupe_keys:
        filtered_df = filtered_df.drop_duplicates(subset=dedupe_keys).reset_index(drop=True)

    if filtered_df.empty:
        if filter_mode == "Skipped Votes":
            raise ValueError("No skipped votes found for the selected criteria.")
        if filter_mode == "Votes Against Party":
            raise ValueError(
                "No votes found where the legislator sided with the specified minority."
            )
        if filter_mode == "Minority Votes":
            raise ValueError(
                "No votes found where the legislator and chamber were both in the minority."
            )
        if filter_mode == "Deciding Votes":
            raise ValueError(
                "No votes found within the specified deciding vote margin."
            )
        if filter_mode == "Search By Term":
            raise ValueError(
                "No vote records found matching the provided search term."
            )
        raise ValueError("No vote records found for the selected criteria.")

    return filtered_df, pre_filter_count


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

    selected_legislator = st.sidebar.selectbox(
        "Legislator",
        legislator_names,
        index=0,
        key="json_legislator_select",
    )

    filter_mode = st.sidebar.selectbox(
        "Vote type",
        options=FILTER_OPTIONS,
        index=0,
        key="json_filter_mode",
        help="Choose a predefined view of the legislator's voting record.",
    )
    search_term = st.sidebar.text_input(
        "Search term (bill description)",
        value="",
        key="json_search_term",
    )

    party_focus_option = "Legislator's Party"
    minority_percent = 20
    min_group_votes = 0
    max_vote_diff = 5

    if filter_mode == "Votes Against Party":
        party_focus_option = st.sidebar.selectbox(
            "Party voting against",
            options=list(FOCUS_PARTY_LOOKUP.keys()),
            index=0,
            key="json_party_focus",
            help="Choose which party's vote breakdown to compare against.",
        )
        minority_percent = st.sidebar.slider(
            "Minority threshold (%)",
            min_value=0,
            max_value=100,
            value=20,
            key="json_votes_against_threshold",
            help="Keep votes where the selected party supported the legislator at or below this percentage.",
        )
        min_group_votes = st.sidebar.slider(
            "Minimum votes in group",
            min_value=0,
            max_value=200,
            value=5,
            key="json_votes_against_min_votes",
            help="Ignore records where the compared party cast fewer total votes than this threshold.",
        )
        st.sidebar.caption("Shows votes where the legislator sided with a minority of the chosen party.")
    elif filter_mode == "Minority Votes":
        minority_percent = st.sidebar.slider(
            "Minority threshold (%)",
            min_value=0,
            max_value=100,
            value=20,
            key="json_minority_threshold",
            help="Keep votes where the legislator's party supported their position at or below this percentage.",
        )
        min_group_votes = st.sidebar.slider(
            "Minimum votes in group",
            min_value=0,
            max_value=200,
            value=5,
            key="json_minority_min_votes",
            help="Ignore records where the compared group cast fewer total votes than this threshold.",
        )
        st.sidebar.caption("Shows votes where the legislator and chamber were both in the minority.")
    elif filter_mode == "Deciding Votes":
        max_vote_diff = st.sidebar.slider(
            "Maximum votes difference",
            min_value=1,
            max_value=50,
            value=5,
            key="json_deciding_max_diff",
            help="Limit to votes where the margin between Yeas and Nays is within this amount.",
        )
        st.sidebar.caption("Shows votes where the legislator's side prevailed by the specified margin or less.")
    elif filter_mode == "Search By Term":
        st.sidebar.caption("Shows votes where the bill description matches the search term.")
    elif filter_mode == "Skipped Votes":
        st.sidebar.caption("Shows votes where the legislator did not cast a Yea or Nay.")

    control_cols = st.columns(2)
    with control_cols[0]:
        generate_summary = st.button("Generate summary", type="primary")
    with control_cols[1]:
        generate_workbook_clicked = st.button("Generate all views workbook")

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

    year_options = sorted(
        int(year)
        for year in summary_df["Year"].dropna().unique().tolist()
        if pd.notna(year)
    )
    st.session_state["json_year_options"] = year_options
    stored_years = st.session_state.get("json_year_selection", [])
    stored_years = [year for year in stored_years if year in year_options]
    if not stored_years and year_options:
        stored_years = year_options
    year_selection = st.sidebar.multiselect(
        "Year",
        options=year_options,
        default=stored_years,
        key="json_year_selection_widget",
        help="Restrict votes to selected calendar years.",
        disabled=not year_options,
    )
    st.session_state["json_year_selection"] = year_selection

    try:
        filtered_df, total_count = apply_filters_json(
            summary_df,
            filter_mode=filter_mode,
            search_term=search_term,
            year_selection=year_selection,
            party_focus_option=party_focus_option,
            minority_percent=minority_percent,
            min_group_votes=min_group_votes,
            max_vote_diff=max_vote_diff,
        )
    except ValueError as exc:
        st.warning(str(exc))
        st.stop()

    filtered_count = len(filtered_df)
    st.success(
        f"Compiled {total_count} votes for {legislator}. "
        f"Showing {filtered_count} after filters."
    )

    display_df = filtered_df.copy()
    if "Date_dt" in display_df.columns:
        display_df["Date"] = display_df["Date_dt"].dt.date
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
    st.dataframe(display_df[display_columns], width="stretch", height=400)

    export_df = filtered_df.copy()
    if "Date_dt" in export_df.columns:
        export_df = export_df.drop(columns=["Date_dt"])
    excel_rows = (
        export_df.reindex(columns=WORKBOOK_HEADERS)
        .fillna("")
        .values
        .tolist()
    )
    excel_buffer = io.BytesIO()
    write_workbook(excel_rows, legislator, excel_buffer)
    st.download_button(
        label="Download filtered Excel sheet",
        data=excel_buffer.getvalue(),
        file_name=f"{legislator.replace(' ', '_')}_JSON_Votes.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    if generate_workbook_clicked:
        workbook_views: List[Tuple[str, List[str], List[List]]] = []
        empty_views: List[str] = []
        stored_party_focus = st.session_state.get("json_party_focus", party_focus_option)
        stored_votes_against_threshold = st.session_state.get("json_votes_against_threshold", 20)
        stored_votes_against_min_votes = st.session_state.get("json_votes_against_min_votes", 5)
        stored_minority_threshold = st.session_state.get("json_minority_threshold", 20)
        stored_minority_min_votes = st.session_state.get("json_minority_min_votes", 5)
        stored_deciding_max_diff = st.session_state.get("json_deciding_max_diff", 5)

        base_params = {
            "search_term": "",
            "year_selection": year_selection or None,
            "party_focus_option": "Legislator's Party",
            "minority_percent": 20,
            "min_group_votes": 0,
            "max_vote_diff": 5,
        }

        for view_name in WORKBOOK_VIEWS:
            params = base_params.copy()
            if view_name == "Votes Against Party":
                params.update(
                    {
                        "party_focus_option": stored_party_focus,
                        "minority_percent": stored_votes_against_threshold,
                        "min_group_votes": stored_votes_against_min_votes,
                    }
                )
            elif view_name == "Minority Votes":
                params.update(
                    {
                        "minority_percent": stored_minority_threshold,
                        "min_group_votes": stored_minority_min_votes,
                    }
                )
            elif view_name == "Deciding Votes":
                params.update({"max_vote_diff": stored_deciding_max_diff})
            try:
                sheet_df, _ = apply_filters_json(
                    summary_df,
                    filter_mode=view_name,
                    **params,
                )
            except ValueError:
                empty_views.append(view_name)
                continue
            export_sheet = sheet_df.copy()
            export_sheet = export_sheet.drop(columns=["Date_dt"], errors="ignore")
            sheet_rows = (
                export_sheet.reindex(columns=WORKBOOK_HEADERS)
                .fillna("")
                .values
                .tolist()
            )
            workbook_views.append((view_name, WORKBOOK_HEADERS, sheet_rows))

        if not workbook_views:
            st.warning("No data available for the selected workbook views.")
        else:
            workbook_buffer = io.BytesIO()
            write_multi_sheet_workbook(workbook_views, workbook_buffer)
            workbook_buffer.seek(0)
            st.download_button(
                label="Download vote summary workbook",
                data=workbook_buffer.getvalue(),
                file_name=f"{legislator.replace(' ', '_')}_JSON_full_workbook.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="json_full_workbook_download",
            )
            if empty_views:
                st.info("No data available for: " + ", ".join(empty_views))


if __name__ == "__main__":
    main()
