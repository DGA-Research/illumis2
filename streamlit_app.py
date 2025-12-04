"""Primary Streamlit entry point.

This script simply imports the JSON-focused interface so that both local runs
and Streamlit Cloud (which auto-detects `streamlit_app.py`) launch the
`streamlit_app_json` experience.
"""

import streamlit_app_json  # noqa: F401  (import side-effects launch the app)

