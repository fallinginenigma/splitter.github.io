"""
config_profile.py
~~~~~~~~~~~~~~~~~
Save and restore all user-configurable session-state choices to/from a JSON
profile file.  Raw DataFrames (sheets, filtered data, salience results) are
intentionally excluded — those are always re-derived from the uploaded file.

Profile schema version: 1
"""
from __future__ import annotations

import json
import datetime
from typing import Any

PROFILE_VERSION = 1

# ── Keys that are safe to serialise (no DataFrames / large objects) ───────────
SERIALISABLE_KEYS = [
    "sheet_map",
    "col_maps",
    "split_level",
    "bb_split_levels",
    "filters",
    "sas_months_selected",
    "sas_forecast_months_selected",
    "basis_source",
    "basis_mode",
    "basis_months_selected",
    "sfu_basis_sources",
    "sfu_manual_months",
    "sfu_specific_months",
    "sfu_remarks",
    "sal_overrides",
    "data_source",
    "db_host",
    "db_http_path",
    "db_tables",
    "db_row_limit",
    "bb_id_col",
    "run_settings",
    "gbb_actions",
    "gbb_types",
]


# ── ExceptionStore ↔ dict helpers ─────────────────────────────────────────────

def _exc_store_to_dict(exc_store) -> dict:
    """Serialise an ExceptionStore to a plain dict."""
    bb_exc_serial: dict = {}
    for bb_id, exc in exc_store.bb_exceptions.items():
        bb_exc_serial[bb_id] = {
            "include": list(exc.get("include", set())),
            "exclude": list(exc.get("exclude", set())),
            "fixed_qty": exc.get("fixed_qty", {}),
        }
    return {
        "global_exclusions": list(exc_store.global_exclusions),
        "bb_exceptions": bb_exc_serial,
    }


def _exc_store_from_dict(d: dict, exc_store_class) -> Any:
    """Restore an ExceptionStore from a plain dict."""
    store = exc_store_class()
    for sku in d.get("global_exclusions", []):
        store.add_global_exclusion(sku, notes="(loaded from profile)")
    for bb_id, exc in d.get("bb_exceptions", {}).items():
        for sku in exc.get("include", []):
            store.add_bb_include(bb_id, sku, notes="(loaded from profile)")
        for sku in exc.get("exclude", []):
            store.add_bb_exclude(bb_id, sku, notes="(loaded from profile)")
        for sku, months in exc.get("fixed_qty", {}).items():
            for month, qty in months.items():
                store.set_fixed_qty(bb_id, sku, month, float(qty), notes="(loaded from profile)")
    return store


# ── sal_overrides: keys are tuples, JSON only supports string keys ────────────

def _sal_overrides_to_serial(sal_overrides: dict) -> list:
    """Convert {(tuple, sku): float} → [{"group": [...], "sku": str, "value": float}]"""
    result = []
    for (group_tuple, sku), value in sal_overrides.items():
        result.append({
            "group": list(group_tuple) if isinstance(group_tuple, tuple) else [group_tuple],
            "sku": sku,
            "value": float(value),
        })
    return result


def _sal_overrides_from_serial(serial: list) -> dict:
    """Reverse of _sal_overrides_to_serial."""
    result = {}
    for entry in serial:
        group_tuple = tuple(entry["group"])
        sku = entry["sku"]
        value = float(entry["value"])
        result[(group_tuple, sku)] = value
    return result


# ── sfu_basis_sources: values may contain lists under "selected" key ──────────
# These are already JSON-serialisable (str, list), so no special handling needed.


# ── Public API ────────────────────────────────────────────────────────────────

def build_profile(session_state, exc_store_class) -> dict:
    """
    Build a JSON-serialisable profile dict from Streamlit session state.

    Parameters
    ----------
    session_state : st.session_state  (passed in to avoid importing streamlit here)
    exc_store_class : the ExceptionStore class (for isinstance checks)

    Returns
    -------
    dict ready for json.dumps
    """
    profile: dict = {
        "_version": PROFILE_VERSION,
        "_saved_at": datetime.datetime.now().isoformat(timespec="seconds"),
    }

    for key in SERIALISABLE_KEYS:
        val = getattr(session_state, key, None)
        if val is None:
            continue
        profile[key] = val

    # ExceptionStore needs custom serialisation
    exc = getattr(session_state, "exc_store", None)
    if exc is not None:
        profile["exc_store"] = _exc_store_to_dict(exc)

    # sal_overrides has tuple keys
    sal_ov = getattr(session_state, "sal_overrides", {})
    if sal_ov:
        profile["sal_overrides"] = _sal_overrides_to_serial(sal_ov)

    return profile


def apply_profile(profile: dict, session_state, exc_store_class) -> list[str]:
    """
    Apply a loaded profile dict back into Streamlit session state.

    Returns a list of human-readable notes about what was loaded.
    """
    notes: list[str] = []

    version = profile.get("_version", 0)
    saved_at = profile.get("_saved_at", "unknown")
    notes.append(f"Profile version {version}, saved {saved_at}")

    for key in SERIALISABLE_KEYS:
        if key in profile:
            setattr(session_state, key, profile[key])
            notes.append(f"  ✔ {key}")

    # ExceptionStore
    if "exc_store" in profile:
        setattr(
            session_state,
            "exc_store",
            _exc_store_from_dict(profile["exc_store"], exc_store_class),
        )
        notes.append("  ✔ exc_store (global exclusions + per-BB exceptions)")

    # sal_overrides
    if "sal_overrides" in profile:
        setattr(
            session_state,
            "sal_overrides",
            _sal_overrides_from_serial(profile["sal_overrides"]),
        )
        notes.append("  ✔ sal_overrides")

    return notes


def profile_to_json(profile: dict) -> str:
    """Serialise profile dict to a JSON string."""
    return json.dumps(profile, indent=2, default=str)


def profile_from_json(json_str: str) -> dict:
    """Deserialise a JSON string to a profile dict.  Raises ValueError on bad input."""
    try:
        return json.loads(json_str)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid profile JSON: {exc}") from exc
