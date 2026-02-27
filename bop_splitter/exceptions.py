"""Exception management: per-BB and global exclusions/inclusions/fixed allocations."""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field, asdict
from typing import Any


@dataclass
class ExceptionEntry:
    scope: str          # "global" | "bb_specific"
    bb_id: str | None
    sku: str
    exc_type: str       # "include" | "exclude" | "fixed_qty" | "salience_override"
    old_value: Any = None
    new_value: Any = None
    notes: str = ""
    timestamp: str = field(default_factory=lambda: datetime.datetime.now().isoformat(timespec="seconds"))

    def to_dict(self) -> dict:
        return asdict(self)


class ExceptionStore:
    """In-memory store for all exceptions."""

    def __init__(self):
        self.global_exclusions: set[str] = set()
        # bb_id -> {"include": set, "exclude": set, "fixed_qty": {sku: {month: qty}}}
        self.bb_exceptions: dict[str, dict] = {}
        self.log: list[ExceptionEntry] = []

    # ---- global ----
    def add_global_exclusion(self, sku: str, notes: str = ""):
        self.global_exclusions.add(sku)
        self.log.append(ExceptionEntry("global", None, sku, "exclude", notes=notes))

    def remove_global_exclusion(self, sku: str):
        self.global_exclusions.discard(sku)

    # ---- bb-specific ----
    def _ensure_bb(self, bb_id: str):
        if bb_id not in self.bb_exceptions:
            self.bb_exceptions[bb_id] = {"include": set(), "exclude": set(), "fixed_qty": {}}

    def add_bb_include(self, bb_id: str, sku: str, notes: str = ""):
        self._ensure_bb(bb_id)
        self.bb_exceptions[bb_id]["include"].add(sku)
        self.log.append(ExceptionEntry("bb_specific", bb_id, sku, "include", notes=notes))

    def remove_bb_include(self, bb_id: str, sku: str):
        self._ensure_bb(bb_id)
        self.bb_exceptions[bb_id]["include"].discard(sku)

    def add_bb_exclude(self, bb_id: str, sku: str, notes: str = ""):
        self._ensure_bb(bb_id)
        self.bb_exceptions[bb_id]["exclude"].add(sku)
        self.log.append(ExceptionEntry("bb_specific", bb_id, sku, "exclude", notes=notes))

    def remove_bb_exclude(self, bb_id: str, sku: str):
        self._ensure_bb(bb_id)
        self.bb_exceptions[bb_id]["exclude"].discard(sku)

    def set_fixed_qty(self, bb_id: str, sku: str, month: str, qty: float, notes: str = ""):
        self._ensure_bb(bb_id)
        if sku not in self.bb_exceptions[bb_id]["fixed_qty"]:
            self.bb_exceptions[bb_id]["fixed_qty"][sku] = {}
        old = self.bb_exceptions[bb_id]["fixed_qty"][sku].get(month)
        self.bb_exceptions[bb_id]["fixed_qty"][sku][month] = qty
        self.log.append(ExceptionEntry("bb_specific", bb_id, sku, "fixed_qty", old_value=old, new_value=qty, notes=notes))

    def get_fixed_qty(self, bb_id: str, sku: str, month: str) -> float | None:
        return self.bb_exceptions.get(bb_id, {}).get("fixed_qty", {}).get(sku, {}).get(month)

    def get_eligible_skus(self, bb_id: str, candidate_skus: list[str]) -> list[str]:
        """Apply include/exclude rules, then global exclusions."""
        exc = self.bb_exceptions.get(bb_id, {})
        forced_include = exc.get("include", set())
        forced_exclude = exc.get("exclude", set())

        eligible = []
        for sku in candidate_skus:
            if sku in self.global_exclusions and sku not in forced_include:
                continue
            if sku in forced_exclude:
                continue
            eligible.append(sku)

        # Forced includes not already in candidate list
        for sku in forced_include:
            if sku not in eligible:
                eligible.append(sku)

        return eligible

    def log_as_df(self):
        import pandas as pd
        if not self.log:
            return pd.DataFrame(columns=["scope", "bb_id", "sku", "exc_type", "old_value", "new_value", "notes", "timestamp"])
        return pd.DataFrame([e.to_dict() for e in self.log])
