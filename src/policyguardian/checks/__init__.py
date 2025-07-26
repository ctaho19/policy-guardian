# SPDX-License-Identifier: MIT
# Copyright (c) 2025 ctaho19

"""Security checks for Policy Guardian."""

from __future__ import annotations

from ..core.checks import check_registry

from . import (
    overly_permissive,
    redundant_rules,
    rules_missing_security,
    shadowing_rules,
    zone_based,
)

__all__ = [
    "overly_permissive",
    "redundant_rules", 
    "rules_missing_security",
    "shadowing_rules",
    "zone_based",
]


def get_available_checks() -> dict[str, str]:
    checks = {}
    for check_id, check in check_registry.get_all_checks().items():
        checks[check_id] = check.description
    return checks 