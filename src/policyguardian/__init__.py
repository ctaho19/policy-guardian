# SPDX-License-Identifier: MIT
# Copyright (c) 2025 ctaho19

"""Policy Guardian - Firewall Policy Analyzer"""

from __future__ import annotations

__version__ = "1.0.0"
__author__ = "ctaho19"
__email__ = "ctaho19@users.noreply.github.com"
__license__ = "MIT"

from .core.analyzer import PolicyAnalyzer
from .core.checks import CheckResult, CheckSeverity
from .core.exceptions import PolicyGuardianError, ConfigurationError, ValidationError

__all__ = [
    "PolicyAnalyzer",
    "CheckResult", 
    "CheckSeverity",
    "PolicyGuardianError",
    "ConfigurationError", 
    "ValidationError",
    "__version__",
] 