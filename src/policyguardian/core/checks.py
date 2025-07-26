from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol

from lxml import etree

from .exceptions import ValidationError

logger = logging.getLogger(__name__)


class CheckSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class CheckResult:
    rule_name: str
    check_name: str
    severity: CheckSeverity
    message: str
    details: Dict[str, Any]
    file_path: Optional[str] = None
    line_number: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "check_name": self.check_name,
            "severity": self.severity.value,
            "message": self.message,
            "details": self.details,
            "file_path": self.file_path,
            "line_number": self.line_number,
        }


class ConfigurationParser(Protocol):
    def parse(self, file_path: str) -> etree._Element:
        ...


class SecurityCheck(ABC):
    def __init__(self) -> None:
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        pass

    @property
    @abstractmethod
    def severity(self) -> CheckSeverity:
        pass

    @abstractmethod
    def check_rule(self, rule: etree._Element, context: Dict[str, Any]) -> List[CheckResult]:
        pass

    def validate_rule_element(self, rule: etree._Element) -> None:
        if rule.tag != "entry":
            raise ValidationError(f"Expected 'entry' element, got '{rule.tag}'")
        
        rule_name = rule.get("name")
        if not rule_name:
            raise ValidationError("Rule missing 'name' attribute")

    def get_rule_name(self, rule: etree._Element) -> str:
        name = rule.get("name")
        if not name:
            raise ValidationError("Rule missing 'name' attribute")
        return name

    def get_element_text_list(self, parent: etree._Element, xpath: str) -> List[str]:
        elements = parent.xpath(xpath)
        return [elem.text for elem in elements if elem.text is not None]

    def has_any_value(self, parent: etree._Element, xpath: str) -> bool:
        values = self.get_element_text_list(parent, xpath)
        return "any" in values

    def get_action(self, rule: etree._Element) -> str:
        action_elem = rule.find("action")
        if action_elem is not None and action_elem.text:
            return action_elem.text.lower()
        return "unknown"


class CheckRegistry:
    def __init__(self) -> None:
        self._checks: Dict[str, SecurityCheck] = {}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def register(self, check: SecurityCheck) -> None:
        check_id = check.__class__.__name__
        if check_id in self._checks:
            self.logger.warning(f"Check {check_id} already registered, overwriting")
        self._checks[check_id] = check
        self.logger.debug(f"Registered check: {check_id}")

    def get_check(self, check_id: str) -> Optional[SecurityCheck]:
        return self._checks.get(check_id)

    def get_all_checks(self) -> Dict[str, SecurityCheck]:
        return self._checks.copy()

    def get_check_names(self) -> List[str]:
        return list(self._checks.keys())


check_registry = CheckRegistry() 