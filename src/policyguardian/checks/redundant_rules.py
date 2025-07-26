from __future__ import annotations

from typing import Any, Dict, List

from lxml import etree

from ..core.checks import CheckResult, CheckSeverity, SecurityCheck, check_registry


class RedundantRulesCheck(SecurityCheck):

    @property
    def name(self) -> str:
        return "Redundant Rules"

    @property
    def description(self) -> str:
        return "Finds rules that have unnecessary or duplicate entries."

    @property
    def severity(self) -> CheckSeverity:
        return CheckSeverity.MEDIUM

    def check_rule(self, rule: etree._Element, context: Dict[str, Any]) -> List[CheckResult]:
        results = []
        
        try:
            self.validate_rule_element(rule)
            rule_name = self.get_rule_name(rule)
            
            redundant_issues = self._find_redundant_issues(rule)
            
            for issue in redundant_issues:
                result = CheckResult(
                    rule_name=rule_name,
                    check_name=self.name,
                    severity=self.severity,
                    message=issue["message"],
                    details={
                        "field": issue["field"],
                        "issue_type": issue["type"],
                        "entries": issue["entries"],
                    },
                    file_path=context.get("file_path"),
                )
                results.append(result)
                
        except Exception as e:
            self.logger.error(f"Error checking rule {rule.get('name', 'unknown')}: {e}")
            
        return results

    def _find_redundant_issues(self, rule: etree._Element) -> List[Dict[str, Any]]:
        issues = []
        
        fields_to_check = {
            "source": ".//source/member",
            "destination": ".//destination/member", 
            "service": ".//service/member",
            "application": ".//application/member",
        }
        
        for field_name, xpath in fields_to_check.items():
            field_issues = self._check_field_for_redundancy(rule, field_name, xpath)
            issues.extend(field_issues)
        
        return issues

    def _check_field_for_redundancy(self, rule: etree._Element, field_name: str, xpath: str) -> List[Dict[str, Any]]:
        issues = []
        
        values = self.get_element_text_list(rule, xpath)
        
        if not values:
            return issues
        
        if "any" in values and len(values) > 1:
            other_values = [v for v in values if v != "any"]
            issues.append({
                "field": field_name,
                "type": "any_with_specific",
                "entries": values,
                "message": f"Field '{field_name}' contains 'any' alongside specific values {other_values}, making specific values redundant"
            })
        
        duplicates = self._find_duplicates(values)
        if duplicates:
            for duplicate in duplicates:
                issues.append({
                    "field": field_name,
                    "type": "duplicate_entries",
                    "entries": values,
                    "message": f"Field '{field_name}' contains duplicate entry: '{duplicate}'"
                })
        
        return issues

    def _find_duplicates(self, values: List[str]) -> List[str]:
        seen = set()
        duplicates = set()
        
        for value in values:
            if value in seen:
                duplicates.add(value)
            else:
                seen.add(value)
        
        return list(duplicates)


check_registry.register(RedundantRulesCheck()) 