from __future__ import annotations

from typing import Any, Dict, List

from lxml import etree

from ..core.checks import CheckResult, CheckSeverity, SecurityCheck, check_registry


class ZoneBasedCheck(SecurityCheck):

    @property
    def name(self) -> str:
        return "Zone Based Checks"

    @property
    def description(self) -> str:
        return "Examines zone configurations in firewall rules."

    @property
    def severity(self) -> CheckSeverity:
        return CheckSeverity.MEDIUM

    def check_rule(self, rule: etree._Element, context: Dict[str, Any]) -> List[CheckResult]:
        results = []
        
        try:
            self.validate_rule_element(rule)
            rule_name = self.get_rule_name(rule)
            
            from_zones = self.get_element_text_list(rule, ".//from/member")
            to_zones = self.get_element_text_list(rule, ".//to/member")
            
            zone_issues = self._find_zone_issues(from_zones, to_zones)
            
            for issue in zone_issues:
                severity = self._get_issue_severity(issue["type"])
                
                result = CheckResult(
                    rule_name=rule_name,
                    check_name=self.name,
                    severity=severity,
                    message=issue["message"],
                    details={
                        "issue_type": issue["type"],
                        "from_zones": from_zones,
                        "to_zones": to_zones,
                    },
                    file_path=context.get("file_path"),
                )
                results.append(result)
                
        except Exception as e:
            self.logger.error(f"Error checking rule {rule.get('name', 'unknown')}: {e}")
            
        return results

    def _find_zone_issues(self, from_zones: List[str], to_zones: List[str]) -> List[Dict[str, Any]]:
        issues = []
        
        if not from_zones:
            issues.append({
                "type": "missing_source_zone",
                "message": "Rule is missing source zone configuration"
            })
        
        if not to_zones:
            issues.append({
                "type": "missing_destination_zone", 
                "message": "Rule is missing destination zone configuration"
            })
        
        if from_zones and to_zones:
            if "any" in from_zones and "any" in to_zones:
                issues.append({
                    "type": "both_zones_any",
                    "message": "Both source and destination zones are set to 'any', creating overly broad access"
                })
            
            identical_zones = set(from_zones) & set(to_zones)
            if identical_zones and "any" not in identical_zones:
                issues.append({
                    "type": "identical_zones",
                    "message": f"Identical zones in source and destination: {', '.join(identical_zones)} - potential intra-zone traffic"
                })
        
        return issues

    def _get_issue_severity(self, issue_type: str) -> CheckSeverity:
        severity_map = {
            "missing_source_zone": CheckSeverity.HIGH,
            "missing_destination_zone": CheckSeverity.HIGH,
            "both_zones_any": CheckSeverity.HIGH,
            "identical_zones": CheckSeverity.MEDIUM,
        }
        
        return severity_map.get(issue_type, CheckSeverity.MEDIUM)


check_registry.register(ZoneBasedCheck()) 