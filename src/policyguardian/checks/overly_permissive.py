from __future__ import annotations

from typing import Any, Dict, List

from lxml import etree

from ..core.checks import CheckResult, CheckSeverity, SecurityCheck, check_registry


class OverlyPermissiveRulesCheck(SecurityCheck):

    @property
    def name(self) -> str:
        return "Overly Permissive Rules"

    @property
    def description(self) -> str:
        return "Identifies firewall rules that are too broad in their permissions."

    @property
    def severity(self) -> CheckSeverity:
        return CheckSeverity.HIGH

    def check_rule(self, rule: etree._Element, context: Dict[str, Any]) -> List[CheckResult]:
        results = []
        
        try:
            self.validate_rule_element(rule)
            rule_name = self.get_rule_name(rule)
            
            action = self.get_action(rule)
            if action != "allow":
                return results
            
            permissive_conditions = self._find_permissive_conditions(rule)
            
            if permissive_conditions:
                severity = self._calculate_severity(permissive_conditions)
                message = self._build_message(permissive_conditions)
                
                result = CheckResult(
                    rule_name=rule_name,
                    check_name=self.name,
                    severity=severity,
                    message=message,
                    details={
                        "action": action,
                        "permissive_fields": permissive_conditions,
                        "risk_level": severity.value,
                    },
                    file_path=context.get("file_path"),
                )
                results.append(result)
                
        except Exception as e:
            self.logger.error(f"Error checking rule {rule.get('name', 'unknown')}: {e}")
            
        return results

    def _find_permissive_conditions(self, rule: etree._Element) -> List[str]:
        permissive_fields = []
        
        if self.has_any_value(rule, ".//source/member"):
            permissive_fields.append("source")
        
  
        if self.has_any_value(rule, ".//destination/member"):
            permissive_fields.append("destination")
        
        if self.has_any_value(rule, ".//service/member"):
            permissive_fields.append("service")
        
        if self.has_any_value(rule, ".//application/member"):
            permissive_fields.append("application")
        
        return permissive_fields

    def _calculate_severity(self, permissive_conditions: List[str]) -> CheckSeverity:
        num_conditions = len(permissive_conditions)
        
        if num_conditions >= 3:
            return CheckSeverity.CRITICAL
        elif num_conditions == 2:
            return CheckSeverity.HIGH
        else:
            return CheckSeverity.MEDIUM

    def _build_message(self, permissive_conditions: List[str]) -> str:
        fields_str = ", ".join(permissive_conditions)
        
        if len(permissive_conditions) == 1:
            return f"Rule allows 'any' for {fields_str}, making it overly permissive"
        else:
            return f"Rule allows 'any' for multiple fields ({fields_str}), creating significant security risk"


check_registry.register(OverlyPermissiveRulesCheck()) 