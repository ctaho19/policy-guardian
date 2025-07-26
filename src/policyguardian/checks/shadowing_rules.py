from __future__ import annotations

from typing import Any, Dict, List, Set

from lxml import etree

from ..core.checks import CheckResult, CheckSeverity, SecurityCheck, check_registry


class ShadowingRulesCheck(SecurityCheck):

    @property
    def name(self) -> str:
        return "Shadowing Rules"

    @property
    def description(self) -> str:
        return "Identifies rules that may be overshadowed by other rules."

    @property
    def severity(self) -> CheckSeverity:
        return CheckSeverity.HIGH

    def check_rule(self, rule: etree._Element, context: Dict[str, Any]) -> List[CheckResult]:
        return []

    def check_all_rules(self, rules: List[etree._Element], context: Dict[str, Any]) -> List[CheckResult]:
        results = []
        
        try:
            for i, rule1 in enumerate(rules):
                for j, rule2 in enumerate(rules):
                    if i >= j:
                        continue
                    
                    shadowing_issue = self._check_rule_pair_for_shadowing(rule1, rule2)
                    if shadowing_issue:
                        result = CheckResult(
                            rule_name=shadowing_issue["shadowed_rule"],
                            check_name=self.name,
                            severity=self.severity,
                            message=shadowing_issue["message"],
                            details={
                                "shadowed_rule": shadowing_issue["shadowed_rule"],
                                "shadowing_rule": shadowing_issue["shadowing_rule"],
                                "conflicting_actions": shadowing_issue["actions"],
                            },
                            file_path=context.get("file_path"),
                        )
                        results.append(result)
                        
        except Exception as e:
            self.logger.error(f"Error checking rules for shadowing: {e}")
            
        return results

    def _check_rule_pair_for_shadowing(self, rule1: etree._Element, rule2: etree._Element) -> Dict[str, Any] | None:
        try:
            rule1_name = self.get_rule_name(rule1)
            rule2_name = self.get_rule_name(rule2)
            
            rule1_action = self.get_action(rule1)
            rule2_action = self.get_action(rule2)
            
            if rule1_action == rule2_action:
                return None
            
            rule1_criteria = self._extract_rule_criteria(rule1)
            rule2_criteria = self._extract_rule_criteria(rule2)
            
            if self._is_subset_rule(rule1_criteria, rule2_criteria):
                return {
                    "shadowed_rule": rule1_name,
                    "shadowing_rule": rule2_name,
                    "actions": {"shadowed": rule1_action, "shadowing": rule2_action},
                    "message": f"Rule '{rule1_name}' ({rule1_action}) may be shadowed by broader rule '{rule2_name}' ({rule2_action})"
                }
            
            if self._is_subset_rule(rule2_criteria, rule1_criteria):
                return {
                    "shadowed_rule": rule2_name,
                    "shadowing_rule": rule1_name,
                    "actions": {"shadowed": rule2_action, "shadowing": rule1_action},
                    "message": f"Rule '{rule2_name}' ({rule2_action}) may be shadowed by broader rule '{rule1_name}' ({rule1_action})"
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error comparing rules: {e}")
            return None

    def _extract_rule_criteria(self, rule: etree._Element) -> Dict[str, Set[str]]:
        criteria = {
            "source": set(self.get_element_text_list(rule, ".//source/member")),
            "destination": set(self.get_element_text_list(rule, ".//destination/member")),
            "service": set(self.get_element_text_list(rule, ".//service/member")),
            "application": set(self.get_element_text_list(rule, ".//application/member")),
            "from_zone": set(self.get_element_text_list(rule, ".//from/member")),
            "to_zone": set(self.get_element_text_list(rule, ".//to/member")),
        }
        
        for key, value in criteria.items():
            if not value:
                criteria[key] = {"any"}
        
        return criteria

    def _is_subset_rule(self, rule1_criteria: Dict[str, Set[str]], rule2_criteria: Dict[str, Set[str]]) -> bool:
        for field in rule1_criteria:
            rule1_values = rule1_criteria[field]
            rule2_values = rule2_criteria[field]
            
            if "any" in rule2_values:
                if "any" not in rule1_values:
                    continue
                else:
                    if rule1_values != rule2_values:
                        return False
            else:
                if "any" in rule1_values:
                    return False
                if not rule1_values.issubset(rule2_values):
                    return False
        
        is_more_specific = False
        for field in rule1_criteria:
            rule1_values = rule1_criteria[field]
            rule2_values = rule2_criteria[field]
            
            if "any" in rule2_values and "any" not in rule1_values:
                is_more_specific = True
                break
            elif ("any" not in rule2_values and "any" not in rule1_values and 
                  rule1_values.issubset(rule2_values) and rule1_values != rule2_values):
                is_more_specific = True
                break
        
        return is_more_specific


check_registry.register(ShadowingRulesCheck()) 