from __future__ import annotations

from typing import Any, Dict, List

from lxml import etree

from ..core.checks import CheckResult, CheckSeverity, SecurityCheck, check_registry


class RulesMissingSecurityProfileCheck(SecurityCheck):

    @property
    def name(self) -> str:
        return "Rules Missing Security Profiles"

    @property
    def description(self) -> str:
        return "Identifies rules that allow traffic but are missing security profiles."

    @property
    def severity(self) -> CheckSeverity:
        return CheckSeverity.MEDIUM

    def check_rule(self, rule: etree._Element, context: Dict[str, Any]) -> List[CheckResult]:
        results = []
        
        try:
            self.validate_rule_element(rule)
            rule_name = self.get_rule_name(rule)
            
            action = self.get_action(rule)
            if action != "allow":
                return results
            
            missing_profiles = self._find_missing_security_profiles(rule)
            
            if missing_profiles:
                severity = self._calculate_severity(missing_profiles)
                message = self._build_message(missing_profiles)
                
                result = CheckResult(
                    rule_name=rule_name,
                    check_name=self.name,
                    severity=severity,
                    message=message,
                    details={
                        "action": action,
                        "missing_profiles": missing_profiles,
                        "recommendation": "Configure appropriate security profiles for this allow rule",
                    },
                    file_path=context.get("file_path"),
                )
                results.append(result)
                
        except Exception as e:
            self.logger.error(f"Error checking rule {rule.get('name', 'unknown')}: {e}")
            
        return results

    def _find_missing_security_profiles(self, rule: etree._Element) -> List[str]:
        missing_profiles = []
        
        profiles_to_check = {
            "virus": ".//profile-setting/profiles/virus/member",
            "spyware": ".//profile-setting/profiles/spyware/member", 
            "vulnerability": ".//profile-setting/profiles/vulnerability/member",
            "url-filtering": ".//profile-setting/profiles/url-filtering/member",
            "file-blocking": ".//profile-setting/profiles/file-blocking/member",
            "wildfire-analysis": ".//profile-setting/profiles/wildfire-analysis/member",
        }
        
        for profile_name, xpath in profiles_to_check.items():
            profile_members = self.get_element_text_list(rule, xpath)
            
            if not profile_members or all(member in ["none", ""] for member in profile_members):
                missing_profiles.append(profile_name)
        
        return missing_profiles

    def _calculate_severity(self, missing_profiles: List[str]) -> CheckSeverity:
        num_missing = len(missing_profiles)
        
        critical_profiles = {"virus", "spyware", "vulnerability"}
        missing_critical = len(set(missing_profiles) & critical_profiles)
        
        if missing_critical >= 2:
            return CheckSeverity.HIGH
        elif missing_critical == 1 or num_missing >= 4:
            return CheckSeverity.MEDIUM
        else:
            return CheckSeverity.LOW

    def _build_message(self, missing_profiles: List[str]) -> str:
        if len(missing_profiles) == 1:
            return f"Allow rule is missing {missing_profiles[0]} security profile"
        else:
            profiles_str = ", ".join(missing_profiles)
            return f"Allow rule is missing multiple security profiles: {profiles_str}"


check_registry.register(RulesMissingSecurityProfileCheck()) 