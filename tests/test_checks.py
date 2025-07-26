from __future__ import annotations

import pytest
from lxml import etree

from policyguardian.checks.overly_permissive import OverlyPermissiveRulesCheck
from policyguardian.checks.redundant_rules import RedundantRulesCheck
from policyguardian.checks.zone_based import ZoneBasedCheck
from policyguardian.checks.rules_missing_security import RulesMissingSecurityProfileCheck
from policyguardian.core.checks import CheckSeverity


class TestOverlyPermissiveRulesCheck:

    def test_detects_permissive_rule(self, sample_rule_element):
        check = OverlyPermissiveRulesCheck()
        results = check.check_rule(sample_rule_element, {"file_path": "test.xml"})
        
        assert len(results) == 1
        result = results[0]
        assert result.rule_name == "test-rule"
        assert result.check_name == "Overly Permissive Rules"
        assert result.severity in [CheckSeverity.MEDIUM, CheckSeverity.HIGH, CheckSeverity.CRITICAL]
        assert "any" in result.message.lower()

    def test_ignores_deny_rules(self):
        rule_xml = '''
        <entry name="deny-rule">
            <source><member>any</member></source>
            <destination><member>any</member></destination>
            <action>deny</action>
        </entry>
        '''
        rule_element = etree.fromstring(rule_xml)
        
        check = OverlyPermissiveRulesCheck()
        results = check.check_rule(rule_element, {})
        
        assert len(results) == 0

    def test_severity_calculation(self):
        check = OverlyPermissiveRulesCheck()
        
        rule_xml = '''
        <entry name="critical-rule">
            <source><member>any</member></source>
            <destination><member>any</member></destination>
            <service><member>any</member></service>
            <application><member>any</member></application>
            <action>allow</action>
        </entry>
        '''
        rule_element = etree.fromstring(rule_xml)
        results = check.check_rule(rule_element, {})
        
        assert len(results) == 1
        assert results[0].severity == CheckSeverity.CRITICAL


class TestRedundantRulesCheck:

    def test_detects_any_with_specific(self):
        rule_xml = '''
        <entry name="redundant-rule">
            <source>
                <member>any</member>
                <member>192.168.1.0/24</member>
            </source>
            <action>allow</action>
        </entry>
        '''
        rule_element = etree.fromstring(rule_xml)
        
        check = RedundantRulesCheck()
        results = check.check_rule(rule_element, {})
        
        assert len(results) == 1
        result = results[0]
        assert result.rule_name == "redundant-rule"
        assert "any" in result.message
        assert "alongside specific values" in result.message

    def test_detects_duplicates(self):
        rule_xml = '''
        <entry name="duplicate-rule">
            <source>
                <member>192.168.1.0/24</member>
                <member>192.168.1.0/24</member>
            </source>
            <action>allow</action>
        </entry>
        '''
        rule_element = etree.fromstring(rule_xml)
        
        check = RedundantRulesCheck()
        results = check.check_rule(rule_element, {})
        
        assert len(results) == 1
        result = results[0]
        assert "duplicate" in result.message.lower()


class TestZoneBasedCheck:

    def test_detects_missing_zones(self):
        rule_xml = '''
        <entry name="missing-zone-rule">
            <source><member>192.168.1.0/24</member></source>
            <destination><member>10.0.0.1</member></destination>
            <action>allow</action>
        </entry>
        '''
        rule_element = etree.fromstring(rule_xml)
        
        check = ZoneBasedCheck()
        results = check.check_rule(rule_element, {})
        
        assert len(results) == 2
        messages = [r.message for r in results]
        assert any("missing source zone" in msg for msg in messages)
        assert any("missing destination zone" in msg for msg in messages)

    def test_detects_both_zones_any(self):
        rule_xml = '''
        <entry name="both-any-rule">
            <from><member>any</member></from>
            <to><member>any</member></to>
            <action>allow</action>
        </entry>
        '''
        rule_element = etree.fromstring(rule_xml)
        
        check = ZoneBasedCheck()
        results = check.check_rule(rule_element, {})
        
        assert len(results) == 1
        assert "both source and destination zones are set to 'any'" in results[0].message

    def test_detects_identical_zones(self):
        rule_xml = '''
        <entry name="identical-zone-rule">
            <from><member>trust</member></from>
            <to><member>trust</member></to>
            <action>allow</action>
        </entry>
        '''
        rule_element = etree.fromstring(rule_xml)
        
        check = ZoneBasedCheck()
        results = check.check_rule(rule_element, {})
        
        assert len(results) == 1
        assert "identical zones" in results[0].message.lower()
        assert "intra-zone" in results[0].message.lower()


class TestRulesMissingSecurityProfileCheck:

    def test_detects_missing_profiles(self, sample_rule_element):
        check = RulesMissingSecurityProfileCheck()
        results = check.check_rule(sample_rule_element, {})
        
        assert len(results) == 1
        result = results[0]
        assert result.rule_name == "test-rule"
        assert "missing" in result.message.lower()

    def test_ignores_deny_rules(self):
        rule_xml = '''
        <entry name="deny-rule">
            <source><member>any</member></source>
            <destination><member>any</member></destination>
            <action>deny</action>
        </entry>
        '''
        rule_element = etree.fromstring(rule_xml)
        
        check = RulesMissingSecurityProfileCheck()
        results = check.check_rule(rule_element, {})
        
        assert len(results) == 0

    def test_complete_security_profiles(self):
        rule_xml = '''
        <entry name="secure-rule">
            <action>allow</action>
            <profile-setting>
                <profiles>
                    <virus><member>default</member></virus>
                    <spyware><member>default</member></spyware>
                    <vulnerability><member>default</member></vulnerability>
                    <url-filtering><member>default</member></url-filtering>
                    <file-blocking><member>default</member></file-blocking>
                    <wildfire-analysis><member>default</member></wildfire-analysis>
                </profiles>
            </profile-setting>
        </entry>
        '''
        rule_element = etree.fromstring(rule_xml)
        
        check = RulesMissingSecurityProfileCheck()
        results = check.check_rule(rule_element, {})
        
        assert len(results) == 0


class TestCheckIntegration:

    def test_invalid_rule_element(self):
        invalid_xml = '<invalid>not a rule</invalid>'
        invalid_element = etree.fromstring(invalid_xml)
        
        check = OverlyPermissiveRulesCheck()
        
        with pytest.raises(Exception):
            check.check_rule(invalid_element, {})

    def test_rule_without_name(self):
        rule_xml = '''
        <entry>
            <action>allow</action>
        </entry>
        '''
        rule_element = etree.fromstring(rule_xml)
        
        check = OverlyPermissiveRulesCheck()
        
        with pytest.raises(Exception):
            check.check_rule(rule_element, {}) 