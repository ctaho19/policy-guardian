from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Generator

import pytest
from lxml import etree


@pytest.fixture
def sample_rule_xml() -> str:
    return '''
    <entry name="test-rule">
        <from>
            <member>any</member>
        </from>
        <to>
            <member>DMZ</member>
        </to>
        <source>
            <member>10.0.0.0/24</member>
        </source>
        <destination>
            <member>any</member>
        </destination>
        <service>
            <member>any</member>
        </service>
        <application>
            <member>web-browsing</member>
        </application>
        <action>allow</action>
        <profile-setting>
            <profiles>
                <virus>
                    <member>none</member>
                </virus>
                <spyware>
                    <member>default</member>
                </spyware>
            </profiles>
        </profile-setting>
    </entry>
    '''


@pytest.fixture
def sample_config_xml() -> str:
    return '''<?xml version="1.0"?>
    <config>
        <devices>
            <entry name="localhost.localdomain">
                <vsys>
                    <entry name="vsys1">
                        <rulebase>
                            <security>
                                <rules>
                                    <entry name="rule1">
                                        <from><member>any</member></from>
                                        <to><member>any</member></to>
                                        <source><member>any</member></source>
                                        <destination><member>any</member></destination>
                                        <service><member>any</member></service>
                                        <application><member>any</member></application>
                                        <action>allow</action>
                                    </entry>
                                    <entry name="rule2">
                                        <from><member>trust</member></from>
                                        <to><member>dmz</member></to>
                                        <source><member>192.168.1.0/24</member></source>
                                        <destination><member>10.0.0.1</member></destination>
                                        <service><member>tcp-443</member></service>
                                        <application><member>ssl</member></application>
                                        <action>allow</action>
                                        <profile-setting>
                                            <profiles>
                                                <virus><member>default</member></virus>
                                                <spyware><member>default</member></spyware>
                                                <vulnerability><member>default</member></vulnerability>
                                            </profiles>
                                        </profile-setting>
                                    </entry>
                                </rules>
                            </security>
                        </rulebase>
                    </entry>
                </vsys>
            </entry>
        </devices>
    </config>
    '''


@pytest.fixture
def temp_xml_file(sample_config_xml: str) -> Generator[Path, None, None]:
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
        f.write(sample_config_xml)
        temp_path = Path(f.name)
    
    try:
        yield temp_path
    finally:
        temp_path.unlink(missing_ok=True)


@pytest.fixture
def sample_rule_element(sample_rule_xml: str) -> etree._Element:
    return etree.fromstring(sample_rule_xml)


@pytest.fixture
def sample_config_element(sample_config_xml: str) -> etree._Element:
    return etree.fromstring(sample_config_xml) 