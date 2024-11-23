import xml.etree.ElementTree as ET

def check_shadowingrules(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    security_rules = root.findall('.//security/rules/entry')
    shadowing_rules = []
    
    for i, rule in enumerate(security_rules):
        rule_name = rule.get('name')
        rule_from = set([member.text for member in rule.findall('./from/member')])
        rule_to = set([member.text for member in rule.findall('./to/member')])
        rule_source = set([member.text for member in rule.findall('./source/member')])
        rule_destination = set([member.text for member in rule.findall('./destination/member')])
        rule_service = set([member.text for member in rule.findall('./service/member')])
        rule_application = set([member.text for member in rule.findall('./application/member')])
        rule_action = rule.find('./action').text if rule.find('./action') is not None else None
        
        for j, other_rule in enumerate(security_rules[i+1:], start=i+1):
            other_name = other_rule.get('name')
            other_from = set([member.text for member in other_rule.findall('./from/member')])
            other_to = set([member.text for member in other_rule.findall('./to/member')])
            other_source = set([member.text for member in other_rule.findall('./source/member')])
            other_destination = set([member.text for member in other_rule.findall('./destination/member')])
            other_service = set([member.text for member in other_rule.findall('./service/member')])
            other_application = set([member.text for member in other_rule.findall('./application/member')])
            other_action = other_rule.find('./action').text if other_rule.find('./action') is not None else None
            
            if (rule_from.issubset(other_from) and rule_to.issubset(other_to) and
                rule_source.issubset(other_source) and rule_destination.issubset(other_destination) and
                rule_service.issubset(other_service) and rule_application.issubset(other_application) and
                rule_action != other_action):
                shadowing_rules.append((rule_name, other_name))
    
    return shadowing_rules

