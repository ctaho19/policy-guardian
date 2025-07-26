import xml.etree.ElementTree as ET

def check_overlypermissiverules(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    security_rules = root.findall('.//security/rules/entry')
    overly_permissive_rules = []
    
    for rule in security_rules:
        rule_name = rule.get('name')
        source = set([member.text for member in rule.findall('./source/member')])
        destination = set([member.text for member in rule.findall('./destination/member')])
        service = set([member.text for member in rule.findall('./service/member')])
        application = set([member.text for member in rule.findall('./application/member')])
        action = rule.find('./action').text if rule.find('./action') is not None else None
        
        if 'any' in source and 'any' in destination and 'any' in service and 'any' in application and action == 'allow':
            overly_permissive_rules.append((rule_name, 'All fields set to "any" and action is "allow"'))
        elif 'any' in source and 'any' in destination and action == 'allow':
            overly_permissive_rules.append((rule_name, 'Source and destination set to "any" and action is "allow"'))
        elif 'any' in service and 'any' in application and action == 'allow':
            overly_permissive_rules.append((rule_name, 'Service and application set to "any" and action is "allow"'))
    
    return overly_permissive_rules

