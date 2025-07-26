import xml.etree.ElementTree as ET

def check_zonebasedchecks(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    security_rules = root.findall('.//security/rules/entry')
    zone_issues = []
    
    for rule in security_rules:
        rule_name = rule.get('name')
        from_zones = [member.text for member in rule.findall('./from/member')]
        to_zones = [member.text for member in rule.findall('./to/member')]
        
        if not from_zones:
            zone_issues.append((rule_name, 'Missing source zone'))
        if not to_zones:
            zone_issues.append((rule_name, 'Missing destination zone'))
        if 'any' in from_zones and 'any' in to_zones:
            zone_issues.append((rule_name, 'Both source and destination zones set to "any"'))
        if len(from_zones) == 1 and len(to_zones) == 1 and from_zones[0] == to_zones[0] and from_zones[0] != 'any':
            zone_issues.append((rule_name, f'Same single source and destination zone: {from_zones[0]}'))
        
        # Check for potential intra-zone traffic
        if set(from_zones).intersection(set(to_zones)):
            zone_issues.append((rule_name, 'Potential intra-zone traffic allowed'))
    
    return zone_issues
