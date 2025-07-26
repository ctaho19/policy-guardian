import xml.etree.ElementTree as ET

def check_redundantrule(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    security_rules = root.findall('.//security/rules/entry')
    redundancies = []
    
    for rule in security_rules:
        rule_name = rule.get('name')
        source = [member.text for member in rule.findall('./source/member')]
        destination = [member.text for member in rule.findall('./destination/member')]
        service = [member.text for member in rule.findall('./service/member')]
        application = [member.text for member in rule.findall('./application/member')]
        
        if 'any' in source and len(source) > 1:
            redundancies.append((rule_name, 'Redundant source addresses'))
        if 'any' in destination and len(destination) > 1:
            redundancies.append((rule_name, 'Redundant destination addresses'))
        if 'any' in service and len(service) > 1:
            redundancies.append((rule_name, 'Redundant services'))
        if 'any' in application and len(application) > 1:
            redundancies.append((rule_name, 'Redundant applications'))
        
        # Check for duplicate entries
        for field, name in [(source, 'source'), (destination, 'destination'), (service, 'service'), (application, 'application')]:
            if len(field) != len(set(field)):
                redundancies.append((rule_name, f'Duplicate {name} entries'))
    
    return redundancies

