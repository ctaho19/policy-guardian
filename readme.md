![alt text](https://github.com/ctaho19/policy-guardian/blob/main/Images/logo.png)
# policy-guardian
policyGuardian is a firewall policy analyzer  graphical user interface. It allows users to select a Palo Alto Firewalls XML configuration file, choose from multiple validation checks, and run them against the firewall rules. The application then displays the results and offers the option to export a detailed PDF report of the findings.

## Features

- Upload multiple firewall configuration files
- Select from various security checks to perform
- Batch processing of files
- Display results in a tabular format
- Export results in PDF format

## Available Checks

**Overly Permissive Rules:** This check identifies firewall rules that are too broad in their permissions. It looks for rules where source, destination, service, or application are set to "any" and the action is "allow".

**Redundant Rules:** This check finds rules that have unnecessary or duplicate entries. It looks for cases where "any" is used alongside specific entries, or where there are duplicate entries in source, destination, service, or application fields. 

**Zone Based Checks:** This check examines the zone configurations in firewall rules. It identifies issues such as missing source or destination zones, both zones set to "any", identical source and destination zones, and potential intra-zone traffic.

**Shadowing Rules:** This check identifies rules that may be overshadowed by other rules. It compares each rule with others to find cases where one rule's criteria are a subset of another's, but with a different action.

**Rules Missing Security Zones:** This check looks for rules that allow traffic but are missing security profiles. It ensures that allow rules have appropriate security measures in place, such as virus scanning, spyware protection, vulnerability protection, URL filtering, file blocking, and wildfire analysis. 

## Sample Configuration

For testing purposes, this project uses a sample Palo Alto Networks firewall configuration XML file from the [Azure Application Gateway Integration repository](https://github.com/PaloAltoNetworks/azure-applicationgateway). This sample configuration helps demonstrate the analyzer's capabilities without requiring access to a production firewall configuration.

Note: When using with your own firewall configurations, ensure you've sanitized any sensitive information.

## Requirements
- Python 3.6+
- PyQt6
- reportlab

## Installation
1. Clone the repository
2. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the application:
   ```bash
   python GUI.py
   ```

## Structure
- `GUI.py`: Main application file
- `Test Scripts/`: Contains validator modules
- `path/to/logo.png`: Application icon

### Usage
Upload your firewall configuration files using the file input.
Select the checks you want to perform from the available options.
Click the "Run Checks" button to analyze the files.
View the results in the table below.
Export the results in PDF format using the respective buttons.

Example Output:
![alt text](https://github.com/ctaho19/policy-guardian/blob/main/Images/Report%20Screenshot.png)

### Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

### License & Acknowldgment
This project is licensed under the MIT License. The idea stemmed from @moshekplan's [palo_alto_firewall_analyzer repository](https://github.com/moshekaplan/palo_alto_firewall_analyzer) and leverages the python validators he created.
