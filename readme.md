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


Tier 1 - Control Execution Check
This query validates that our control is being executed by checking for recent records in the QER dataset within the last 3 days. It's a simple existence check that returns a binary result (1/0) based on whether we find any records for our control in the most recent batch, essentially confirming that our control evaluation process is running as expected.
Tier 2 - Resource Compliance Rate
This query calculates what percentage of our resources are compliant by comparing the total evaluated resources from QER against the count of non-compliant resources from our non-compliant history. The calculation takes the total evaluated resources as the denominator and subtracts any active (non-closed) non-compliant findings to determine how many resources are currently compliant, expressing this as a percentage.
Tier 2 Supporting Evidence
This query breaks down the compliance calculation by account/region/ASV to show exactly how many resources were evaluated and how many are non-compliant in each segment. By joining the QER data with non-compliant history and excluding closed findings, we can see the detailed distribution of compliant vs non-compliant resources that roll up into our Tier 2 metric.
Tier 3 - Past SLA Non-Compliance
This query measures remediation effectiveness by calculating what percentage of our total resources are past their SLA for remediation. It uses the TCRD view to identify non-compliant resources and determines if they're past SLA based on their risk level and age, then compares this count against total resources from QER to produce a percentage of resources that have exceeded their remediation timeline.
Tier 3 Supporting Evidence
This query provides visibility into the specific resources that are contributing to our past-SLA metric by showing each non-compliant resource's age and SLA status based on its risk level. It helps us understand which resources are driving our Tier 3 metric by showing their aging details and SLA classification, making it clear which ones need immediate attention for remediation.
