# policy-guardian
policyGuardian is a firewall policy analyzer web application. It allows users to select a Palo Alto Firewalls XML configuration file, choose from multiple validation checks, and run them against the firewall rules. The application then displays the results and offers the option to export a detailed PDF report of the findings.

## Features

- Upload multiple firewall configuration files
- Select from various security checks to perform
- Batch processing of files
- Display results in a tabular format
- Export results in CSV and PDF formats

## Available Checks

**Overly Permissive Rules:** This check identifies firewall rules that are too broad in their permissions. It looks for rules where source, destination, service, or application are set to "any" and the action is "allow".

**Redundant Rules:** This check finds rules that have unnecessary or duplicate entries. It looks for cases where "any" is used alongside specific entries, or where there are duplicate entries in source, destination, service, or application fields. 

**Zone Based Checks:** This check examines the zone configurations in firewall rules. It identifies issues such as missing source or destination zones, both zones set to "any", identical source and destination zones, and potential intra-zone traffic.

**Shadowing Rules:** This check identifies rules that may be overshadowed by other rules. It compares each rule with others to find cases where one rule's criteria are a subset of another's, but with a different action.

**Rules Missing Security Zones:** This check looks for rules that allow traffic but are missing security profiles. It ensures that allow rules have appropriate security measures in place, such as virus scanning, spyware protection, vulnerability protection, URL filtering, file blocking, and wildfire analysis. 


## Technology Stack

- **Next.js**
- **React**
- **Tailwind CSS**
- **shadcn/ui components**

## Getting Started

### Prerequisites

- **Node.js** (v14 or later)
- **npm** (v6 or later)

### Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/ctaho19/policy-guardian.git

2. **Navigate to the project directory:**

   ```bash
   cd <project-directory>
   
3. **Install dependencies:**

   ```bash
   npm install

4. **Start the development server:**

   ```bash
   npm run dev

4. **Open your browser and visit http://localhost:3000**

### Usage
Upload your firewall configuration files using the file input.
Select the checks you want to perform from the available options.
Click the "Run Checks" button to analyze the files.
View the results in the table below.
Export the results in CSV or PDF format using the respective buttons.

### Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

### License
This project is licensed under the MIT License.