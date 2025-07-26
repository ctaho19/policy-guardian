# PolicyGuardian

A comprehensive security policy analysis tool for Palo Alto Networks firewall configurations. PolicyGuardian provides both CLI and GUI interfaces for analyzing security rules and identifying potential vulnerabilities, misconfigurations, and compliance issues.

## Features

### Security Analysis
- **Overly Permissive Rules**: Detects rules with overly broad source, destination, or service configurations
- **Missing Security Profiles**: Identifies allow rules lacking proper security profiles (antivirus, anti-spyware, vulnerability protection, etc.)
- **Rule Shadowing**: Finds rules that may be rendered ineffective by other rules with broader scope
- **Redundant Rules**: Locates duplicate or unnecessary rule configurations
- **Zone-Based Issues**: Validates zone configurations and identifies potential zone-based security problems

### User Interfaces
- **Modern GUI**: PyQt6-based interface with dark/light themes, interactive charts, filtering, and PDF export
- **Command Line Interface**: Flexible CLI with multiple output formats (table, JSON) and advanced filtering options
- **Batch Processing**: Analyze multiple configuration files simultaneously

### Output and Reporting
- **Interactive Charts**: Severity distribution visualization and analysis summaries
- **PDF Reports**: Professional reports with charts, detailed findings, and recommendations
- **Multiple Formats**: Table, JSON, and structured output options
- **Severity Filtering**: Filter results by severity level (low, medium, high, critical)

## Installation

### Prerequisites
- Python 3.8 or higher
- Required packages: `lxml`, `click`, `PyQt6` (for GUI)

### Install Dependencies
```bash
pip install lxml click PyQt6
```

### Clone Repository
```bash
git clone https://github.com/ctaho19/policy-guardian.git
cd policy-guardian
```

## Usage

### GUI Application
Launch the graphical interface:
```bash
cd src
PYTHONPATH=. python policyguardian/gui.py
```

The GUI provides:
- File selection and drag-and-drop support
- Real-time analysis with progress indicators
- Interactive results table with detailed panels
- Severity-based filtering and search functionality
- Theme switching (dark/light mode)
- PDF export with charts and recommendations

### Command Line Interface

#### Basic Analysis
```bash
cd src
PYTHONPATH=. python -m policyguardian.cli analyze /path/to/config.xml
```

#### Advanced Options
```bash
# Analyze with specific checks only
python -m policyguardian.cli analyze config.xml --checks overly_permissive --checks missing_security

# Filter by minimum severity
python -m policyguardian.cli analyze config.xml --severity high

# JSON output format
python -m policyguardian.cli analyze config.xml --output json

# Save results to file
python -m policyguardian.cli analyze config.xml --save results.json

# Memory-efficient parsing for large files
python -m policyguardian.cli analyze config.xml --iterative
```

#### List Available Checks
```bash
python -m policyguardian.cli list-checks
```

## Security Checks

### 1. Overly Permissive Rules
Identifies rules that use "any" for critical fields, creating potential security risks.

**Severity Calculation:**
- Critical: Multiple "any" values (source, destination, service, application)
- High: Two "any" values
- Medium: Single "any" value with allow action

### 2. Rules Missing Security Profiles
Detects allow rules lacking essential security profiles.

**Profiles Checked:**
- Antivirus protection
- Anti-spyware protection  
- Vulnerability protection
- URL filtering
- File blocking
- WildFire analysis

**Severity Calculation:**
- High: Missing 2+ critical profiles (virus, spyware, vulnerability)
- Medium: Missing 1 critical profile or 4+ total profiles
- Low: Missing other profiles

### 3. Rule Shadowing
Finds rules that may be ineffective due to broader rules above them.

**Detection Logic:**
- Compares rule scope and positioning
- Identifies potential rule conflicts
- Analyzes source, destination, and service overlap

### 4. Redundant Rules
Locates duplicate or unnecessary rule configurations.

**Detection Types:**
- "Any" values alongside specific entries
- Duplicate entries within the same rule
- Completely duplicate rules

### 5. Zone-Based Checks
Validates zone configurations and identifies issues.

**Checks Include:**
- Missing source or destination zones
- Both zones set to "any"
- Identical source and destination zones (intra-zone traffic)

## Project Structure

```
policy-guardian/
├── src/policyguardian/
│   ├── __init__.py              # Package initialization and main analyzer
│   ├── cli.py                   # Command line interface
│   ├── gui.py                   # PyQt6 graphical interface
│   ├── core/
│   │   ├── analyzer.py          # Main analysis engine
│   │   ├── parser.py            # XML configuration parser
│   │   ├── checks.py            # Base check classes and registry
│   │   └── exceptions.py        # Custom exceptions
│   └── checks/
│       ├── overly_permissive.py # Permissive rule detection
│       ├── rules_missing_security.py # Security profile validation
│       ├── shadowing_rules.py   # Rule shadowing analysis
│       ├── redundant_rules.py   # Redundancy detection
│       └── zone_based.py        # Zone configuration checks
├── tests/                       # Test suite
├── pyproject.toml              # Project configuration
└── README.md                   # This file
```

## Configuration File Support

PolicyGuardian supports Palo Alto Networks XML configuration files, including:
- Full device configurations
- Partial policy exports
- Panorama configurations
- Device group policies

## Output Examples

### CLI Table Output
```
Analysis Results for config.xml
================================================================================
Found 3 total issues:

CRITICAL (1 issues):
----------------------------------------
  • allow_all: Rule allows 'any' for multiple fields (source, destination, service)

HIGH (2 issues):
----------------------------------------
  • web_rule: Allow rule is missing multiple security profiles: virus, spyware
  • web_rule: Both source and destination zones are set to 'any'
```

### JSON Output
```json
[
  {
    "rule_name": "allow_all",
    "check_name": "Overly Permissive Rules",
    "severity": "critical",
    "message": "Rule allows 'any' for multiple fields",
    "details": {
      "any_fields": ["source", "destination", "service"],
      "recommendation": "Restrict rule scope to specific addresses and services"
    },
    "file_path": "config.xml"
  }
]
```

## Development

### Running Tests
```bash
cd policy-guardian
python -m pytest tests/
```

### Code Structure
The project follows a modular architecture:
- **Core Engine**: Handles XML parsing and analysis coordination
- **Check Registry**: Plugin-style architecture for security checks
- **Interfaces**: Separate CLI and GUI implementations
- **Extensibility**: Easy to add new security checks

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is available under the MIT License. See LICENSE file for details.

## Requirements

- Python 3.8+
- lxml (XML parsing)
- click (CLI framework)
- PyQt6 (GUI framework)

## Support

For issues, questions, or contributions, please use the GitHub issue tracker at:
https://github.com/ctaho19/policy-guardian/issues