# Policy Guardian

[![PyPI version](https://badge.fury.io/py/policy-guardian.svg)](https://badge.fury.io/py/policy-guardian)
[![Python Support](https://img.shields.io/pypi/pyversions/policy-guardian.svg)](https://pypi.org/project/policy-guardian/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![CI/CD](https://github.com/ctaho19/policy-guardian/actions/workflows/ci.yml/badge.svg)](https://github.com/ctaho19/policy-guardian/actions)

A comprehensive firewall policy analyzer for Palo Alto Networks configurations with both command-line and graphical interfaces. Policy Guardian helps security professionals identify potential vulnerabilities, misconfigurations, and optimization opportunities in firewall rulesets.

![Policy Guardian Screenshot](Images/policy-guardian-demo.gif)

## ✨ Features

### 🔍 Security Analysis
- **Overly Permissive Rules**: Identifies rules with broad permissions that may pose security risks
- **Redundant Rules**: Finds unnecessary or duplicate rule entries
- **Zone-Based Checks**: Examines zone configurations for security issues
- **Shadowing Rules**: Detects rules that may be overshadowed by other rules
- **Missing Security Profiles**: Ensures allow rules have appropriate security measures

### 🚀 Multiple Interfaces
- **Command Line Interface (CLI)**: Perfect for automation and CI/CD pipelines
- **Graphical User Interface (GUI)**: User-friendly interface for interactive analysis
- **Python API**: Programmatic access for custom integrations

### ⚡ Performance & Scalability
- **Memory-efficient parsing**: Handles large configuration files without excessive memory usage
- **Parallel processing**: Multi-threaded analysis for faster results
- **Batch processing**: Analyze multiple configuration files at once

### 🔧 Modern Development Practices
- **Type safety**: Full type hints throughout the codebase
- **Comprehensive testing**: High test coverage with pytest
- **Extensible architecture**: Plugin-based system for custom checks
- **Security-hardened**: Protection against XXE attacks and other vulnerabilities

## 📦 Installation

### Quick Install (Recommended)

```bash
# Install CLI version
pip install policy-guardian

# Install with GUI support
pip install policy-guardian[gui]

# Install development version with all dependencies
pip install policy-guardian[dev,gui,docs]
```

### Using pipx (Isolated Installation)

```bash
# Install globally while keeping dependencies isolated
pipx install policy-guardian[gui]
```

### From Source

```bash
git clone https://github.com/ctaho19/policy-guardian.git
cd policy-guardian
pip install -e ".[dev,gui]"
```

### Pre-built Executables

Download standalone executables from the [releases page](https://github.com/ctaho19/policy-guardian/releases) - no Python installation required!

## 🚀 Quick Start

### Command Line Interface

```bash
# Analyze a single configuration file
policy-guardian analyze config.xml

# Analyze multiple files with specific checks
policy-guardian analyze config1.xml config2.xml -c OverlyPermissiveRulesCheck -c ZoneBasedCheck

# Export results to JSON
policy-guardian analyze config.xml --output json --save results.json

# Filter by severity level
policy-guardian analyze config.xml --severity high

# List available security checks
policy-guardian list-checks
```

### Graphical Interface

```bash
# Launch the GUI application
policyguardian-gui
```

### Python API

```python
from policyguardian import PolicyAnalyzer

# Create analyzer instance
analyzer = PolicyAnalyzer()

# Analyze a configuration file
results = analyzer.analyze_file("firewall-config.xml")

# Process results
for result in results:
    print(f"Rule: {result.rule_name}")
    print(f"Issue: {result.message}")
    print(f"Severity: {result.severity.value}")
    print("---")
```

## 📊 Available Security Checks

| Check Name | Description | Severity |
|------------|-------------|----------|
| **OverlyPermissiveRulesCheck** | Identifies rules with broad permissions (source, destination, service, or application set to "any") | High |
| **RedundantRulesCheck** | Finds unnecessary or duplicate entries in rule fields | Medium |
| **ZoneBasedCheck** | Examines zone configurations for missing or problematic zone settings | Medium |
| **ShadowingRulesCheck** | Detects rules that may be overshadowed by other rules with conflicting actions | High |
| **RulesMissingSecurityProfileCheck** | Ensures allow rules have appropriate security profiles configured | Medium |

## 🔧 Configuration

### Environment Variables

```bash
# Enable debug logging
export POLICY_GUARDIAN_LOG_LEVEL=DEBUG

# Set custom configuration directory
export POLICY_GUARDIAN_CONFIG_DIR=/path/to/config
```

### Configuration File

Create a `~/.policy-guardian/config.toml` file:

```toml
[analysis]
# Default checks to run (empty = all)
default_checks = [
    "OverlyPermissiveRulesCheck",
    "ZoneBasedCheck"
]

# Memory-efficient parsing for large files
use_iterative_parsing = true

[output]
# Default output format
default_format = "table"

# Include rule details in output
include_details = true

[logging]
# Log level (DEBUG, INFO, WARNING, ERROR)
level = "INFO"

# Log file location
file = "~/.policy-guardian/policy-guardian.log"
```

## 🧪 Development

### Setting Up Development Environment

```bash
# Clone the repository
git clone https://github.com/ctaho19/policy-guardian.git
cd policy-guardian

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e ".[dev,gui,docs]"

# Install pre-commit hooks
pre-commit install
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=policyguardian --cov-report=html

# Run specific test file
pytest tests/test_checks.py

# Run with verbose output
pytest -v
```

### Code Quality

```bash
# Format code
black src tests

# Sort imports
isort src tests

# Lint code
flake8 src tests

# Type checking
mypy src

# Security audit
bandit -r src
pip-audit
```

### Building Documentation

```bash
cd docs
make html
open _build/html/index.html
```

##  Creating Custom Checks

Policy Guardian supports custom security checks through a plugin system:

```python
from policyguardian.core.checks import SecurityCheck, CheckResult, CheckSeverity, check_registry
from typing import List, Dict, Any
from lxml import etree

class CustomSecurityCheck(SecurityCheck):
    @property
    def name(self) -> str:
        return "Custom Security Check"
    
    @property
    def description(self) -> str:
        return "Description of what this check does"
    
    @property
    def severity(self) -> CheckSeverity:
        return CheckSeverity.MEDIUM
    
    def check_rule(self, rule: etree._Element, context: Dict[str, Any]) -> List[CheckResult]:
        results = []
        
        # Your custom logic here
        rule_name = self.get_rule_name(rule)
        
        # Example: Check for specific condition
        if self.has_any_value(rule, ".//source/member"):
            results.append(CheckResult(
                rule_name=rule_name,
                check_name=self.name,
                severity=self.severity,
                message="Custom security issue found",
                details={"custom_field": "custom_value"},
                file_path=context.get("file_path")
            ))
        
        return results

# Register the custom check
check_registry.register(CustomSecurityCheck())
```

##  CI/CD Integration

### GitHub Actions

```yaml
name: Security Analysis
on: [push, pull_request]

jobs:
  firewall-analysis:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install Policy Guardian
        run: pip install policy-guardian
      
      - name: Analyze Firewall Configs
        run: |
          policy-guardian analyze configs/*.xml \
            --output json \
            --save analysis-results.json \
            --severity medium
      
      - name: Upload Results
        uses: actions/upload-artifact@v3
        with:
          name: firewall-analysis
          path: analysis-results.json
```

### Jenkins Pipeline

```groovy
pipeline {
    agent any
    
    stages {
        stage('Firewall Analysis') {
            steps {
                sh 'pip install policy-guardian'
                sh '''
                    policy-guardian analyze firewall-configs/*.xml \
                        --output json \
                        --save ${WORKSPACE}/analysis-results.json
                '''
                archiveArtifacts artifacts: 'analysis-results.json'
            }
        }
    }
}
```

##  Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Quick Contribution Workflow

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes and add tests
4. Run the test suite: `pytest`
5. Run code quality checks: `pre-commit run --all-files`
6. Submit a pull request

### Reporting Issues

Please use the [GitHub issue tracker](https://github.com/ctaho19/policy-guardian/issues) to report bugs or request features. Include:

- Policy Guardian version
- Python version and operating system
- Sample configuration file (sanitized)
- Expected vs actual behavior
- Full error messages

##  License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

##  Acknowledgments

- Inspired by [@moshekplan's palo_alto_firewall_analyzer](https://github.com/moshekaplan/palo_alto_firewall_analyzer)

##  Links

- **Documentation**: [https://policy-guardian.readthedocs.io](https://policy-guardian.readthedocs.io)
- **PyPI Package**: [https://pypi.org/project/policy-guardian/](https://pypi.org/project/policy-guardian/)
- **GitHub Repository**: [https://github.com/ctaho19/policy-guardian](https://github.com/ctaho19/policy-guardian)
- **Issue Tracker**: [https://github.com/ctaho19/policy-guardian/issues](https://github.com/ctaho19/policy-guardian/issues)
- **Changelog**: [CHANGELOG.md](CHANGELOG.md)

---

<div align="center">
  <strong>Made with ❤️ for the cybersecurity community</strong>
</div> 