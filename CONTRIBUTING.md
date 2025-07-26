# Contributing to Policy Guardian

Thank you for your interest in contributing to Policy Guardian! This document provides guidelines and information for contributors.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Contributing Process](#contributing-process)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Documentation](#documentation)
- [Security Policy](#security-policy)

## Code of Conduct

This project adheres to a [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Getting Started

### Types of Contributions

We welcome several types of contributions:

- **Bug Reports**: Found a bug? Let us know!
- **Feature Requests**: Have an idea for a new feature?
- **Code Contributions**: Bug fixes, new features, or improvements
- **Documentation**: Improve or add documentation
- **Security Checks**: Develop new security checks for firewall policies
- **Testing**: Help improve test coverage

### Before You Start

1. **Check existing issues**: Before creating a new issue, please search existing issues to avoid duplicates
2. **Discuss major changes**: For significant changes, please open an issue first to discuss the approach
3. **Review the roadmap**: Check our [project roadmap](https://github.com/ctaho19/policy-guardian/projects) to see planned features

## Development Setup

### Prerequisites

- Python 3.8 or higher
- Git
- A GitHub account

### Setup Instructions

1. **Fork the repository**
   ```bash
   # Click the "Fork" button on GitHub, then clone your fork
   git clone https://github.com/YOUR_USERNAME/policy-guardian.git
   cd policy-guardian
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install development dependencies**
   ```bash
   pip install -e ".[dev,gui,docs]"
   ```

4. **Install pre-commit hooks**
   ```bash
   pre-commit install
   ```

5. **Verify the setup**
   ```bash
   # Run tests
   pytest
   
   # Run code quality checks
   pre-commit run --all-files
   
   # Test CLI
   policy-guardian --version
   ```

## Contributing Process

### 1. Create a Branch

```bash
# Create and switch to a new branch
git checkout -b feature/your-feature-name

# Or for bug fixes
git checkout -b fix/issue-description
```

### 2. Make Your Changes

- Write clear, concise commit messages
- Include tests for new functionality
- Update documentation as needed
- Follow our coding standards (see below)

### 3. Test Your Changes

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=policyguardian --cov-report=html

# Run code quality checks
pre-commit run --all-files

# Test your changes manually
policy-guardian analyze sample-config.xml
```

### 4. Create a Pull Request

1. Push your branch to your fork
2. Create a pull request from your branch to the main repository
3. Fill out the pull request template
4. Wait for review and address any feedback

### Pull Request Guidelines

- **Title**: Use a clear, descriptive title
- **Description**: Explain what your changes do and why
- **Link issues**: Reference any related issues using `Fixes #123` or `Closes #123`
- **Screenshots**: Include screenshots for UI changes
- **Breaking changes**: Clearly mark any breaking changes

## Coding Standards

### Python Style

We follow PEP 8 with some modifications. Our automated tools enforce these standards:

- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Linting
- **mypy**: Type checking

### Type Hints

All new code must include type hints:

```python
from typing import List, Dict, Optional

def analyze_rules(rules: List[Rule], config: Dict[str, Any]) -> Optional[AnalysisResult]:
    """Analyze firewall rules and return results."""
    # Implementation here
    pass
```

### Documentation

- **Docstrings**: Use Google-style docstrings
- **Comments**: Explain complex logic and business rules
- **Type hints**: Include comprehensive type annotations

```python
def check_rule(self, rule: etree._Element, context: Dict[str, Any]) -> List[CheckResult]:
    """
    Check a firewall rule for security issues.
    
    Args:
        rule: XML element representing a firewall rule
        context: Additional context information (file path, etc.)
        
    Returns:
        List of CheckResult objects for any issues found
        
    Raises:
        ValidationError: If the rule element is invalid
    """
```

### Error Handling

- Use specific exception types
- Provide helpful error messages
- Log errors appropriately

```python
try:
    result = parse_config(file_path)
except ConfigurationError as e:
    self.logger.error(f"Failed to parse configuration: {e}")
    raise
```

## Testing

### Test Structure

- **Unit tests**: Test individual functions and classes
- **Integration tests**: Test component interactions
- **End-to-end tests**: Test complete workflows

### Writing Tests

```python
import pytest
from policyguardian.checks.overly_permissive import OverlyPermissiveRulesCheck

class TestOverlyPermissiveRulesCheck:
    """Test overly permissive rules check."""

    def test_detects_permissive_rule(self, sample_rule_element):
        """Test that overly permissive rules are detected."""
        check = OverlyPermissiveRulesCheck()
        results = check.check_rule(sample_rule_element, {"file_path": "test.xml"})
        
        assert len(results) == 1
        assert results[0].severity == CheckSeverity.HIGH
```

### Test Coverage

- Aim for >90% test coverage
- Test both positive and negative cases
- Test error conditions and edge cases

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_checks.py

# Run with coverage
pytest --cov=policyguardian --cov-report=html

# Run tests for specific Python version
tox -e py311
```

## Documentation

### Types of Documentation

1. **Code documentation**: Docstrings and comments
2. **User documentation**: README, tutorials, examples
3. **API documentation**: Auto-generated from docstrings
4. **Developer documentation**: This file and other guides

### Building Documentation

```bash
cd docs
make html
open _build/html/index.html
```

### Documentation Standards

- Keep documentation up-to-date with code changes
- Use clear, concise language
- Include examples where helpful
- Link to related sections

## Security Policy

### Reporting Security Issues

Please do not report security vulnerabilities through public GitHub issues. Instead:

1. Email us at: security@policy-guardian.dev
2. Include a detailed description of the vulnerability
3. Provide steps to reproduce the issue
4. Allow us reasonable time to fix the issue before public disclosure

### Security Best Practices

- Never commit sensitive information (API keys, passwords, etc.)
- Follow secure coding practices
- Validate all inputs
- Use parameterized queries for any database operations
- Keep dependencies up-to-date

## Creating Security Checks

Policy Guardian's extensible architecture allows you to create custom security checks:

### Check Development Template

```python
from policyguardian.core.checks import SecurityCheck, CheckResult, CheckSeverity, check_registry
from typing import List, Dict, Any
from lxml import etree

class YourSecurityCheck(SecurityCheck):
    """
    Brief description of what this check does.
    
    Detailed explanation of the security issue this check identifies,
    why it's important, and what to do about it.
    """

    @property
    def name(self) -> str:
        return "Your Security Check"

    @property
    def description(self) -> str:
        return "Detailed description of the check's purpose and function"

    @property
    def severity(self) -> CheckSeverity:
        return CheckSeverity.MEDIUM  # Choose appropriate default severity

    def check_rule(self, rule: etree._Element, context: Dict[str, Any]) -> List[CheckResult]:
        """
        Implement your check logic here.
        
        Args:
            rule: The firewall rule to check
            context: Additional context (file path, etc.)
            
        Returns:
            List of issues found
        """
        results = []
        
        try:
            self.validate_rule_element(rule)
            rule_name = self.get_rule_name(rule)
            
            # Your check logic here
            if self._has_security_issue(rule):
                results.append(CheckResult(
                    rule_name=rule_name,
                    check_name=self.name,
                    severity=self.severity,
                    message="Description of the specific issue found",
                    details={
                        "field": "value",
                        "recommendation": "What the user should do"
                    },
                    file_path=context.get("file_path")
                ))
                
        except Exception as e:
            self.logger.error(f"Error checking rule {rule.get('name', 'unknown')}: {e}")
            
        return results

    def _has_security_issue(self, rule: etree._Element) -> bool:
        """Helper method to check for specific security issues."""
        # Implement your logic here
        return False

# Register the check
check_registry.register(YourSecurityCheck())
```

### Check Guidelines

1. **Single responsibility**: Each check should focus on one specific security issue
2. **Clear naming**: Use descriptive names that explain what the check does
3. **Comprehensive testing**: Include tests for various rule configurations
4. **Performance**: Ensure checks are efficient for large configurations
5. **Documentation**: Document the security implications and remediation steps

## Release Process

### Version Numbering

We use [Semantic Versioning](https://semver.org/):

- **Major version**: Breaking changes
- **Minor version**: New features (backward compatible)
- **Patch version**: Bug fixes (backward compatible)

### Release Checklist

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Run full test suite
4. Create release PR
5. Tag release after merge
6. GitHub Actions handles PyPI publication

## Getting Help

### Communication Channels

- **GitHub Issues**: Bug reports and feature requests
- **GitHub Discussions**: General questions and discussions
- **Email**: security@policy-guardian.dev (security issues only)

### Resources

- [Python Developer's Guide](https://devguide.python.org/)
- [GitHub Flow](https://guides.github.com/introduction/flow/)
- [Semantic Versioning](https://semver.org/)
- [Keep a Changelog](https://keepachangelog.com/)

Thank you for contributing to Policy Guardian! 🚀 