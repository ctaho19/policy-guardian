from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from lxml import etree

from .checks import CheckResult, SecurityCheck, check_registry
from .exceptions import ConfigurationError, ValidationError
from .parser import PaloAltoConfigParser

logger = logging.getLogger(__name__)


class PolicyAnalyzer:

    def __init__(self, parser: Optional[PaloAltoConfigParser] = None) -> None:
        self.parser = parser or PaloAltoConfigParser()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        self._available_checks = check_registry.get_all_checks()
        self.logger.info(f"Loaded {len(self._available_checks)} security checks")

    def analyze_file(
        self, 
        file_path: Union[str, Path], 
        check_names: Optional[List[str]] = None,
        use_iterative_parsing: bool = False
    ) -> List[CheckResult]:
        file_path = Path(file_path)
        self.logger.info(f"Starting analysis of {file_path}")
        
        try:
            checks_to_run = self._select_checks(check_names)
            
            if use_iterative_parsing:
                results = self._analyze_iteratively(file_path, checks_to_run)
            else:
                results = self._analyze_standard(file_path, checks_to_run)
            
            self.logger.info(f"Analysis complete. Found {len(results)} issues in {file_path}")
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to analyze {file_path}: {e}")
            raise

    def analyze_files(
        self, 
        file_paths: List[Union[str, Path]], 
        check_names: Optional[List[str]] = None,
        use_iterative_parsing: bool = False
    ) -> Dict[str, List[CheckResult]]:
        results = {}
        
        for file_path in file_paths:
            try:
                file_results = self.analyze_file(
                    file_path, 
                    check_names=check_names,
                    use_iterative_parsing=use_iterative_parsing
                )
                results[str(file_path)] = file_results
                
            except Exception as e:
                self.logger.error(f"Failed to analyze {file_path}: {e}")
                results[str(file_path)] = []
        
        return results

    def get_available_checks(self) -> Dict[str, str]:
        return {
            check_id: check.description 
            for check_id, check in self._available_checks.items()
        }

    def _select_checks(self, check_names: Optional[List[str]]) -> Dict[str, SecurityCheck]:
        if check_names is None:
            return self._available_checks.copy()
        
        selected_checks = {}
        for check_name in check_names:
            if check_name in self._available_checks:
                selected_checks[check_name] = self._available_checks[check_name]
            else:
                available = list(self._available_checks.keys())
                raise ValidationError(
                    f"Unknown check '{check_name}'. Available checks: {available}"
                )
        
        return selected_checks

    def _analyze_standard(self, file_path: Path, checks: Dict[str, SecurityCheck]) -> List[CheckResult]:
        root = self.parser.parse(file_path)
        
        rules = self.parser.get_security_rules(root)
        
        if not rules:
            self.logger.warning(f"No security rules found in {file_path}")
            return []
        
        # Run checks
        return self._run_checks_on_rules(rules, checks, {"file_path": str(file_path)})

    def _analyze_iteratively(self, file_path: Path, checks: Dict[str, SecurityCheck]) -> List[CheckResult]:
        results = []
        context = {"file_path": str(file_path)}
        
        all_rules = []
        for rule in self.parser.parse_iterative(file_path):
            all_rules.append(rule)
        
        if not all_rules:
            self.logger.warning(f"No security rules found in {file_path}")
            return []
        
        # Run checks
        return self._run_checks_on_rules(all_rules, checks, context)

    def _run_checks_on_rules(
        self, 
        rules: List[etree._Element], 
        checks: Dict[str, SecurityCheck], 
        context: Dict[str, Any]
    ) -> List[CheckResult]:
        all_results = []
        
        self.logger.info(f"Running {len(checks)} checks on {len(rules)} rules")
        
        for check_id, check in checks.items():
            try:
                self.logger.debug(f"Running check: {check_id}")
                
                if hasattr(check, 'check_all_rules'):
                    check_results = check.check_all_rules(rules, context)
                else:
                    check_results = []
                    for rule in rules:
                        rule_results = check.check_rule(rule, context)
                        check_results.extend(rule_results)
                
                all_results.extend(check_results)
                self.logger.debug(f"Check {check_id} found {len(check_results)} issues")
                
            except Exception as e:
                self.logger.error(f"Error running check {check_id}: {e}")
        
        all_results.sort(key=lambda r: (r.severity.value, r.rule_name))
        
        return all_results 