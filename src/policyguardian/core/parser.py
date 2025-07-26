from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator, Union

from lxml import etree

from .exceptions import ConfigurationError, UnsupportedFormatError

logger = logging.getLogger(__name__)


class PaloAltoConfigParser:
    def __init__(self) -> None:
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        self._parser = etree.XMLParser(
            resolve_entities=False,
            no_network=True,
            remove_comments=True,
            remove_pis=True,
            strip_cdata=False,
            huge_tree=False,
        )

    def parse(self, file_path: Union[str, Path]) -> etree._Element:
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise ConfigurationError(f"Configuration file not found: {file_path}")
        
        if not file_path.is_file():
            raise ConfigurationError(f"Path is not a file: {file_path}")
        
        if file_path.suffix.lower() not in {'.xml'}:
            raise UnsupportedFormatError(
                f"Unsupported file extension: {file_path.suffix}. "
                "Only .xml files are supported."
            )
        
        try:
            self.logger.info(f"Parsing configuration file: {file_path}")
            
            with open(file_path, 'rb') as f:
                max_size = 100 * 1024 * 1024
                content = f.read(max_size)
                
                if f.read(1):
                    raise ConfigurationError(
                        f"Configuration file too large (>100MB): {file_path}"
                    )
            
            root = etree.fromstring(content, parser=self._parser)
            
            self._validate_palo_alto_format(root, file_path)
            
            self.logger.info(f"Successfully parsed configuration: {file_path}")
            return root
            
        except etree.XMLSyntaxError as e:
            raise ConfigurationError(f"Invalid XML syntax in {file_path}: {e}")
        except Exception as e:
            if isinstance(e, (ConfigurationError, UnsupportedFormatError)):
                raise
            raise ConfigurationError(f"Failed to parse {file_path}: {e}")

    def parse_iterative(self, file_path: Union[str, Path]) -> Iterator[etree._Element]:
        file_path = Path(file_path)
        
        try:
            self.logger.info(f"Starting iterative parse of: {file_path}")
            
            context = etree.iterparse(
                str(file_path),
                events=('start', 'end'),
                parser=self._parser
            )
            
            root = None
            rule_count = 0
            
            for event, elem in context:
                if event == 'start' and root is None:
                    root = elem
                    self._validate_palo_alto_format(root, file_path)
                
                if (event == 'end' and 
                    elem.tag == 'entry' and 
                    self._is_security_rule(elem)):
                    
                    rule_count += 1
                    yield elem
                    
                    elem.clear()
                    
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]
            
            self.logger.info(f"Processed {rule_count} rules from {file_path}")
            
        except etree.XMLSyntaxError as e:
            raise ConfigurationError(f"Invalid XML syntax in {file_path}: {e}")
        except Exception as e:
            if isinstance(e, (ConfigurationError, UnsupportedFormatError)):
                raise
            raise ConfigurationError(f"Failed to parse {file_path}: {e}")

    def get_security_rules(self, root: etree._Element) -> list[etree._Element]:
        try:
            rule_xpaths = [
                ".//rulebase/security/rules/entry",
                ".//security/rules/entry", 
                ".//rules/entry",
                ".//entry[action]",
            ]
            
            for xpath in rule_xpaths:
                rules = root.xpath(xpath)
                if rules:
                    self.logger.debug(f"Found {len(rules)} rules using xpath: {xpath}")
                    security_rules = [rule for rule in rules if self._is_security_rule(rule)]
                    self.logger.info(f"Extracted {len(security_rules)} security rules")
                    return security_rules
            
            self.logger.warning("No security rules found in configuration")
            return []
            
        except Exception as e:
            raise ConfigurationError(f"Failed to extract security rules: {e}")

    def _validate_palo_alto_format(self, root: etree._Element, file_path: Path) -> None:
        if root is None:
            raise ConfigurationError(f"Empty XML document: {file_path}")
        
        palo_alto_indicators = [
            ".//rulebase",
            ".//security", 
            ".//rules",
            ".//devices",
            ".//entry[@name]",
        ]
        
        found_indicators = sum(1 for xpath in palo_alto_indicators if root.xpath(xpath))
        
        if found_indicators == 0:
            raise UnsupportedFormatError(
                f"File does not appear to be a Palo Alto configuration: {file_path}"
            )
        
        self.logger.debug(f"Found {found_indicators} Palo Alto format indicators")

    def _is_security_rule(self, elem: etree._Element) -> bool:
        if elem.tag != "entry":
            return False
        
        security_rule_elements = ["action", "from", "to", "source", "destination"]
        found_elements = sum(1 for child_name in security_rule_elements 
                           if elem.find(child_name) is not None)
        
        return found_elements >= 2 