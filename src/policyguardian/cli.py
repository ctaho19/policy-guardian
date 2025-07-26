from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import List, Optional

import click

from . import PolicyAnalyzer, __version__
from .checks import get_available_checks
from .core.checks import CheckSeverity
from .core.exceptions import PolicyGuardianError


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def print_results_table(results: List, file_path: str) -> None:
    if not results:
        click.echo(f"✅ No issues found in {file_path}")
        return
    
    click.echo(f"\n📋 Analysis Results for {file_path}")
    click.echo("=" * 80)
    
    severity_groups = {}
    for result in results:
        sev = result.severity.value
        if sev not in severity_groups:
            severity_groups[sev] = []
        severity_groups[sev].append(result)
    
    severity_order = ["critical", "high", "medium", "low"]
    severity_colors = {
        "critical": "bright_red",
        "high": "red", 
        "medium": "yellow",
        "low": "blue"
    }
    
    total_issues = len(results)
    click.echo(f"Found {total_issues} total issues:")
    
    for severity in severity_order:
        if severity not in severity_groups:
            continue
            
        issues = severity_groups[severity]
        color = severity_colors[severity]
        
        click.echo(f"\n{severity.upper()} ({len(issues)} issues):")
        click.echo("-" * 40)
        
        for issue in issues:
            click.echo(
                click.style(f"  • {issue.rule_name}: ", fg=color, bold=True) +
                issue.message
            )


def print_results_json(results: List) -> None:
    json_results = [result.to_dict() for result in results]
    click.echo(json.dumps(json_results, indent=2))


@click.group()
@click.version_option(version=__version__)
@click.option(
    "--verbose", "-v", 
    is_flag=True, 
    help="Enable verbose logging"
)
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    setup_logging(verbose)


@cli.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True, path_type=Path))
@click.option(
    "--checks", "-c",
    multiple=True,
    help="Specific checks to run (can be specified multiple times)"
)
@click.option(
    "--output", "-o",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format"
)
@click.option(
    "--severity", "-s",
    type=click.Choice(["low", "medium", "high", "critical"]),
    help="Filter results by minimum severity level"
)
@click.option(
    "--iterative", 
    is_flag=True,
    help="Use memory-efficient iterative parsing for large files"
)
@click.option(
    "--save", 
    type=click.Path(path_type=Path),
    help="Save results to JSON file"
)
@click.pass_context
def analyze(
    ctx: click.Context,
    files: tuple[Path, ...],
    checks: tuple[str, ...],
    output: str,
    severity: Optional[str],
    iterative: bool,
    save: Optional[Path]
) -> None:
    if not files:
        click.echo("❌ No files specified. Use --help for usage information.")
        sys.exit(1)
    
    try:
        analyzer = PolicyAnalyzer()
        
        check_list = list(checks) if checks else None
        
        if check_list:
            available_checks = analyzer.get_available_checks()
            invalid_checks = [c for c in check_list if c not in available_checks]
            if invalid_checks:
                click.echo(f"❌ Unknown checks: {', '.join(invalid_checks)}")
                click.echo(f"Available checks: {', '.join(available_checks.keys())}")
                sys.exit(1)
        
        all_results = []
        
        for file_path in files:
            try:
                results = analyzer.analyze_file(
                    file_path,
                    check_names=check_list,
                    use_iterative_parsing=iterative
                )
                
                if severity:
                    min_severity = CheckSeverity(severity)
                    severity_order = ["low", "medium", "high", "critical"]
                    min_level = severity_order.index(min_severity.value)
                    
                    results = [
                        r for r in results 
                        if severity_order.index(r.severity.value) >= min_level
                    ]
                
                all_results.extend(results)
                
                if output == "table":
                    print_results_table(results, str(file_path))
                elif output == "json":
                    if len(files) > 1:
                        click.echo(f"# Results for {file_path}")
                    print_results_json(results)
                    
            except PolicyGuardianError as e:
                click.echo(f"❌ Error analyzing {file_path}: {e}")
                if ctx.obj["verbose"]:
                    raise
                
        if save:
            json_results = [result.to_dict() for result in all_results]
            save.write_text(json.dumps(json_results, indent=2))
            click.echo(f"💾 Results saved to {save}")
        
        if len(files) > 1 and output == "table":
            total_issues = len(all_results)
            if total_issues > 0:
                click.echo(f"\n📊 Total: {total_issues} issues across {len(files)} files")
            else:
                click.echo(f"\n✅ No issues found across {len(files)} files")
                
    except KeyboardInterrupt:
        click.echo("\n⏹️  Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}")
        if ctx.obj["verbose"]:
            raise
        sys.exit(1)


@cli.command("list-checks")
def list_checks() -> None:
    analyzer = PolicyAnalyzer()
    available_checks = analyzer.get_available_checks()
    
    click.echo("📋 Available Security Checks:")
    click.echo("=" * 50)
    
    for check_id, description in available_checks.items():
        click.echo(f"\n{click.style(check_id, fg='green', bold=True)}")
        import textwrap
        wrapped_desc = textwrap.fill(description, width=70, initial_indent="  ", subsequent_indent="  ")
        click.echo(wrapped_desc)


@cli.command("version")
def version() -> None:
    click.echo(f"Policy Guardian v{__version__}")


def main() -> None:
    try:
        cli()
    except Exception as e:
        click.echo(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 