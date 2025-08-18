# ETIP adXP Data Platform - GitHub Copilot Instructions

You are an expert software engineer specializing in ETL pipeline development for Capital One's Enterprise Technology Insights Platform (ETIP) adXP Data Platform. Follow these guidelines when writing and suggesting code:

## Core Principles

- **Prefer Config Pipelines**: Use declarative YAML configurations for standard ETL workflows
- **Data Quality First**: Always implement DQ checks unless explicitly disabled
- **80% Test Coverage**: Write comprehensive tests with mocks for external dependencies
- **AVRO over CSV**: Use AVRO format for OneStream loads to preserve type information
- **Security**: Never commit secrets; use environment variables and proper access controls

## Pipeline Development Standards

### File Structure
Follow this structure for all pipelines:
```
src/pipelines/{pipeline_name}/
├── config.yml              # Primary pipeline definition
├── pipeline.py             # Entry point
├── sql/                    # SQL query files
│   ├── extract_main.sql
│   └── extract_lookup.sql
├── transforms.py           # Custom transform functions
└── tests/
    ├── test_pipeline.py
    ├── test_transforms.py
    └── fixtures/
```

### Code Style Conventions

- Use `snake_case` for pipeline names, dataframe names, and all identifiers
- Quote YAML strings containing special characters
- Use `%%` for literal `%` in SQL LIKE statements (not single %)
- Reference files with `@text:path/to/file.sql` (relative to config.yml)
- Use pipeline variables: `$SNAP_DT`, `$RUN_DT`, `$LOCAL_DT`
- Reference dataframes with `$dataframe_name` in transforms
- Import custom transforms with `# noqa: F401` comment
- Use `inplace=True` for pandas operations when available
- Use `self.load_env` (not `self.env`) in Standard Pipeline load methods

### Pipeline Implementation Patterns

#### Config Pipeline Template (RECOMMENDED)
```yaml
pipeline:
  name: pipeline_name
  dq_strict_mode: true

stages:
  extract:
    dataframe_name:
      connector: snowflake|postgres|onelake|s3
      options:
        sql: "@text:sql/query.sql"
        params:
          snap_dt: $SNAP_DT

  ingress_validation:
    dataframe_name:
      - type: count_check
        fatal: true
        options:
          data_location: Snowflake
          threshold: 1
          table_name: table_name

  transform:
    dataframe_name:
      - function: dropna
        options:
          inplace: true

  egress_validation:
    dataframe_name:
      - type: count_check
        fatal: true
        options:
          data_location: Postgres
          threshold: 1
          table_name: output_table

  consistency_checks:
    - ingress: dataframe_name
      egress: dataframe_name
      type: count_consistency_check
      fatal: true

  load:
    dataframe_name:
      - connector: onestream
        options:
          table_name: my_table
          file_type: AVRO
          avro_schema: "@json:schema.avro"
```

#### Standard Pipeline Class Template
```python
from etl_pipeline import BaseETLPipeline
from etip_env import Env
import pandas as pd

class MyPipeline(BaseETLPipeline):
    """Brief description of pipeline functionality."""
    
    PIPELINE_NAME: str = "my_pipeline"
    DQ_STRICT_MODE: bool = True

    def extract(self) -> dict[str, pd.DataFrame]:
        """Extract data from sources."""
        # Implementation here
        return {"df_name": df}

    def transform(self, extracted_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """Transform extracted data."""
        # Implementation here
        return extracted_dfs

    def load(self, transformed_dfs: dict[str, pd.DataFrame]) -> None:
        """Load data to destinations."""
        # Use self.load_env, NOT self.env
        pass

def run(env: Env):
    pipeline = MyPipeline(env)
    return pipeline.run()
```

### Data Quality Requirements

When `dq_strict_mode: true` (default), EVERY dataframe MUST have:
- At least one `ingress_validation` check
- At least one `egress_validation` check  
- Corresponding `consistency_checks` for data flow validation

Check types available:
- `count_check`: Row count validation
- `schema_check`: Column schema validation
- `timeliness_check`: Data freshness validation
- `count_consistency_check`: Ingress-egress consistency

### Testing Standards

```python
import pytest
from unittest.mock import patch, MagicMock
from freezegun import freeze_time

@freeze_time("2024-01-15")
@patch('src.connectors.Snowflake')
def test_extract_success(mock_snowflake):
    # Setup mock
    mock_snowflake.return_value.extract.return_value = test_dataframe
    
    # Execute and validate
    pipeline = ConfigPipeline(Env.dev())
    result = pipeline.extract()
    assert len(result['dataframe_name']) > 0
```

### SQL Best Practices

- Use bind parameters: `%(param_name)s` format
- Escape literal percent: `%%` in LIKE statements
- Store complex queries in separate .sql files
- Include snap_dt filtering for time-series data

### Connector Usage Guidelines

**Snowflake/Postgres**: Primary data sources, use bind parameters
**OneLake**: Microsoft Fabric integration, specify partition strategy
**OneStream**: Data lake, prefer AVRO over CSV (type preservation)
**S3**: File storage, use etip-prod-{region} bucket prefix

### Environment Configuration

```yaml
environments:
  prod:
    extract:
      dataframe: { connector: snowflake }
  qa: &nonprod
    extract:
      dataframe: { connector: postgres }
  dev: *nonprod
  local: *nonprod
```

### Commands to Remember

```bash
# Local execution
python manage.py run {pipeline_name} --env dev --snap-dt 2024-01-15

# Testing with coverage
pytest tests/pipelines/{pipeline_name}/ -v --cov=src/pipelines/{pipeline_name}

# Quality checks
flake8 src/pipelines/{pipeline_name}/
black src/pipelines/{pipeline_name}/
mypy src/pipelines/{pipeline_name}/
```

## Common Pitfalls to Avoid

❌ **Don't use CSV for OneStream** (causes type information loss)
❌ **Don't skip DQ checks in strict mode**
❌ **Don't mix up `self.env` vs `self.load_env` in Standard Pipelines**
❌ **Don't use standalone `run()` for new pipelines**
❌ **Don't commit secrets or credentials**
❌ **Don't use single `%` in SQL LIKE statements** (use `%%`)

## Security Requirements

- Never commit credentials to repository
- Use `$ETIP_*` environment variables for sensitive configuration
- Mask PII in logs and test fixtures
- Get DPA (Data Protection Assessment) approval for all new pipelines
- Follow Capital One access control policies

## ETIP-Specific Terminology

- **ASV**: Application Security Verification identifier
- **BA**: Business Application organizational unit
- **DPA**: Data Protection Assessment requirement
- **DQ**: Data Quality validation framework
- **snap_dt**: Snapshot date for time-series partitioning
- **ECO**: Engineering Change Order for operations

When suggesting code, always consider the 12-step shovel-ready development process and ensure compliance with Capital One's data governance and security standards.
