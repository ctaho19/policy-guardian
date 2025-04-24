--evaluated_roles:
-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
select distinct
    upper(resource_name) as resource_name,
    compliance_status,
    control_id,
    role_type,
    create_date,
    current_timestamp() as load_timestamp
from EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_CONTROLS_VIOLATIONS_STREAM_V2
where control_id = %(control_id)s
    and date(create_date) = current_date
    and role_type = 'MACHINE' 

--iam_roles:
-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
SELECT DISTINCT
    RESOURCE_ID,
    UPPER(AMAZON_RESOURCE_NAME) as AMAZON_RESOURCE_NAME,
    BA,
    ACCOUNT,
    CREATE_DATE,
    TYPE,
    FULL_RECORD,
    ROLE_TYPE,
    SF_LOAD_TIMESTAMP,
    CURRENT_TIMESTAMP() as LOAD_TIMESTAMP
FROM EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_IAM_RESOURCE_V3
WHERE TYPE = 'role'
    AND AMAZON_RESOURCE_NAME LIKE 'arn:aws:iam::%role/%'
    AND NOT REGEXP_LIKE(UPPER(FULL_RECORD), '.(DENY[-]?ALL|QUARANTINEPOLICY).')
    AND DATE(SF_LOAD_TIMESTAMP) = CURRENT_DATE
    AND ROLE_TYPE = 'MACHINE'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY RESOURCE_ID
    ORDER BY CREATE_DATE DESC
) = 1 

--monitoring_thresholds:
-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
select
    monitoring_metric_id,
    control_id,
    monitoring_metric_tier,
    metric_name,
    metric_description,
    warning_threshold,
    alerting_threshold,
    current_timestamp() as load_timestamp
from CYBR_DB_COLLAB.LAB_ESRA_TCRD.CYBER_CONTROLS_MONITORING_THRESHOLD
where control_id = %(control_id)s 

--sla_data.sql
-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
SELECT RESOURCE_ID, CONTROL_RISK, OPEN_DATE_UTC_TIMESTAMP
FROM CLCN_DB.PHDP_CLOUD.OZONE_NON_COMPLIANT_RESOURCES_TCRD_VIEW_V01
WHERE CONTROL_ID = %(control_id)s
  AND RESOURCE_ID IN (%(resource_id_list)s)
  AND ID NOT IN (SELECT ID FROM CLCN_DB.PHDP_CLOUD.OZONE_CLOSED_NON_COMPLIANT_RESOURCES_V04) 


--yml:
pipeline:
  name: pl_automated_monitoring_machine_iam
  use_test_data_on_nonprod: false
  dq_strict_mode: false

stages:
  extract:
    thresholds_raw:
      connector: snowflake
      options:
        sql: "@text:sql/monitoring_thresholds.sql"
    iam_roles:
      connector: snowflake
      options:
        sql: "@text:sql/iam_roles.sql"
    evaluated_roles:
      connector: snowflake
      options:
        sql: "@text:sql/evaluated_roles.sql"

  transform:
    monitoring_metrics:
      function: custom/calculate_machine_iam_metrics
      options:
        thresholds_raw: $thresholds_raw
        iam_roles: $iam_roles
        evaluated_roles: $evaluated_roles

  load:
    monitoring_metrics:
      connector: onestream
      options:
        table_name: etip_controls_monitoring_metrics
        file_type: AVRO
        avro_schema: "@json:avro_schema.json"
        business_application: BAENTERPRISETECHINSIGHTS

ingress_validation:
  thresholds_raw:
    - type: count_check
      fatal: true
      envs:
        - qa
        - prod
      options:
        data_location: Snowflake
        threshold: 1
        table_name: CYBR_DB_COLLAB.LAB_ESRA_TCRD.CYBER_CONTROLS_MONITORING_THRESHOLD
  iam_roles:
    - type: count_check
      fatal: true
      envs:
        - qa
        - prod
      options:
        data_location: Snowflake
        threshold: 1
        table_name: EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_IAM_RESOURCE_V3
        date_filter: "DATE(SF_LOAD_TIMESTAMP) = CURRENT_DATE"
  evaluated_roles:
    - type: count_check
      fatal: true
      envs:
        - qa
        - prod
      options:
        data_location: Snowflake
        threshold: 1
        table_name: EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_CONTROLS_VIOLATIONS_STREAM_V2
        date_filter: "DATE(CREATE_DATE) = CURRENT_DATE"
