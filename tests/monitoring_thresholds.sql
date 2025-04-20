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
    control_executor,
    metric_threshold_start_date,
    metric_threshold_end_date
from
    etip_db.phdp_etip_controls_monitoring.etip_controls_monitoring_metrics_details
where 
    metric_threshold_end_date is Null
    and control_id = 'CTRL-1077231'
