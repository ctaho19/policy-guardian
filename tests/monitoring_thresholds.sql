-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
select
    monitoring_metric_id,
    control_id,
    monitoring_metric_tier,
    metric_name,
    metric_description,
    warning_threshold,
    alerting_threshold, -- Renamed from ALERT_THRESHOLD
    control_executor,
    metric_threshold_start_date,
    metric_threshold_end_date
from
    etip_db.phdp_etip_controls_monitoring.etip_controls_monitoring_metrics_details
where 
    metric_threshold_end_date is Null
    -- Filter for the specific metrics related to CTRL-1077231
    and monitoring_metric_id in ('MNTR-1077231-T1', 'MNTR-1077231-T2') 
    # Alternatively, filter by control_id if that's more standard:
    # and control_id = 'CTRL-1077231' 