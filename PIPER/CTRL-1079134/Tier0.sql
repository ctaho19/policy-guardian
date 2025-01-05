-- Tier 0 (Heartbeat) Metric
WITH LATEST_SCAN_INFO AS (
    -- Get the most recent scan info based on SF_LOAD_TIMESTAMP
    SELECT 
        METRIC_DATE,
        SF_LOAD_TIMESTAMP,
        -- Consider scan current if timestamp is from today or yesterday
        CASE 
            WHEN DATE(SF_LOAD_TIMESTAMP) >= DATEADD(DAY, -1, CURRENT_DATE()) THEN TRUE 
            ELSE FALSE 
        END AS IS_CURRENT,
        TOTAL_BUCKETS_SCANNED_BY_MACIE AS BUCKETS_SCANNED
    FROM CYBR_DB.PHDP_CYBR.MACIE_METRICS_V3
    -- Get the row with the most recent load timestamp
    WHERE SF_LOAD_TIMESTAMP = (
        SELECT MAX(SF_LOAD_TIMESTAMP) 
        FROM CYBR_DB.PHDP_CYBR.MACIE_METRICS_V3
    )
)
SELECT 
    CURRENT_DATE() AS DATE,
    'MNTR-1079134-T0' AS MONITORING_METRIC_NUMBER,
    CASE 
        WHEN IS_CURRENT = TRUE AND BUCKETS_SCANNED > 0 THEN 100
        ELSE 0
    END AS MONITORING_METRIC,
    CASE 
        WHEN IS_CURRENT = TRUE AND BUCKETS_SCANNED > 0 THEN 'GREEN'
        ELSE 'RED'
    END AS COMPLIANCE_STATUS,
    BUCKETS_SCANNED AS NUMERATOR,
    1 AS DENOMINATOR
FROM LATEST_SCAN_INFO;