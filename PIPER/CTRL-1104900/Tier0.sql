-- Tier 0 (Heartbeat) Metric
WITH ONE_YEAR_AGO AS (
    -- Calculate the date exactly one year ago from today
    SELECT DATEADD(YEAR, -1, CURRENT_DATE()) AS START_DATE
)
SELECT 
    CURRENT_DATE() AS DATE,
    'MNTR-XXXX-T0' AS MONITORING_METRIC_NUMBER,
    -- Metric is 1 if any reviews exist, 0 if none
    CASE 
        WHEN COUNT(*) > 0 THEN 1
        ELSE 0
    END AS MONITORING_METRIC,
    -- Compliance is GREEN if any reviews exist, RED if none
    CASE 
        WHEN COUNT(*) > 0 THEN 'GREEN'
        ELSE 'RED'
    END AS COMPLIANCE_STATUS,
    -- Numerator shows count of reviews initiated in past year
    TO_DOUBLE(ROUND(COUNT(CASE WHEN TO_TIMESTAMP(cert.certification_opened_date) >= START_DATE THEN 1 END),2)) AS NUMERATOR,
    1 AS DENOMINATOR
FROM eiam_db.phap_eram_11acapt.aa_microcertification cert
CROSS JOIN ONE_YEAR_AGO
WHERE cert.certification_type = 'Compute Groups APR'  -- Filter for compute group reviews only
  AND TO_TIMESTAMP(cert.certification_opened_date) >= START_DATE;  -- Within last year