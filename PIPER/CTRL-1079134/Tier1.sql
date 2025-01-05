--Tier 1 (Completeness) Main Query
-- This query measures what percentage of CloudFronted buckets are being scanned by Macie
WITH LATEST_SCAN_INFO AS (
    -- Get the most recent scan metrics
    SELECT 
        METRIC_DATE,
        SF_LOAD_TIMESTAMP,
        TOTAL_BUCKETS_SCANNED_BY_MACIE,
        TOTAL_CLOUDFRONTED_BUCKETS,
        -- Determine if scan data is current
        CASE 
            WHEN DATE(SF_LOAD_TIMESTAMP) >= DATEADD(DAY, -1, CURRENT_DATE()) THEN TRUE 
            ELSE FALSE 
        END AS IS_CURRENT
    FROM CYBR_DB.PHDP_CYBR.MACIE_METRICS_V3
    WHERE SF_LOAD_TIMESTAMP = (
        -- Get the most recent load timestamp
        SELECT MAX(SF_LOAD_TIMESTAMP) 
        FROM CYBR_DB.PHDP_CYBR.MACIE_METRICS_V3
    )
)
-- Calculate completeness metrics and determine compliance status
SELECT
    CURRENT_DATE() AS DATE,
    'MNTR-1079134-T1' AS MONITORING_METRIC_NUMBER,
    -- Calculate percentage of buckets scanned if data is current, otherwise 0
    CASE 
        WHEN IS_CURRENT THEN 
            ROUND(100.0 * TOTAL_BUCKETS_SCANNED_BY_MACIE / NULLIF(TOTAL_CLOUDFRONTED_BUCKETS, 0), 2)
        ELSE 0
    END AS MONITORING_METRIC,
    -- Determine compliance status based on scan coverage
    CASE 
        WHEN NOT IS_CURRENT THEN 'RED'                                                            -- Not current = RED
        WHEN (TOTAL_BUCKETS_SCANNED_BY_MACIE / NULLIF(TOTAL_CLOUDFRONTED_BUCKETS, 0)) >= 0.9 THEN 'GREEN'  -- >=90% = GREEN
        WHEN (TOTAL_BUCKETS_SCANNED_BY_MACIE / NULLIF(TOTAL_CLOUDFRONTED_BUCKETS, 0)) >= 0.8 THEN 'YELLOW' -- >=80% = YELLOW
        ELSE 'RED'                                                                                -- <80% = RED
    END AS COMPLIANCE_STATUS,
    TOTAL_BUCKETS_SCANNED_BY_MACIE AS NUMERATOR,
    TOTAL_CLOUDFRONTED_BUCKETS AS DENOMINATOR
FROM LATEST_SCAN_INFO;