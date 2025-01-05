-- Tier 1 Supporting Evidence
-- This query provides detailed evidence about bucket scanning coverage and status
WITH LATEST_SCAN_INFO AS (
    -- Get the most recent scan metrics and status
    SELECT
        METRIC_DATE,
        SF_LOAD_TIMESTAMP,
        TOTAL_BUCKETS_SCANNED_BY_MACIE AS SCANNED_BUCKETS,
        TOTAL_CLOUDFRONTED_BUCKETS AS TOTAL_BUCKETS,
        -- Determine if scan data is current
        CASE 
            WHEN DATE(SF_LOAD_TIMESTAMP) >= DATEADD(DAY, -1, CURRENT_DATE()) THEN 'Current'
            ELSE 'Not Current'
        END AS SCAN_STATUS
    FROM CYBR_DB.PHDP_CYBR.MACIE_METRICS_V3
    WHERE SF_LOAD_TIMESTAMP = (
        -- Get the most recent load timestamp
        SELECT MAX(SF_LOAD_TIMESTAMP) 
        FROM CYBR_DB.PHDP_CYBR.MACIE_METRICS_V3
    )
)
-- Output detailed scan coverage information
SELECT
    SCANNED_BUCKETS,                   -- Number of buckets scanned
    TOTAL_BUCKETS,                     -- Total number of CloudFronted buckets
    -- Calculate percentage of buckets scanned
    ROUND(100.0 * SCANNED_BUCKETS / NULLIF(TOTAL_BUCKETS, 0), 2) AS SCAN_PERCENTAGE,
    METRIC_DATE AS SCAN_DATE_USED,     -- Date metrics were collected
    SF_LOAD_TIMESTAMP AS LAST_UPDATED, -- When data was last loaded
    SCAN_STATUS                        -- Whether scan data is current
FROM LATEST_SCAN_INFO;