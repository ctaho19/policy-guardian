-- Tier 0 Supporting Evidence
WITH LATEST_SCAN_INFO AS (
    -- Get the most recent scan information based on the latest SF_LOAD_TIMESTAMP
    SELECT
        METRIC_DATE,
        SF_LOAD_TIMESTAMP,
        TOTAL_BUCKETS_SCANNED_BY_MACIE AS TOTAL_SCANNED_BUCKETS,
        -- Determine if scan data is current (within last day) or not
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
-- Output the scan details and status
SELECT
    TOTAL_SCANNED_BUCKETS,             -- Number of buckets scanned by Macie
    METRIC_DATE AS SCAN_DATE_USED,     -- Date the metric was collected
    SF_LOAD_TIMESTAMP AS LAST_UPDATED, -- When the data was last loaded
    SCAN_STATUS                        -- Whether scan data is current or not
FROM LATEST_SCAN_INFO;