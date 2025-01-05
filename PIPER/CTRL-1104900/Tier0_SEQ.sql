--Tier 0 Supporting Evidence Query
-- Purpose: Provides additional context about review initiation patterns
WITH LAST_YEAR_START AS (
    -- Calculate start of previous calendar year
    SELECT DATEADD(YEAR, -1, DATE_TRUNC('YEAR', CURRENT_DATE())) AS START_DATE
)
SELECT 
    COUNT(*) AS TOTAL_REVIEWS_INITIATED,                                          -- Total number of reviews started
    MIN(TO_TIMESTAMP(cert.certification_opened_date)) AS EARLIEST_REVIEW_DATE,    -- First review date
    MAX(TO_TIMESTAMP(cert.certification_opened_date)) AS LATEST_REVIEW_DATE       -- Most recent review date
FROM eiam_db.phap_eram_11acapt.aa_microcertification cert
CROSS JOIN LAST_YEAR_START
WHERE cert.certification_type = 'Compute Groups APR'
  AND TO_TIMESTAMP(cert.certification_opened_date) >= START_DATE;