-- Tier 2 (Accuracy) Metric
-- Calculate statistics for compute group certifications
WITH REVIEW_STATS AS (
    SELECT 
        COUNT(*) AS TOTAL_ROLES, -- Total number of roles being reviewed
        -- Count roles that have are not past due review period at 30/60/90 day thresholds
        SUM(CASE WHEN CERT_PAST_DUE_30_DAYS = 0 AND CERT_PAST_DUE_60_DAYS = 0 AND CERT_PAST_DUE_90_DAYS = 0 THEN 1 ELSE 0 END) AS COMPLIANT_ROLES
        FROM (
            SELECT CERT.*,
            -- Flag certifications past due at different thresholds (30/60/90 days)
            -- Status codes 0-3 represent non-completed states
            CASE WHEN CERT.STATUS IN (0, 1, 2, 3) AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.CREATED_DATE), CURRENT_TIMESTAMP()) >= 30 THEN 1 ELSE 0 END AS CERT_PAST_DUE_30_DAYS,
            CASE WHEN CERT.STATUS IN (0, 1, 2, 3) AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.CREATED_DATE), CURRENT_TIMESTAMP()) >= 60 THEN 1 ELSE 0 END AS CERT_PAST_DUE_60_DAYS,
            CASE WHEN CERT.STATUS IN (0, 1, 2, 3) AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.CREATED_DATE), CURRENT_TIMESTAMP()) >= 90 THEN 1 ELSE 0 END AS CERT_PAST_DUE_90_DAYS.
            -- Flag to exclude in-remediation certifications (status 6)
            CASE WHEN CERT.STATUS = 6 THEN 0 ELSE 1 END AS NOT_IN_REMEDIATION
        FROM eiam_db.phap_eram_11acapt.aa_microcertification cert
        -- Filter for compute group certifications within the last year
        WHERE CERT.CERTIFICATION_TYPE = 'Compute Groups APR'
        AND TO_TIMESTAMP(CERT.CREATED_DATE) >= DATEADD(YEAR, -1, CURRENT_DATE())
        ) CERT
        WHERE NOT_IN_REMEDIATION = 1 -- Exclude in-remediation certifications
)
-- Generate final monitoring metric output
SELECT 
    CURRENT_DATE() AS DATE,
    'MNTR-XXXX-T2' AS MONITORING_METRIC_NUMBER,
    -- Calculate compliance percentage (compliant roles / total roles)
    ROUND(100.0 * COMPLIANT_ROLES / NULLIF(TOTAL_ROLES, 0), 2) AS MONITORING_METRIC,
    -- Determine compliance status:
    -- RED if any certifications have null status
    -- GREEN if all roles are compliant
    -- RED otherwise
    CASE WHEN EXISTS (SELECT 1 FROM EIAM_DB.PHAP_ERAM_11ACAPT.AA_MICROCERTIFICATION WHERE STATUS IS NULL AND CERTIFICATION_TYPE = 'Compute Groups APR' AND TO_TIMESTAMP(CREATED_DATE) >= DATEADD(YEAR, -1, CURRENT_DATE())) THEN 'RED' 
    WHEN (COMPLIANT_ROLES = TOTAL_ROLES) THEN 'GREEN'
    ELSE 'RED'
    END AS COMPLIANCE_STATUS,
    COMPLIANT_ROLES AS NUMERATOR,
    TOTAL_ROLES AS DENOMINATOR
FROM REVIEW_STATS;