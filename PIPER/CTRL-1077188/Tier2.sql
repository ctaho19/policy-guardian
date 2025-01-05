-- Tier 2 (Accuracy)
-- CTE to calculate certificate compliance statistics
WITH CERT_STATUS AS (
    SELECT
        -- Count certificates where expiration date is after last observation (compliant)
        COUNT(DISTINCT CASE WHEN NOT_VALID_ATER_UTC_TIMESTAMP >= LAST_USAGE_OBSERVATION_UTC_TIMESTAMP 
            THEN CERTIFICATE_ID ELSE NULL END) AS COMPLIANT_CERTS,
        -- Count total certificates issued within the last year
        COUNT(DISTINCT CASE WHEN NOT_VALID_AFTER_UTC_TIMESTAMP >= DATEADD('DAY', -365, CURRENT_TIMESTAMP) 
            THEN CERTIFICATE_ID ELSE NULL END) AS TOTAL_CERTS
    FROM CYBR_DB.PHDP_CYBR.CERTIFICATE_CATALOG_CERTIFICATE_USAGE
    -- Filter for only ACM certificates
    WHERE CERTIFICATE_ARN LIKE '%arn:aws:acm%'
    -- Only include certificates from the last year
    AND NOT_VALID_ATER_UTC_TIMESTAMP >= DATEADD('DAY', -365, CURRENT_TIMESTAMP)
)
SELECT
    CURRENT_DATE AS DATE,
    'MNTR-1077188-T2' AS MONITORING_METRIC_NUMBER,
    -- Calculate percentage of compliant certificates (rounded to 2 decimal places)
    ROUND(CAST(COMPLIANT_CERTS AS FLOAT) / CAST(TOTAL_CERTS AS FLOAT) * 100, 2) AS MONITORING_METRIC,
    -- RED if no compliant certificates, GREEN otherwise
    CASE WHEN COMPLIANT_CERTS = 0 THEN 'RED' ELSE 'GREEN' END AS COMPLIANCE_STATUS,
    -- Number of compliant certificates
    COMPLIANT_CERTS AS NUMERATOR,
    -- Total number of certificates
    TOTAL_CERTS AS DENOMINATOR
FROM CERT_STATUS;