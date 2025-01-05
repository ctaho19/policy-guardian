-- Tier 1:
-- CTE to calculate certificate statistics
WITH CERT_STATUS AS (
    SELECT
        -- Count certificates with NULL expiration timestamps
        SUM(CASE WHEN NOT_VALID_ATER_UTC_TIMESTAMP IS NULL THEN 1 ELSE 0 END) AS NULL_CERTS,
        -- Count total unique certificates
        COUNT(DISTINCT CERTIFICATE_ID) AS TOTAL_CERTS
        FROM CYBR_DB.PHDP_CYBR.CERTIFICATE_CATALOG_CERTIFICATE_USAGE
        -- Filter for only ACM certificates
        WHERE CERTIFICATE_ARN LIKE '%arn:aws:acm%'
)
SELECT
    CURRENT_DATE AS DATE,
    'MNTR-1077188-T1' AS MONITORING_METRIC_NUMBER,
    -- Calculate percentage of certificates with valid expiration dates
    -- Returns 0 if all certificates have expiration dates
    CASE WHEN NULL_CERTS > 0 THEN ROUND (((TOTAL_CERTS - NULL_CERTS) * 100.0 / TOTAL_CERTS), 2)
    ELSE 0 END AS MONITORING_METRIC,
    -- RED if any certificates are missing expiration dates
    -- GREEN if all certificates have expiration dates
    CASE WHEN NULL_CERTS > 0 THEN 'RED'
    ELSE 'GREEN' END AS COMPLIANCE_STATUS,
    -- Number of certificates with valid expiration dates
    TOTAL_CERTS - NULL_CERTS AS NUMERATOR,
    -- Total number of certificates
    TOTAL_CERTS AS DENOMINATOR
    FROM CERT_STATUS;