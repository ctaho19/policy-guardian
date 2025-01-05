-- Tier 3 (Past SLA) Metric
WITH REMEDIATION_STATS AS (
    SELECT
        COUNT(*) AS TOTAL_ROLES,
        SUM(CASE WHEN REMEDIATION_PAST_DUE_30_DAYS = 0 AND REMEDIATION_PAST_DUE_60_DAYS = 0 AND REMEDIATION_PAST_DUE_90_DAYS = 0 THEN 1 ELSE 0 END) AS COMPLIANT_ROLES
        FROM
            (
                SELECT CERT.*,
                CASE WHEN CERT.STATUS IN (0, 1, 2, 3) AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.TARGET_REMEDIATION_DATE), CURRENT_TIMESTAMP()) >= 30 THEN 1 ELSE 0 END AS CERT_PAST_DUE_30_DAYS,
                CASE WHEN CERT.STATUS IN (0, 1, 2, 3) AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.TARGET_REMEDIATION_DATE), CURRENT_TIMESTAMP()) >= 60 THEN 1 ELSE 0 END AS CERT_PAST_DUE_60_DAYS,
                CASE WHEN CERT.STATUS IN (0, 1, 2, 3) AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.TARGET_REMEDIATION_DATE), CURRENT_TIMESTAMP()) >= 90 THEN 1 ELSE 0 END AS CERT_PAST_DUE_90_DAYS,
                FROM EIAM_DB.PHAP_ERAM_11ACAPT.AA_MICROCERTIFICATION CERT WHERE CERT.CERTIFICATION_TYPE = 'Compute Groups APR' AND TO_TIMESTAMP(CERT.CREATED_DATE) >= DATEADD(YEAR, -1, CURRENT_DATE())
                AND CERT.STATUS = 6
            ) CERT
)
SELECT
    CURRENT_DATE() AS DATE,
    'MNTR-1104900-T3' AS MONITORING_METRIC_NUMBER,
    CASE WHEN TOTAL_ROLES = 0 THEN 100 ELSE ROUND(100.0 * COMPLIANT_ROLES / NULLIF(TOTAL_ROLES, 0), 2) END AS MONITORING_METRIC,
    CASE WHEN TOTAL_ROLES = 0 THEN 'GREEN'
    WHEN (COMPLIANT_ROLES = TOTAL_ROLES) THEN 'GREEN'
    ELSE 'RED'
    END AS COMPLIANCE_STATUS,
    CASE WHEN COMPLIANT_ROLES = NULL THEN 0 END AS NUMERATOR,
    TOTAL_ROLES AS DENOMINATOR
FROM REMEDIATION_STATS;