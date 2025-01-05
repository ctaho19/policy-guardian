-- Tier 2 Supporting Evidence Query
SELECT
    CERT.APPLICATION_ASV,          -- Application name
    CERT.ID,                       -- Certification ID
    CERT.CERTIFICATION_TYPE,       -- Type of certification (filtered to Compute Groups APR)
    TO_TIMESTAMP(CERT.CREATED_DATE) AS CREATION_DATE,  -- When certification was created
    CASE 
        WHEN CERT_PAST_DUE_30_DAYS = 1 THEN 'PAST DUE 30+ DAYS'
        WHEN CERT_PAST_DUE_60_DAYS = 1 THEN 'PAST DUE 60+ DAYS'
        WHEN CERT_PAST_DUE_90_DAYS = 1 THEN 'PAST DUE 90+ DAYS'
        ELSE 'COMPLIANT'
    END AS COMPLIANCE_STATUS,      -- Shows how far past due the certification is
FROM (
    -- Subquery to calculate past due status for each certification
    SELECT
        CERT.*,
        -- Flag certifications past due at 30 day threshold
        CASE WHEN CERT.STATUS IN (0, 1, 2, 3) AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.CREATED_DATE), CURRENT_TIMESTAMP()) >= 30 THEN 1 ELSE 0 END AS CERT_PAST_DUE_30_DAYS,
        -- Flag certifications past due at 60 day threshold  
        CASE WHEN CERT.STATUS IN (0, 1, 2, 3) AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.CREATED_DATE), CURRENT_TIMESTAMP()) >= 60 THEN 1 ELSE 0 END AS CERT_PAST_DUE_60_DAYS,
        -- Flag certifications past due at 90 day threshold
        CASE WHEN CERT.STATUS IN (0, 1, 2, 3) AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.CREATED_DATE), CURRENT_TIMESTAMP()) >= 90 THEN 1 ELSE 0 END AS CERT_PAST_DUE_90_DAYS,
        -- Flag to exclude certifications in remediation (status 6)
        CASE WHEN CERT.STATUS = 6 THEN 0 ELSE 1 END AS NOT_IN_REMEDIATION
    FROM EIAM_DB.PHAP_ERAM_11ACAPT.AA_MICROCERTIFICATION CERT
    WHERE CERT.CERTIFICATION_TYPE = 'Compute Groups APR'
    AND TO_TIMESTAMP(CERT.CREATED_DATE) >= DATEADD(YEAR, -1, CURRENT_DATE())  -- Only look at last year
) CERT
WHERE NOT_IN_REMEDIATION = 1  -- Exclude certifications in remediation
AND (CERT_PAST_DUE_30_DAYS = 1 OR CERT_PAST_DUE_60_DAYS = 1 OR CERT_PAST_DUE_90_DAYS = 1)  -- Only show past due certifications
ORDER BY 
    -- Sort overdue items first, then by target date
    CASE WHEN TO_TIMESTAMP(CERT.TARGET_REMEDIATION_DATE) < CURRENT_TIMESTAMP() THEN 1 ELSE 2 END,
    TO_TIMESTAMP(CERT.TARGET_REMEDIATION_DATE) ASC;