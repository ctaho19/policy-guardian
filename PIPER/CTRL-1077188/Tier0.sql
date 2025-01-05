-- Tier 0 (Heartbeat): Monitor certificate rotation compliance
-- This query identifies certificates needing rotation and checks if they are being actively rotated

-- First CTE: Identify certificates that need rotation based on expiration criteria
WITH CERTS_NEEDING_ROTATION AS (
    SELECT 
        Certificate_ARN,
        NOT_VALID_BEFORE_UTC_TIMESTAMP,
        NOT_VALID_AFTER_UTC_TIMESTAMP,
        LAST_USAGE_OBSERVATION_UTC_TIMESTAMP,
        -- Calculate total validity period in days
        DATEDIFF(day, NOT_VALID_BEFORE_UTC_TIMESTAMP, NOT_VALID_AFTER_UTC_TIMESTAMP) as CERT_VALIDITY_DAYS,
        -- Calculate days remaining until expiry
        DATEDIFF(day, CURRENT_TIMESTAMP, NOT_VALID_AFTER_UTC_TIMESTAMP) as DAYS_UNTIL_EXPIRY
    FROM CYBR_DB.PHDP_CYBR.CERTIFICATE_CATALOG_CERTIFICATE_USAGE
    WHERE CERTIFICATE_ARN LIKE '%arn:aws:acm%'
    -- Only include certificates expiring within next 30 days but not yet expired
    AND DATEDIFF(day, CURRENT_TIMESTAMP, NOT_VALID_AFTER_UTC_TIMESTAMP) <= 30
    AND DATEDIFF(day, CURRENT_TIMESTAMP, NOT_VALID_AFTER_UTC_TIMESTAMP) > 0
    -- Only include ~1 year certificates (350-380 days)
    AND DATEDIFF(day, NOT_VALID_BEFORE_UTC_TIMESTAMP, NOT_VALID_AFTER_UTC_TIMESTAMP) BETWEEN 350 AND 380
    -- Take most recent observation for each certificate
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Certificate_ARN ORDER BY LAST_USAGE_OBSERVATION_UTC_TIMESTAMP DESC) = 1
),

-- Second CTE: Identify certificates that were recently rotated
RECENT_ROTATIONS AS (
    SELECT 
        Certificate_ARN,
        NOT_VALID_BEFORE_UTC_TIMESTAMP,
        NOT_VALID_AFTER_UTC_TIMESTAMP,
        LAST_USAGE_OBSERVATION_UTC_TIMESTAMP
    FROM CYBR_DB.PHDP_CYBR.CERTIFICATE_CATALOG_CERTIFICATE_USAGE
    WHERE CERTIFICATE_ARN LIKE '%arn:aws:acm%'
    -- Only include certificates created in last 3 days
    AND DATE(NOT_VALID_BEFORE_UTC_TIMESTAMP) >= DATEADD(day, -3, CURRENT_DATE())
    -- Take most recent certificate for each ARN
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Certificate_ARN ORDER BY NOT_VALID_BEFORE_UTC_TIMESTAMP DESC) = 1
),

-- Third CTE: Calculate rotation metrics by comparing certificates needing rotation vs recently rotated
ROTATION_METRICS AS (
    SELECT
        COUNT(CNR.Certificate_ARN) as CERTS_NEEDING_ROTATION,
        COUNT(RR.Certificate_ARN) as CERTS_RECENTLY_ROTATED,
        -- Determine overall rotation status
        CASE 
            WHEN COUNT(CNR.Certificate_ARN) = 0 THEN 'No certificates currently need rotation'
            WHEN COUNT(RR.Certificate_ARN) > 0 THEN 'Certificates are being actively rotated'
            ELSE 'Certificates need rotation but none rotated recently'
        END as ROTATION_STATUS
    FROM CERTS_NEEDING_ROTATION CNR
    LEFT JOIN RECENT_ROTATIONS RR ON CNR.Certificate_ARN = RR.Certificate_ARN
)

-- Final output with compliance status
SELECT
    CURRENT_DATE() as DATE,
    'MNTR-1077188-T0' as MONITORING_METRIC_NUMBER,
    -- Binary metric: 1 if compliant, 0 if not
    CASE 
        WHEN ROTATION_STATUS = 'No certificates currently need rotation' THEN 1
        WHEN ROTATION_STATUS = 'Certificates are being actively rotated' THEN 1
        ELSE 0
    END as MONITORING_METRIC,
    -- Compliance status based on rotation status
    CASE 
        WHEN ROTATION_STATUS = 'No certificates currently need rotation' THEN 'GREEN'
        WHEN ROTATION_STATUS = 'Certificates are being actively rotated' THEN 'GREEN'
        ELSE 'RED'
    END as COMPLIANCE_STATUS,
    CASE 
        WHEN ROTATION_STATUS IN ('No certificates currently need rotation', 'Certificates are being actively rotated') THEN 1
        ELSE 0
    END as NUMERATOR,
    1 as DENOMINATOR
FROM ROTATION_METRICS;