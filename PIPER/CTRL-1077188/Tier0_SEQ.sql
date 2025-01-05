-- Tier 0 Supporting Evidence Query
-- First CTE: Identify certificates that need rotation within next 3 days
WITH CERTS_NEEDING_ROTATION AS (
    SELECT 
        Certificate_ARN,
        Certificate_ID,
        NOT_VALID_BEFORE_UTC_TIMESTAMP,
        NOT_VALID_AFTER_UTC_TIMESTAMP,
        LAST_USAGE_OBSERVATION_UTC_TIMESTAMP,
        -- Calculate total validity period in days from start to expiry
        DATEDIFF(day, NOT_VALID_BEFORE_UTC_TIMESTAMP, NOT_VALID_AFTER_UTC_TIMESTAMP) as CERT_VALIDITY_DAYS,
        -- Calculate remaining days until certificate expires
        DATEDIFF(day, CURRENT_TIMESTAMP, NOT_VALID_AFTER_UTC_TIMESTAMP) as DAYS_UNTIL_EXPIRY
    FROM CYBR_DB.PHDP_CYBR.CERTIFICATE_CATALOG_CERTIFICATE_USAGE
    WHERE CERTIFICATE_ARN LIKE '%arn:aws:acm%'
    -- Only include certificates expiring within next 3 days but not yet expired
    AND DATEDIFF(day, CURRENT_TIMESTAMP, NOT_VALID_AFTER_UTC_TIMESTAMP) <= 3
    AND DATEDIFF(day, CURRENT_TIMESTAMP, NOT_VALID_AFTER_UTC_TIMESTAMP) > 0
    -- Take most recent observation for each certificate
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Certificate_ARN ORDER BY LAST_USAGE_OBSERVATION_UTC_TIMESTAMP DESC) = 1
),

-- Second CTE: Identify certificates that were rotated in the last 3 days
RECENT_ROTATIONS AS (
    SELECT 
        Certificate_ARN,
        Certificate_ID,
        NOT_VALID_BEFORE_UTC_TIMESTAMP,
        NOT_VALID_AFTER_UTC_TIMESTAMP,
        LAST_USAGE_OBSERVATION_UTC_TIMESTAMP
    FROM CYBR_DB.PHDP_CYBR.CERTIFICATE_CATALOG_CERTIFICATE_USAGE
    WHERE CERTIFICATE_ARN LIKE '%arn:aws:acm%'
    -- Only include certificates created in last 3 days
    AND DATE(NOT_VALID_BEFORE_UTC_TIMESTAMP) >= DATEADD(day, -3, CURRENT_DATE())
    -- Take most recent certificate for each ARN
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Certificate_ARN ORDER BY NOT_VALID_BEFORE_UTC_TIMESTAMP DESC) = 1
)

-- Final output showing certificates needing rotation and their current status
SELECT
    CNR.Certificate_ID,
    CNR.Certificate_ARN,
    CNR.DAYS_UNTIL_EXPIRY,
    CNR.CERT_VALIDITY_DAYS,
    CNR.NOT_VALID_AFTER_UTC_TIMESTAMP as EXPIRATION_DATE,
    -- Determine if certificate has been recently rotated
    CASE 
        WHEN RR.Certificate_ARN IS NOT NULL THEN 'Recently Rotated'
        ELSE 'Pending Rotation'
    END as ROTATION_STATUS,
    RR.NOT_VALID_BEFORE_UTC_TIMESTAMP as NEW_CERT_START_DATE,
    CNR.LAST_USAGE_OBSERVATION_UTC_TIMESTAMP as LAST_OBSERVATION_TIME
FROM CERTS_NEEDING_ROTATION CNR
LEFT JOIN RECENT_ROTATIONS RR ON CNR.Certificate_ARN = RR.Certificate_ARN
-- Sort by most urgent expiring certificates first
ORDER BY CNR.DAYS_UNTIL_EXPIRY ASC;