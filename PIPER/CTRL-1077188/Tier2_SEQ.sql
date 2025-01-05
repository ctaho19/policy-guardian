--Tier 2 Supporting Evidence Query
-- Query to analyze ACM certificate usage and expiration statistics
SELECT
    -- Get total count of all ACM certificates
    COUNT(DISTINCT CERTIFICATE_ID) AS "TOTAL ACM CERTIFICATE COUNT",
    
    -- Count certificates from the past year that were used after their expiration date
    COUNT(DISTINCT CASE 
        WHEN NOT_VALID_AFTER_UTC_TIMESTAMP >= DATEADD('DAY', -365, CURRENT_TIMESTAMP) 
        AND NOT_VALID_AFTER_UTC_TIMESTAMP < LAST_USAGE_OBSERVATION_UTC_TIMESTAMP 
        THEN CERTIFICATE_ID 
    END) AS "ACM CERTIFICATES USED PAST EXPIRATION DATE FROM THE PAST YEAR",
    
    -- Count all certificates that were ever used after expiration (no time limit)
    COUNT(DISTINCT CASE 
        WHEN NOT_VALID_AFTER_UTC_TIMESTAMP < LAST_USAGE_OBSERVATION_UTC_TIMESTAMP 
        THEN CERTIFICATE_ID 
    END) AS "ACM CERTIFICATES USED BEFORE EXPIRATION DATE (ALL TIME)",
    
    -- Count certificates that will expire within the next 30 days
    COUNT(DISTINCT CASE 
        WHEN DATEDIFF('DAY', CURRENT_TIMESTAMP, NOT_VALID_AFTER_UTC_TIMESTAMP) <= 30 
        AND NOT_VALID_AFTER_UTC_TIMESTAMP < CURRENT_TIMESTAMP 
        THEN CERTIFICATE_ID 
    END) AS "ACM CERTIFICATES NEARING EXPIRATION (WITHIN 30 DAYS)",
    
    -- Count certificates that were not used for 30+ days after expiration
    COUNT(DISTINCT CASE 
        WHEN NOT_VALID_AFTER_UTC_TIMESTAMP > LAST_USAGE_OBSERVATION_UTC_TIMESTAMP + INTERVAL '30 DAYS' 
        THEN CERTIFICATE_ID 
    END) AS "ACM CERTIFICATES NOT USED PAST EXPIRATION DATE"
FROM CYBR_DB.PHDP_CYBR.CERTIFICATE_CATALOG_CERTIFICATE_USAGE
-- Filter for only ACM certificates
WHERE CERTIFICATE_ARN LIKE '%arn:aws:acm%'
-- Exclude certificates with NULL expiration dates
AND NOT_VALID_AFTER_UTC_TIMESTAMP IS NOT NULL;