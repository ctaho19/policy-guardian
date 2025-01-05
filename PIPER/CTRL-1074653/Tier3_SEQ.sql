-- Tier 3 Supporting Evidence Query
SELECT 
    ACCOUNT_ID,                                           -- AWS Account identifier
    REGION,                                              -- AWS Region
    RESOURCE_TYPE,                                       -- Type of AWS resource
    RESOURCE_ID,                                         -- Unique resource identifier
    CONTROL_RISK,                                        -- Risk level (Critical/High/Medium/Low)
    OPEN_DATE_UTC_TIMESTAMP,                             -- When the non-compliance was first detected
    DATEDIFF('days', OPEN_DATE_UTC_TIMESTAMP, CURRENT_DATE) as AGING_DAYS  -- Days since detection
FROM CLCN_DB.PHDP_CLOUD.OZONE_NON_COMPLIANT_RESOURCES_TCRD_VIEW_V01
WHERE CONTROL_ID = 'AC-3.AWS.39.v02'
AND ID NOT IN (
    SELECT ID 
    FROM CLCN_DB.PHDP_CLOUD.OZONE_CLOSED_NON_COMPLIANT_RESOURCES_V04
)
-- Only include resources that have exceeded their SLA based on risk level
AND (
    (CONTROL_RISK = 'Critical' AND DATEDIFF('days', OPEN_DATE_UTC_TIMESTAMP, CURRENT_DATE) > 0) OR
    (CONTROL_RISK = 'High' AND DATEDIFF('days', OPEN_DATE_UTC_TIMESTAMP, CURRENT_DATE) > 30) OR
    (CONTROL_RISK = 'Medium' AND DATEDIFF('days', OPEN_DATE_UTC_TIMESTAMP, CURRENT_DATE) > 60) OR
    (CONTROL_RISK = 'Low' AND DATEDIFF('days', OPEN_DATE_UTC_TIMESTAMP, CURRENT_DATE) > 90)
)
ORDER BY AGING_DAYS DESC; 