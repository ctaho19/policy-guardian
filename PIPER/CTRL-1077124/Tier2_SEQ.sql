-- Tier 2 Supporting Evidence Query
WITH NON_COMPLIANT_RESOURCES AS (
    SELECT 
        nc.ACCOUNT_ID,                                    -- AWS Account identifier
        nc.REGION,                                       -- AWS Region
        nc.RESOURCE_TYPE,                                -- Type of AWS resource
        nc.RESOURCE_ID,                                  -- Unique resource identifier
        nc.CONTROL_ID,                                   -- Control being evaluated
        nc.OPEN_DATE_UTC_TIMESTAMP,                      -- When non-compliance was first detected
        DATEDIFF('days', nc.OPEN_DATE_UTC_TIMESTAMP, CURRENT_DATE) as DAYS_OPEN  -- Duration of non-compliance
    FROM CLCN_DB.PHDP_CLOUD.OZONE_NON_COMPLIANT_RESOURCES_TCRD_VIEW_V01 nc
    WHERE nc.CONTROL_ID = 'AC-6.AWS.35.v02'             -- Filter for specific control
    AND nc.ID NOT IN (                                   -- Exclude resolved cases
        SELECT ID 
        FROM CLCN_DB.PHDP_CLOUD.OZONE_CLOSED_NON_COMPLIANT_RESOURCES_V04
    )
)
-- Final output with handling for no results case
SELECT 
    -- Return placeholder text if no non-compliant resources found
    COALESCE(r.ACCOUNT_ID, 'No non-compliant resources found') as ACCOUNT_ID,
    r.REGION,
    r.RESOURCE_TYPE,
    r.RESOURCE_ID,
    r.CONTROL_ID,
    r.OPEN_DATE_UTC_TIMESTAMP,
    r.DAYS_OPEN
FROM (
    -- Union actual results with placeholder row
    SELECT * FROM NON_COMPLIANT_RESOURCES
    UNION ALL
    -- Add placeholder row only if no results exist
    SELECT 
        'No non-compliant resources found',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    WHERE NOT EXISTS (SELECT 1 FROM NON_COMPLIANT_RESOURCES)
) r
ORDER BY r.DAYS_OPEN DESC NULLS LAST;                    -- Sort by oldest issues first, nulls at end