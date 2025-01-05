-- Tier 3 (SLA) Metric:
WITH NON_COMPLIANT_COUNTS AS (
    SELECT 
        -- Count total non-compliant resources
        COUNT(DISTINCT RESOURCE_ID) as TOTAL_NON_COMPLIANT,
        -- Count resources that are past their SLA based on risk level
        COUNT(DISTINCT CASE 
            WHEN (CONTROL_RISK = 'Critical' AND DATEDIFF('days', OPEN_DATE_UTC_TIMESTAMP, CURRENT_DATE) > 0) OR
                 (CONTROL_RISK = 'High' AND DATEDIFF('days', OPEN_DATE_UTC_TIMESTAMP, CURRENT_DATE) > 30) OR
                 (CONTROL_RISK = 'Medium' AND DATEDIFF('days', OPEN_DATE_UTC_TIMESTAMP, CURRENT_DATE) > 60) OR
                 (CONTROL_RISK = 'Low' AND DATEDIFF('days', OPEN_DATE_UTC_TIMESTAMP, CURRENT_DATE) > 90)
            THEN RESOURCE_ID 
        END) as PAST_SLA_COUNT
    FROM CLCN_DB.PHDP_CLOUD.OZONE_NON_COMPLIANT_RESOURCES_TCRD_VIEW_V01
    WHERE CONTROL_ID = 'AC-3.AWS.39.v02'
    -- Exclude resources that have been remediated
    AND ID NOT IN (
        SELECT ID 
        FROM CLCN_DB.PHDP_CLOUD.OZONE_CLOSED_NON_COMPLIANT_RESOURCES_V04
    )
)
SELECT 
    CURRENT_DATE AS DATE,
    'MNTR-XXXXX-T3' AS MONITORING_METRIC_NUMBER,
    -- Calculate percentage of resources within SLA (higher is better)
    ROUND(100.0 * (1 - (CAST(PAST_SLA_COUNT AS DECIMAL(18,2)) / NULLIF(CAST(TOTAL_NON_COMPLIANT AS DECIMAL(18,2)), 0))), 2) AS MONITORING_METRIC,
    -- Determine compliance status:
    -- GREEN: 95% or more within SLA (5% or less past due)
    -- YELLOW: 90-95% within SLA (5-10% past due)
    -- RED: Less than 90% within SLA (more than 10% past due)
    CASE 
        WHEN (PAST_SLA_COUNT / NULLIF(TOTAL_NON_COMPLIANT, 0)) <= 0.05 THEN 'GREEN'
        WHEN (PAST_SLA_COUNT / NULLIF(TOTAL_NON_COMPLIANT, 0)) <= 0.10 THEN 'YELLOW'
        ELSE 'RED'
    END AS COMPLIANCE_STATUS,
    TOTAL_NON_COMPLIANT - PAST_SLA_COUNT AS NUMERATOR,   -- Resources within SLA
    TOTAL_NON_COMPLIANT AS DENOMINATOR                    -- Total non-compliant resources
FROM NON_COMPLIANT_COUNTS;