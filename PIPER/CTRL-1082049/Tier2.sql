-- Tier 2 (Accuracy) Metric:
WITH LATEST_BATCH AS (
    SELECT MAX(BATCH_ID) AS BATCH_ID 
    FROM CLCN_DB.PHDP_CLOUD.OZONE_QUARTERLY_EXAMINER_REVIEW_TOTAL_COUNTS_REPORT_V01_V4
),
-- Get total evaluated resources using same logic as Tier 1
-- This ensures consistency between Tier 1 and Tier 2 metrics
EVALUATED_RESOURCES AS (
    SELECT 
        SUM(TOTAL_EVALUATIONS_COUNT) as TOTAL_EVALUATED
    FROM CLCN_DB.PHDP_CLOUD.OZONE_QUARTERLY_EXAMINER_REVIEW_TOTAL_COUNTS_REPORT_V01_V4
    WHERE BATCH_ID = (SELECT BATCH_ID FROM LATEST_BATCH)
    AND CONTROL_ID = 'AC-3.AWS.12.v02'                    -- Filter for specific control
    AND RESOURCE_TYPE = 'AWS::IAM::Role'                  -- Resource type filter
    AND ROLE_TYPE = 'MACHINE'                             -- Role type filter
),
-- Get count of currently non-compliant resources
-- Uses same base logic as Tier 3 but without SLA status filtering
NON_COMPLIANT_COUNT AS (
    SELECT 
        COUNT(DISTINCT nc.RESOURCE_ID) as TOTAL_NON_COMPLIANT
    FROM CLCN_DB.PHDP_CLOUD.OZONE_NON_COMPLIANT_RESOURCES_TCRD_VIEW_V01 nc
    WHERE nc.CONTROL_ID = 'AC-3.AWS.12.v02'
    AND nc.ID NOT IN (                                    -- Exclude closed/resolved cases
        SELECT ID 
        FROM CLCN_DB.PHDP_CLOUD.OZONE_CLOSED_NON_COMPLIANT_RESOURCES_V04
    )
),
-- Calculate final compliance metrics
COMPLIANCE_METRICS AS (
    SELECT 
        e.TOTAL_EVALUATED as EVALUATED_RESOURCES,         -- Total resources evaluated
        nc.TOTAL_NON_COMPLIANT as NON_COMPLIANT_RESOURCES, -- Total non-compliant resources
        -- Calculate compliant resources (ensure non-negative result)
        GREATEST(0, e.TOTAL_EVALUATED - nc.TOTAL_NON_COMPLIANT) as COMPLIANT_RESOURCES
    FROM EVALUATED_RESOURCES e
    CROSS JOIN NON_COMPLIANT_COUNT nc
)
-- Generate the final metric report
SELECT 
    CURRENT_DATE AS DATE,                                 -- Current date for the report
    'MNTR-1082049-T2' AS MONITORING_METRIC_NUMBER,         -- Metric identifier
    -- Calculate compliance rate as percentage of evaluated resources that are compliant
    ROUND(100.0 * COMPLIANT_RESOURCES / NULLIF(EVALUATED_RESOURCES, 0), 2) AS MONITORING_METRIC,
    -- Determine compliance status based on compliance rate:
    -- >= 95% : GREEN
    -- >= 90% : YELLOW
    -- < 90%  : RED
    CASE 
        WHEN (COMPLIANT_RESOURCES / NULLIF(EVALUATED_RESOURCES, 0)) >= 0.95 THEN 'GREEN'
        WHEN (COMPLIANT_RESOURCES / NULLIF(EVALUATED_RESOURCES, 0)) >= 0.90 THEN 'YELLOW'
        ELSE 'RED'
    END AS COMPLIANCE_STATUS,
    COMPLIANT_RESOURCES AS NUMERATOR,                     -- Number of compliant resources
    EVALUATED_RESOURCES AS DENOMINATOR                    -- Total number of evaluated resources
FROM COMPLIANCE_METRICS;