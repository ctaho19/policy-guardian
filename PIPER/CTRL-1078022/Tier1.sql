--Tier 1: Evaluated vs. Total Resources
-- This query calculcates the ratio of evaluated resources to total resources and determines compliance status based on the ratio.

--CTE to get the latest batch ID from the ddataset.
-- LATEST_BATCH: Retrieves the maximum BATCH_ID from the dataset to identify the latest batch of data.

WITH LATEST_BATCH AS (
        SELECT MAX(BATCH_ID) AS BATCH_ID
        FROM CLCN_DB.PHDP_CLOUD.OZONE_QUARTERLY_EXAMINER_REVIEW_TOTAL_COUNTS_REPORT_V01_V4
),

-- CTE To Calculate The Total Inventory Count for AWS Resources
-- INVENTORY_COUNTS: Calculates the totoal inventory counts for AWS resoruces by summing the INVETORY_COUNT for the latest batch, filtered by specific criteria such as cloud provider, role type, control Id, and region.
INVENTORY_COUNTS AS (
    SELECT SUM(INVENTORY_COUNT) AS TOTAL_RESOURCESFROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NUMBER, REGION, ASV, RESOURCE_TYPE ORDER BY BATCH_ID DESC) AS RANK FROM CLCN_DB.PHDP_CLOUD.OZONE_QUARTERLY_EXAMINER_REVIEW_TOTAL_COUNTS_REPORT_V01_V4 TC
                WHERE BATCH_ID = (SELECT MAX(BATCH_ID) FROM CLCN_DB.PHDP_CLOUD.OZONE_QUARTERLY_EXAMINER_REVIEW_TOTAL_COUNTS_REPORT_V01_V4)
                AND CLOUD_PROVIDER = 'AWS'
                AND ROLE_TYPE IN ('', 'N/A', 'MACHINE')
                AND CONTROL_ID = 'AC-3.AWS.137.v02'
                AND INVENTORY_COUNT > 0
                AND LOWER(REGION) IN ('ca-central-1', 'eu-west-1', 'eu-west-2', 'us-east-1', 'us-east-2', 'us-west-2', 'global')
                AND RESOURCE_TYPE IN (
                    -- Filters RESOURCE_TYPE based on criteria from the LAZER_CONTROLS_V03 and LAZER_COMPONENETS_V03 tables.
                    SELECT DISTINCT RESOURCE_TYPE
                    FROM CLCN_DB.PHDP_CLOUD.LAZER_CONTROLS_V03 LCTRL
                    LEFT JOIN CLCN_DB.PHDP_CLOUD.LAZER_COMPONENTS_V03 LCOMP
                        USING (CLOUD_CONTROL_VERSION_ID, BATCH_ID)
                    WHERE LCTRL.BATCH_ID = (SELECT MAX(BATCH_ID) FROM CLCN_DB.PHDP_CLOUD.LAZER_CONTROLS_V03)
                    AND ESCALARING
                    AND PHASE = 'Operational'
                    AND RESOURCE_TYPE NOT ILIKE ''
                )
    ) RANKEDRECORDS
    WHERE RANK = 1
),
-- CTE To Calculate The Total Evaluations Count for AWS IAM Machine Roles
-- EVALUATION_COUNTS: Calculates the total evaluation counts for AWS IAM Machine roles by summing the TOTAL_EVALUATIONS_COUNT for the latest batch, filtered by specific criteria such as cloud provider, role type, control Id, and region.
EVALUATION_COUNTS AS (
    SELECT SUM(TOTAL_EVALUATIONS_COUNT) AS EVALUATED_RESOURCES FROM CLCN_DB.PHDP_CLOUD.OZONE_QUARTERLY_EXAMINER_REVIEW_TOTAL_COUNTS_REPORT_V01_V4 TC
    JOIN LATEST_BATCH ON TC.BATCH_ID = LB.BATCH_ID -- Join the latest batch ID to the TC table
    WHERE TC.CLOUD_PROVIDER = 'AWS'
    AND TC.CONTROL_ID = 'AC-3.AWS.137.v02'
    AND TC.RESOURCE_TYPE = 'AWS::IAM::Role'
    AND TC.ROLE_TYPE = 'MACHINE'
    AND TC.INVENTORY_COUNT > 0
    AND LOWER(TC.REGION) IN ('ca-central-1', 'eu-west-1', 'eu-west-2', 'us-east-1', 'us-east-2', 'us-west-2', 'global')
)
-- Final Output
SELECT CURRENT_DATE() AS DATE,
    'MNTR-1078022-T1' AS MONITORING_METRIC_NUMBER,
    ROUND(100.0 * EVALUATED_RESOURCES / NULLIF(TOTAL_RESOURCES, 0), 2) AS MONITORING_METRIC,
    CASE WHEN EVALUATED_RESOURCES >= TOTAL_RESOURCES THEN 'GREEN' ELSE 'RED' END AS COMPLIANCE_STATUS,
    TO_DOUBLE(ROUND(EVALUATED_RESOURCES, 2)) AS NUMERATOR,
    TO_DOUBLE(ROUND(TOTAL_RESOURCES, 2)) AS DENOMINATOR
FROM INVENTORY_COUNTS, EVALUATION_COUNTS;