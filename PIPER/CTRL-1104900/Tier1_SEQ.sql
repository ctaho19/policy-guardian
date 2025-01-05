-- Tier 1 Supporting Evidence Query
WITH APP_COUNTS AS (
    SELECT 
        COUNT(DISTINCT A.APPLICATION_ASV) AS TOTAL_ASV_APPS,
        COUNT(DISTINCT B.HPSM_NAME) AS TOTAL_CMDB_APPS,
        COUNT(DISTINCT CASE WHEN B.HPSM_NAME IS NOT NULL THEN A.APPLICATION_ASV END) AS MATCHED_APPS
    FROM EIAM_DB.PHOP_ETAM_IDENTITYIQ.SPT_APPLICATION_ASV A
    LEFT JOIN CMDB_DB.PHDP_CMDB.CMDB_CI_SUMMARY_BC B
        ON A.APPLICATION_ASV = B.HPSM_NAME
    WHERE A.APPLICATION_ASV IS NOT NULL
        AND B.INSTALL_STATUS_DESC = 'In Production'
)
SELECT 
    TOTAL_ASV_APPS AS "Total Applications in ASV",
    TOTAL_CMDB_APPS AS "Total Applications in CMDB",
    MATCHED_APPS AS "Matched Applications",
    (TOTAL_ASV_APPS - MATCHED_APPS) AS "Unmatched Applications",
    ROUND((MATCHED_APPS::FLOAT / NULLIF(TOTAL_ASV_APPS, 0)) * 100, 2) AS "Match Percentage"
FROM APP_COUNTS;

-- Detailed list of unmatched applications
SELECT 
    A.APPLICATION_ASV AS "Unmatched Application",
    'Not Found in CMDB' AS "Status"
FROM EIAM_DB.PHOP_ETAM_IDENTITYIQ.SPT_APPLICATION_ASV A
LEFT JOIN CMDB_DB.PHDP_CMDB.CMDB_CI_SUMMARY_BC B
    ON A.APPLICATION_ASV = B.HPSM_NAME
WHERE A.APPLICATION_ASV IS NOT NULL
    AND B.HPSM_NAME IS NULL
ORDER BY A.APPLICATION_ASV;