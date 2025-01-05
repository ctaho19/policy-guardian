-- Tier 2 (Accuracy) What % of the daily tests passed?
WITH LAST_SCAN_DATE AS (
    -- Get the most recent scan date within the last 3 days
    SELECT MAX(REPORTDATE) AS RECENT_DATE
    FROM CYBR_DB.PHDP_CYBR.MACIE_CONTROLS_TESTING
    WHERE REPORTDATE BETWEEN DATEADD(DAY, -3, CURRENT_DATE()) AND DATEADD(DAY, -1, CURRENT_DATE())
),
SUMMARY AS (
    -- Calculate total tests and successful tests for most recent scan
    SELECT
        COUNT(*) AS TOTAL_TESTS,
        SUM(CASE WHEN TESTISSUCCESSFUL = TRUE THEN 1 ELSE 0 END) AS TOTAL_SUCCESSFUL_TESTS
    FROM CYBR_DB.PHDP_CYBR.MACIE_CONTROLS_TESTING
    JOIN LAST_SCAN_DATE LSD ON REPORTDATE = LSD.RECENT_DATE
),
HISTORICAL_DATA AS (
    -- Get historical test statistics over past 14 days for anomaly detection
    SELECT
        AVG(TOTAL_TESTS) AS AVG_HISTORICAL_TESTS,
        MIN(TOTAL_TESTS) AS MIN_HISTORICAL_TESTS,
        MAX(TOTAL_TESTS) AS MAX_HISTORICAL_TESTS
    FROM (
        SELECT REPORTDATE, COUNT(*) AS TOTAL_TESTS
        FROM CYBR_DB.PHDP_CYBR.MACIE_CONTROLS_TESTING
        WHERE REPORTDATE BETWEEN DATEADD(DAY, -14, CURRENT_DATE()) AND DATEADD(DAY, -1, CURRENT_DATE())
        GROUP BY REPORTDATE
    ) DAILY_TESTS
)
SELECT
    LSD.RECENT_DATE AS DATE,
    'MNTR-1079134-T2' AS MONITORING_METRIC_NUMBER,
    -- Calculate percentage of successful tests
    ROUND(100.00 * TOTAL_SUCCESSFUL_TESTS / NULLIF(TOTAL_TESTS, 0), 2) AS MONITORING_METRIC,
    -- Determine compliance status based on test results and historical patterns
    CASE 
        WHEN TOTAL_TESTS = 0 THEN NULL                                            -- No tests run
        WHEN TOTAL_SUCCESSFUL_TESTS = TOTAL_TESTS THEN 'GREEN'                    -- All tests passed
        WHEN TOTAL_SUCCESSFUL_TESTS = 0 THEN 'RED'                               -- All tests failed
        WHEN TOTAL_TESTS < (SELECT MIN_HISTORICAL_TESTS FROM HISTORICAL_DATA)     -- Anomaly detection:
          OR TOTAL_TESTS > (SELECT MAX_HISTORICAL_TESTS FROM HISTORICAL_DATA)     -- Test count outside historical range
          OR ABS(TOTAL_TESTS - (SELECT AVG_HISTORICAL_TESTS FROM HISTORICAL_DATA)) 
             > (SELECT AVG_HISTORICAL_TESTS FROM HISTORICAL_DATA) * 0.2           -- Test count deviates >20% from average
        THEN 'YELLOW'
        ELSE 'GREEN'
    END AS COMPLIANCE_STATUS
FROM SUMMARY, LAST_SCAN_DATE LSD;