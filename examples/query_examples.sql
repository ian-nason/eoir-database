-- =============================================================================
-- EOIR Database — Example Queries
-- =============================================================================
-- Run these against the built eoir.duckdb file:
--   duckdb eoir.duckdb < examples/query_examples.sql
--   OR interactively: duckdb eoir.duckdb
-- =============================================================================


-- 1. Database overview: row counts for every table
SELECT table_name, row_count, column_count, is_lookup
FROM _metadata
ORDER BY row_count DESC;


-- 2. Proceedings by fiscal year (Oct-Sep), last 10 years
SELECT
    CASE
        WHEN EXTRACT(MONTH FROM OSC_DATE) >= 10
        THEN EXTRACT(YEAR FROM OSC_DATE) + 1
        ELSE EXTRACT(YEAR FROM OSC_DATE)
    END AS fiscal_year,
    COUNT(*) AS proceedings
FROM proceedings
WHERE OSC_DATE IS NOT NULL
  AND EXTRACT(YEAR FROM OSC_DATE) >= EXTRACT(YEAR FROM CURRENT_DATE) - 10
GROUP BY 1
ORDER BY 1 DESC;


-- 3. Top 20 immigration courts by total proceedings
SELECT
    p.BASE_CITY_CODE,
    bc.BASE_CITY_NAME AS court_name,
    COUNT(*) AS total_proceedings,
    COUNT(DISTINCT p.IDNCASE) AS unique_cases
FROM proceedings p
LEFT JOIN lu_base_city bc ON p.BASE_CITY_CODE = bc.BASE_CITY_CODE
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 20;


-- 4. Top 15 nationalities in cases filed since 2020
SELECT
    n.NAT_COUNTRY_NAME AS country,
    COUNT(*) AS cases
FROM cases c
JOIN lu_nationality n ON c.NAT = n.NAT_CODE
WHERE c.INPUT_DATE >= '2020-01-01'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 15;


-- 5. Asylum grant rates by fiscal year (last 5 years)
--    Uses applications table with asylum application codes
SELECT
    EXTRACT(YEAR FROM a.COMP_DATE) AS year,
    COUNT(*) AS total_decisions,
    SUM(CASE WHEN d.strCourtApplnDecDesc ILIKE '%grant%' THEN 1 ELSE 0 END) AS granted,
    ROUND(
        100.0 * SUM(CASE WHEN d.strCourtApplnDecDesc ILIKE '%grant%' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 1
    ) AS grant_rate_pct
FROM applications a
LEFT JOIN lu_app_decision d ON a.APPL_DEC_CODE = d.strCourtApplnDecCode
WHERE a.COMP_DATE IS NOT NULL
  AND EXTRACT(YEAR FROM a.COMP_DATE) >= EXTRACT(YEAR FROM CURRENT_DATE) - 5
  AND a.APPL_CODE IN (SELECT strcode FROM lu_application WHERE strdescription ILIKE '%asylum%')
GROUP BY 1
ORDER BY 1 DESC;


-- 6. Median days from NTA to first hearing, by court (top 15 busiest)
SELECT
    bc.BASE_CITY_NAME AS court_name,
    COUNT(*) AS cases,
    MEDIAN(days_osc_to_hearing) AS median_days_to_hearing,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_osc_to_hearing)
        AS p75_days
FROM v_first_hearing fh
JOIN lu_base_city bc ON fh.BASE_CITY_CODE = bc.BASE_CITY_CODE
WHERE days_osc_to_hearing BETWEEN 0 AND 3650  -- exclude outliers
GROUP BY 1
HAVING COUNT(*) > 1000
ORDER BY 2 DESC
LIMIT 15;


-- 7. Bond amounts over time — average initial bond by year
SELECT
    EXTRACT(YEAR FROM ADJ_DATE) AS year,
    COUNT(*) AS bond_hearings,
    ROUND(AVG(INITIAL_BOND), 0) AS avg_initial_bond,
    ROUND(MEDIAN(INITIAL_BOND), 0) AS median_initial_bond,
    ROUND(AVG(NEW_BOND), 0) AS avg_new_bond
FROM bonds
WHERE ADJ_DATE IS NOT NULL
  AND INITIAL_BOND IS NOT NULL
  AND EXTRACT(YEAR FROM ADJ_DATE) >= 2010
GROUP BY 1
ORDER BY 1 DESC;


-- 8. Representation rates: % of cases with attorney, by year
SELECT
    EXTRACT(YEAR FROM p.OSC_DATE) AS year,
    COUNT(DISTINCT p.IDNCASE) AS total_cases,
    COUNT(DISTINCT r.IDNCASE) AS represented_cases,
    ROUND(
        100.0 * COUNT(DISTINCT r.IDNCASE) / NULLIF(COUNT(DISTINCT p.IDNCASE), 0), 1
    ) AS representation_rate_pct
FROM proceedings p
LEFT JOIN representatives r ON p.IDNCASE = r.IDNCASE
WHERE p.OSC_DATE IS NOT NULL
  AND EXTRACT(YEAR FROM p.OSC_DATE) >= EXTRACT(YEAR FROM CURRENT_DATE) - 10
GROUP BY 1
ORDER BY 1 DESC;


-- 9. Most common charges in removal proceedings
SELECT
    lc.strCodeDescription AS charge_description,
    COUNT(*) AS times_charged
FROM charges c
JOIN lu_charges lc ON c.CHARGE = lc.strCode
GROUP BY 1
ORDER BY 2 DESC
LIMIT 15;


-- 10. Adjournment reasons — why hearings get postponed
SELECT
    la.strDesciption AS adjournment_reason,
    COUNT(*) AS occurrences,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM schedule s
JOIN lu_adjournment la ON s.ADJ_RSN = la.strcode
WHERE s.ADJ_RSN IS NOT NULL AND s.ADJ_RSN != ''
GROUP BY 1
ORDER BY 2 DESC
LIMIT 15;


-- 11. Case outcomes by nationality (top 10 nationalities, last 5 years)
--     Shows grant vs. denial vs. other for each country
WITH top_nats AS (
    SELECT NAT, COUNT(*) AS cnt
    FROM proceedings
    WHERE OSC_DATE >= CURRENT_DATE - INTERVAL '5 years'
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
)
SELECT
    n.NAT_COUNTRY_NAME AS country,
    COUNT(*) AS decisions,
    SUM(CASE WHEN d.strDecDescription ILIKE '%grant%' THEN 1 ELSE 0 END) AS granted,
    SUM(CASE WHEN d.strDecDescription ILIKE '%order of removal%'
              OR d.strDecDescription ILIKE '%deport%' THEN 1 ELSE 0 END) AS removed,
    ROUND(
        100.0 * SUM(CASE WHEN d.strDecDescription ILIKE '%grant%' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 1
    ) AS grant_rate_pct
FROM proceedings p
JOIN top_nats t ON p.NAT = t.NAT
JOIN lu_nationality n ON p.NAT = n.NAT_CODE
LEFT JOIN lu_court_decision d ON p.DEC_CODE = d.strDecCode AND p.CASE_TYPE = d.strCaseType
WHERE p.COMP_DATE IS NOT NULL
  AND p.OSC_DATE >= CURRENT_DATE - INTERVAL '5 years'
GROUP BY 1
ORDER BY 2 DESC;


-- 12. Judge caseload and completion rates (active judges)
SELECT
    j.JUDGE_NAME,
    COUNT(*) AS total_proceedings,
    SUM(CASE WHEN p.COMP_DATE IS NOT NULL THEN 1 ELSE 0 END) AS completed,
    ROUND(
        100.0 * SUM(CASE WHEN p.COMP_DATE IS NOT NULL THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 1
    ) AS completion_rate_pct
FROM proceedings p
JOIN lu_judge j ON p.IJ_CODE = j.JUDGE_CODE
WHERE p.OSC_DATE >= CURRENT_DATE - INTERVAL '3 years'
GROUP BY 1
HAVING COUNT(*) > 100
ORDER BY 2 DESC
LIMIT 20;


-- 13. Detained vs. non-detained case timelines
SELECT
    CASE WHEN c.CUSTODY = 'D' THEN 'Detained' ELSE 'Non-detained' END AS custody_status,
    COUNT(*) AS cases,
    MEDIAN(fh.days_osc_to_hearing) AS median_days_to_first_hearing
FROM cases c
JOIN v_first_hearing fh ON c.IDNCASE = fh.IDNCASE
WHERE fh.days_osc_to_hearing BETWEEN 0 AND 3650
GROUP BY 1;


-- 14. Appeals volume by year
SELECT
    EXTRACT(YEAR FROM OSC_DATE) AS year,
    COUNT(*) AS appeals_filed
FROM appeals
WHERE OSC_DATE IS NOT NULL
  AND EXTRACT(YEAR FROM OSC_DATE) >= 2010
GROUP BY 1
ORDER BY 1 DESC;


-- 15. Languages spoken by respondents (top 20)
SELECT
    l.strDescription AS language,
    COUNT(*) AS cases,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM cases c
JOIN lu_language l ON c.LANG = l.strCode
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;
