/*===========================================================================
 SCRIPT NAME  : OPERATION CLEARWATER — FRAUD DETECTION & RISK INTELLIGENCE
 DATABASE     : NorthAxis Bank | Gold Layer Data Warehouse
 ENVIRONMENT  : PostgreSQL

 DELIVERABLES :
     1. Transaction Overview & Baseline KPIs
     2. Anomaly Detection & Velocity Checks
     3. Customer Risk Profiling
     4. Merchant & Channel Risk Scoring
     5. Fraud Risk Scoring Model 
     6. Executive Risk Report & Recommendations

 SCHEMA       : public
 TABLES       : fact_transactions, dim_customer, dim_account,
                dim_merchant, dim_location, dim_date
===========================================================================*/


/*===========================================================================
DELIVERABLE 1: TRANSACTION OVERVIEW & BASELINE KPIS
============================================================================*/

-- 1.1 Portfolio level transation statistics
-- This query provides a high-level summary of transaction activity, helping establish a baseline for volume, value, and distribution.
SELECT
	COUNT(*)                         AS total_transaction,
	COUNT(DISTINCT transaction_id)   AS total_distinct_transaction,
	COUNT(DISTINCT customer_key)     AS total_customer,
	COUNT(DISTINCT account_key)      AS total_account,
	ROUND(SUM(amount_usd), 2)        AS total_amount,
	ROUND(AVG(amount_usd), 2)        AS avg_transaction_amount,
	ROUND(MIN(amount_usd), 2)        AS min_transaction_amount,
	ROUND(MAX(amount_usd), 2)        AS max_transaction_amount,
	ROUND(STDDEV(amount_usd), 2)     AS stv_dev_amount
FROM 
    fact_transactions;


-- 1.2 Volume & Transaction Share by Channel
-- Shows how transactions and total value are distributed across channels
SELECT
	channel,
	COUNT(*)                                                          AS total_transactions,
	COUNT(DISTINCT customer_key)                                      AS distinct_customers,
	ROUND(SUM(amount_usd), 2)                                         AS total_volume,
	ROUND(AVG(amount_usd), 2)                                         AS avg_transaction_amount,
	ROUND((COUNT(*) / SUM(COUNT(*)) OVER ()) * 100, 2)                AS transaction_pct,
	ROUND((SUM(amount_usd) / SUM(SUM(amount_usd)) OVER ()) * 100, 2)  AS volume_pct
FROM
	fact_transactions
GROUP BY 
  channel
ORDER BY 
  total_volume DESC;


-- 1.3 Transaction & Volume Distribution by KYC Status
-- Shows how transactions, total value, and customers are distributed across KYC categories

SELECT
    c.kyc_status,
    COUNT(*)                                                               AS total_transactions,
    ROUND(SUM(t.amount_usd), 2)                                            AS total_volume,
    ROUND(AVG(t.amount_usd), 2)                                            AS avg_transaction_amount,
    ROUND((COUNT(*) / SUM(COUNT(*)) OVER ()) * 100, 2)                     AS transaction_pct,
    ROUND((SUM(t.amount_usd) / SUM(SUM(t.amount_usd)) OVER ()) * 100, 2)   AS volume_pct,
    ROUND((COUNT(DISTINCT c.customer_key) /
        SUM(COUNT(DISTINCT c.customer_key)) OVER ()) * 100, 2)             AS customer_pct
FROM
    dim_customer AS c
LEFT JOIN
    fact_transactions AS t ON c.customer_key = t.customer_key
GROUP BY
    c.kyc_status
ORDER BY
    total_volume DESC;


-- 1.4 Monthly Transaction Trends & MoM Growth
-- Aggregates transactions by month, then calculates month-over-month (MoM)
-- change in total transaction volume and percentage growth

WITH monthly_summary AS (
    SELECT
        year_number,
        month_number,
        month_name,
        quarter_name,
        COUNT(*)                          AS total_transactions,
        COUNT(DISTINCT customer_key)      AS distinct_customers,
        ROUND(SUM(amount_usd), 2)         AS total_volume,
        ROUND(AVG(amount_usd), 2)         AS avg_transaction_amount
    FROM 
      fact_transactions AS t
    LEFT JOIN 
      dim_date AS d ON t.date_key = d.date_key
    GROUP BY 
        year_number, month_number, month_name, quarter_name
),
month_lag AS (
    SELECT
        *,
        LAG(total_volume) OVER (
            ORDER BY year_number, month_number
        )                                  AS prevs_month_volume
    FROM 
      monthly_summary
)
SELECT
    year_number,
    month_number,
    month_name,
    quarter_name,
    total_transactions,
    distinct_customers,
    total_volume,
    avg_transaction_amount,
    prevs_month_volume,
    ROUND(total_volume - prevs_month_volume, 2)                                   AS mom_volume_change,
    ROUND(((total_volume - prevs_month_volume) / prevs_month_volume) * 100, 2)    AS mom_change_pct
FROM 
    month_lag;


-- 1.5 Country-Level Monthly Transaction Trends & MoM Change
-- Tracks monthly transaction behavior per country and calculates
-- month-over-month (MoM) changes in total transaction volume

WITH monthly_summary AS (
    SELECT
        l.country,
        d.month_number,
        d.month_name,
        COUNT(*)                          AS total_transactions,
        ROUND(SUM(t.amount_usd), 2)       AS total_volume,
        ROUND(AVG(t.amount_usd), 2)       AS avg_volume
    FROM 
      fact_transactions AS t
    LEFT JOIN 
      dim_date AS d ON t.date_key = d.date_key
    LEFT JOIN 
      dim_location AS l ON t.location_key = l.location_key
    GROUP BY 
        l.country, d.month_number, d.month_name
),
month_lag AS (
    SELECT
        *,
        LAG(total_volume) OVER (
            PARTITION BY country
            ORDER BY month_number
        )                                 AS prevs_month_volume
    FROM monthly_summary
)
SELECT
    country,
    month_number,
    month_name,
    total_transactions,
    avg_volume,
    total_volume,
    prevs_month_volume,
    total_volume - prevs_month_volume                                          AS mom_volume_change,
    ROUND(((total_volume - prevs_month_volume) / prevs_month_volume) * 100, 2) AS mom_volume_pt_change
FROM 
    month_lag;


























