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
DELIVERABLE 1: Transaction Overview & Baseline KPIs
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
FROM fact_transactions;


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
FROM fact_transactions
GROUP BY channel
ORDER BY total_volume DESC;


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
FROM dim_customer AS c
LEFT JOIN fact_transactions AS t ON c.customer_key = t.customer_key
GROUP BY c.kyc_status
ORDER BY total_volume DESC;


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
    FROM fact_transactions AS t
    LEFT JOIN dim_date AS d ON t.date_key = d.date_key
    GROUP BY 
        year_number, 
		month_number, 
		month_name, 
		quarter_name
),
month_lag AS (
    SELECT
        *,
        LAG(total_volume) OVER (
            ORDER BY year_number, month_number
        )                                  AS prevs_month_volume
    FROM monthly_summary
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
FROM month_lag;


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
    FROM fact_transactions AS t
    LEFT JOIN dim_date AS d ON t.date_key = d.date_key
    LEFT JOIN dim_location AS l ON t.location_key = l.location_key
    GROUP BY 
        l.country, 
		d.month_number, 
		d.month_name
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
FROM month_lag;



/*===========================================================================
DELIVERABLE 2: Anomaly Detection & Velocity Checks
============================================================================*/

-- 2.1 Rapid Transaction Behavior Analysis (≤ 60 mins)
-- Identifies customers with frequent fast transactions within short time gaps and summarizes their transaction behavior and spending patterns

WITH lag_cte AS (
    SELECT
        customer_key,
        transaction_id,
        transaction_datetime,
        amount_usd,
        LAG(transaction_id) OVER (
            PARTITION BY customer_key 
            ORDER BY transaction_datetime 
        ) 										AS prevs_transaction_id,
        LAG(transaction_datetime) OVER (
            PARTITION BY customer_key 
            ORDER BY transaction_datetime 
        ) 										AS prevs_transaction_datetime,
        LAG(amount_usd) OVER (
            PARTITION BY customer_key 
            ORDER BY transaction_datetime 
        ) 										AS prevs_amount_usd
    FROM fact_transactions
),
transaction_filter AS (
    SELECT
        customer_key,
        transaction_id,
        prevs_transaction_id,
        transaction_datetime,
        prevs_transaction_datetime,
        amount_usd,
        prevs_amount_usd,
        ROUND(EXTRACT(EPOCH FROM (transaction_datetime - prevs_transaction_datetime)) / 60, 2) AS transaction_gap
    FROM lag_cte
    WHERE prevs_transaction_datetime IS NOT NULL
),
rapid_transactions AS (
    SELECT
        customer_key,
        COUNT(*)                                                   AS total_transactions,
        ROUND(MIN(transaction_gap), 2)                             AS min_transaction_gap,
        ROUND(AVG(transaction_gap), 2)                             AS avg_transaction_gap,
        ROUND(AVG(prevs_amount_usd + amount_usd), 2)               AS avg_amount_usd
    FROM transaction_filter
    WHERE transaction_gap <= 60
    GROUP BY customer_key
    HAVING COUNT(*) >= 5
)
SELECT
    t.customer_key,
    c.full_name,
    c.country,
    c.kyc_status,
    t.total_transactions,
    t.min_transaction_gap,
    t.avg_transaction_gap,
    t.avg_amount_usd
FROM rapid_transactions AS t
LEFT JOIN dim_customer AS c ON t.customer_key = c.customer_key
ORDER BY t.total_transactions DESC
LIMIT 50;


-- 2.2 Transaction Activity by Time Window
-- Categorizes transactions into time buckets (off-hours, business hours, others) and summarizes volume, customer activity, and percentage distribution

SELECT
    CASE
        WHEN transaction_hour BETWEEN 1 AND 4  THEN 'Off_Hours [1AM-4AM]'
        WHEN transaction_hour BETWEEN 8 AND 18 THEN 'Business_Hours [8AM-6PM]'
        ELSE 'Other Hours'
    END                                                           AS time_window,
    COUNT(*)                                                      AS total_transactions,
    COUNT(DISTINCT customer_key)                                  AS distinct_customers,
    ROUND(SUM(amount_usd), 2)                                     AS total_volume,
    ROUND(AVG(amount_usd), 2)                                     AS avg_volume,
    ROUND((COUNT(*) / SUM(COUNT(*)) OVER ()) * 100, 2)            AS transactions_pct
FROM fact_transactions
GROUP BY
    CASE
        WHEN transaction_hour BETWEEN 1 AND 4  THEN 'Off_Hours [1AM-4AM]'
        WHEN transaction_hour BETWEEN 8 AND 18 THEN 'Business_Hours [8AM-6PM]'
        ELSE 'Other Hours'
    END;


-- 2.3 High-Value Outlier Detection Using Z-Score
-- Calculates the average and standard deviation of transaction amounts, then identifies the top 100 transactions that are significantly above 
-- normal (z-score > 3), enriching results with customer, merchant, and location details

WITH stats AS (
    SELECT
        ROUND(AVG(amount_usd), 2)              AS avg_amount,
        ROUND(STDDEV(amount_usd), 2)           AS std_dev_amount
    FROM fact_transactions
)
SELECT
    t.transaction_id,
    t.customer_key,
    c.full_name,
    t.transaction_datetime,
    t.transaction_type,
    t.channel,
    t.amount_usd,
    s.avg_amount,
    s.std_dev_amount,
    ROUND((t.amount_usd - s.avg_amount) / NULLIF(s.std_dev_amount,0), 2) AS z_score,
    m.merchant_name,
    m.merchant_category,
    m.is_shell_merchant,
    l.country,
    l.is_high_risk_country
FROM fact_transactions AS t
LEFT JOIN dim_customer AS c ON t.customer_key = c.customer_key
LEFT JOIN dim_merchant AS m ON t.merchant_key = m.merchant_key
LEFT JOIN dim_location AS l ON t.location_key = l.location_key
CROSS JOIN stats AS s
WHERE
    ROUND((t.amount_usd - s.avg_amount) / NULLIF(s.std_dev_amount,0), 2) > 3
ORDER BY z_score DESC
LIMIT 100;



-- 2.4 Daily Transaction Volume Spike Detection (7-Day Rolling Analysis)
-- Aggregates daily transaction volume and compares each day to a rolling 7-day average to identify unusual spikes or elevated activity levels

WITH daily_volume AS (
    SELECT
        t.transaction_date,
        d.day_of_week,
        d.is_weekend,
        COUNT(*)                          AS total_transactions,
        ROUND(SUM(t.amount_usd), 2)       AS daily_volume,
        ROUND(AVG(t.amount_usd), 2)       AS daily_avg_volume
    FROM fact_transactions AS t
    LEFT JOIN dim_date AS d ON t.date_key = d.date_key
    GROUP BY
        t.transaction_date, 
		d.day_of_week, 
		d.is_weekend
),
rolling_avg AS (
    SELECT
        *,
        ROUND(
            AVG(daily_volume) OVER (
                ORDER BY transaction_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2
        )										AS rolling_7days_avg,
        ROUND(
            daily_volume / NULLIF(
                AVG(daily_volume) OVER (
                    ORDER BY transaction_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ), 0
            ), 2
        )										AS spike_ratio
    FROM daily_volume
)
SELECT
    transaction_date,
    day_of_week,
    is_weekend,
    total_transactions,
    daily_volume,
    rolling_7days_avg,
    spike_ratio,
    CASE
        WHEN spike_ratio >= 2.0 THEN 'Spike Detected'
        WHEN spike_ratio >= 1.5 THEN 'Elevated'
        ELSE                         'Normal'
    END 									AS spike_flag
FROM rolling_avg
ORDER BY spike_ratio DESC;


/*===========================================================================
DELIVERABLE 3: Customer Risk Profiling
============================================================================*/
-- 3.1 Customer Behavior Shift & Risk Detection (Early vs Late Period)
-- Compares customer transaction behavior across two time periods (early vs late), analyzing changes in spending, transaction volume, 
-- time-of-day activity, and geographic spread to flag potential high-risk or anomalous behavior

WITH base AS (
    SELECT
        t.customer_key,
        t.amount_usd,
        t.transaction_hour,
        l.country,
        CASE
            WHEN t.transaction_hour BETWEEN 1 AND 4  THEN 'Off-Hours'
            WHEN t.transaction_hour BETWEEN 8 AND 18 THEN 'Business-Hours'
            ELSE 'Other-Hours'
        END AS time_window,
        CASE
            WHEN t.transaction_date < '2024-05-01' THEN 'early'
            ELSE 'late'
        END AS period
    FROM fact_transactions AS t
    LEFT JOIN dim_location AS l ON t.location_key = l.location_key
),
customer_features AS (
    SELECT
        customer_key,
        period,
        COUNT(*)                          AS total_txn,
        ROUND(AVG(amount_usd), 2)         AS avg_spend,
        ROUND(STDDEV(amount_usd), 2)      AS stddev_spend,
        ROUND(
            SUM(CASE WHEN time_window = 'Off-Hours' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
        ) AS off_hours_pct,
        ROUND(
            SUM(CASE WHEN time_window = 'Business-Hours' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
        ) AS business_hours_pct,
        ROUND(
            SUM(CASE WHEN time_window = 'Other-Hours' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
        ) AS other_hours_pct,
        COUNT(DISTINCT country)           AS country_count
    FROM base
    GROUP BY 
		customer_key, 
		period
),
early AS (
    SELECT * FROM customer_features WHERE period = 'early'
),
late AS (
    SELECT * FROM customer_features WHERE period = 'late'
)
SELECT
    e.customer_key,
    c.full_name,
    c.country                        AS home_country,
    c.kyc_status,
    c.customer_segment,
    c.preferred_channel,
    e.avg_spend                     AS early_avg_spend,
    l.avg_spend                     AS late_avg_spend,
    ROUND(l.avg_spend / NULLIF(e.avg_spend, 0), 2) AS spend_multiplier,
    e.total_txn,
    l.total_txn,
    (l.total_txn - e.total_txn)     AS txn_difference,
    e.off_hours_pct,
    l.off_hours_pct,
    (l.off_hours_pct - e.off_hours_pct) AS off_hours_shift,
    e.country_count                 AS early_country,
    l.country_count                 AS late_country,
    CASE
        WHEN l.avg_spend / NULLIF(e.avg_spend, 0) >= 5
         AND ABS(l.off_hours_pct - e.off_hours_pct) >= 20
            THEN 'HIGH RISK - Likely Takeover'
        WHEN l.avg_spend / NULLIF(e.avg_spend, 0) >= 3
         OR ABS(l.off_hours_pct - e.off_hours_pct) >= 15
            THEN 'MEDUIM RISK - Investigate'
        ELSE 'Normal Behavior'
    END AS risk_flag
FROM early AS e
LEFT JOIN late AS l ON e.customer_key = l.customer_key
LEFT JOIN dim_customer AS c ON e.customer_key = c.customer_key
WHERE
    e.total_txn >= 3
    AND l.total_txn >= 3
ORDER BY spend_multiplier DESC
LIMIT 200;


-- 3.2 High-Risk Foreign Transaction Analysis (Customer-Level)
-- Identifies customers performing transactions in foreign high-risk countries, summarizing transaction volume, 
-- frequency, off-hours activity, and time range to highlight potentially suspicious cross-border behavior

SELECT
    t.customer_key,
    c.full_name,
    c.country                                       AS registered_country,
    l.country                                       AS transaction_country,
    l.is_high_risk_country,
    COUNT(*)                                        AS foreign_transaction_count,
    ROUND(SUM(t.amount_usd), 2)                     AS foreign_volume_usd,
    ROUND(AVG(t.amount_usd), 2)                     AS avg_foreign_txn_value,
    SUM(
        CASE 
            WHEN t.transaction_hour BETWEEN 1 AND 4 THEN 1 ELSE 0 
        END
    )                                               AS off_hours_txn_count,
    ROUND(
        SUM(
            CASE 
                WHEN t.transaction_hour BETWEEN 1 AND 4 THEN 1 ELSE 0 
            END
        ) * 100.0 / COUNT(*), 2
    )                                               AS off_hours_pct,
    MIN(t.transaction_date)                         AS first_foreign_txn,
    MAX(t.transaction_date)                         AS last_foreign_txn
FROM fact_transactions AS t
LEFT JOIN dim_customer c ON t.customer_key = c.customer_key
LEFT JOIN dim_location l ON t.location_key = l.location_key
WHERE 
    l.country != c.country
    AND l.is_high_risk_country = 1
GROUP BY
    t.customer_key, 
	c.full_name, 
	c.country, 
	l.country, 
	l.is_high_risk_country
HAVING COUNT(*) >= 3
ORDER BY foreign_volume_usd DESC;



/*===========================================================================
DELIVERABLE 4: Merchant & Channel Risk Scoring
============================================================================*/

-- 4.1 Merchant Risk Profiling & Flagged Activity Analysis
-- Aggregates transaction data at the merchant level to evaluate risk by analyzing flagged transactions, 
-- transaction volume, customer reach, and off-hours activity, and ranks merchants based on flagged activity

SELECT
    m.merchant_key,
    m.merchant_name,
    m.merchant_category,
    CASE 
        WHEN m.is_shell_merchant = 1 THEN 'Yes'
        ELSE 'No' 
    END                                              AS is_shell_merchant,
    m.risk_rating,
    m.country,
    COUNT(*)                                         AS total_transactions,
    COUNT(DISTINCT t.customer_key)                   AS unique_customer,
    SUM(t.is_flagged)                                AS flagged_count,
    ROUND(SUM(t.amount_usd), 2)                      AS total_volume,
    ROUND(AVG(t.amount_usd), 2)                      AS avg_volume,
    ROUND((SUM(t.is_flagged) / NULLIF(COUNT(*), 0)) * 100, 2) AS flagged_rate_pct,
    RANK() OVER (ORDER BY SUM(t.is_flagged) DESC)    AS risk_rank,
    SUM(t.is_off_hours)                              AS off_hour_count,
    ROUND(SUM(t.is_off_hours) * 100 / NULLIF(COUNT(*), 0), 2) AS off_hours_pct
FROM fact_transactions AS t
LEFT JOIN dim_merchant AS m ON t.merchant_key = m.merchant_key
GROUP BY
    m.merchant_key,
    m.merchant_name,
    m.merchant_category,
    m.is_shell_merchant,
    m.risk_rating,
    m.country
ORDER BY flagged_count DESC;


-- 4.2 Shell Merchant Transaction Analysis by Country & Channel
-- Analyzes transactions involving shell merchants, breaking down activity by merchant details, transaction location, 
-- risk level, and channel to understand volume, customer reach, and geographic exposure

SELECT
    m.merchant_name,
    m.merchant_category,
    m.country                                      AS merchant_country,
    l.country                                      AS transaction_country,
    CASE 
        WHEN l.is_high_risk_country = 1 THEN 'Yes'
        ELSE 'No' 
    END                                            AS is_high_risk_country,
    t.channel,
    COUNT(*)                                       AS total_transactions,
    COUNT(DISTINCT t.customer_key)                 AS unique_customer,
    ROUND(SUM(amount_usd), 2)                      AS total_volume,
    ROUND(AVG(amount_usd), 2)                      AS avg_volume
FROM fact_transactions AS t
LEFT JOIN dim_merchant AS m ON t.merchant_key = m.merchant_key
LEFT JOIN dim_location AS l ON t.location_key = l.location_key
WHERE m.is_shell_merchant = 1
GROUP BY
    m.merchant_name,
    m.merchant_category,
    m.country,
    l.country,
    l.is_high_risk_country,
    t.channel
ORDER BY total_volume DESC;


-- 4.3 Channel-Level Fraud & Transaction Performance Analysis
-- Evaluates each transaction channel by summarizing customer reach, transaction volume, flagged activity (count and value),
--  and off-hours behavior to identify high-risk channels and fraud concentration

SELECT
    channel,
    COUNT(DISTINCT customer_key)                                   		AS unique_customer,
    COUNT(*)                                                       		AS total_transactions,
    SUM(is_flagged)                                                		AS total_flagged,
    ROUND(SUM(is_flagged) * 100.0 / NULLIF(COUNT(*), 0), 2)        		AS flagged_pct,
    ROUND(SUM(amount_usd), 2)                                      		AS total_volume,
    ROUND(SUM(CASE WHEN is_flagged = 1 THEN amount_usd ELSE 0 END), 2) 	AS flagged_volume,
    ROUND(
        SUM(CASE WHEN is_flagged = 1 THEN amount_usd ELSE 0 END) * 100.0 
        / NULLIF(SUM(amount_usd), 0), 2
    )                                                             		AS flagged_volume_pct,
    ROUND(AVG(amount_usd), 2)                                     		AS avg_volume,
    SUM(is_off_hours)                                             		AS off_hour_count
FROM fact_transactions
GROUP BY channel
ORDER BY flagged_volume DESC;


-- 4.4 Country-Level Transaction & Fraud Risk Analysis
-- Aggregates transactions by country to evaluate total activity, customer reach, flagged transaction rates, 
-- and each country's contribution to overall volume, with emphasis on identifying high-risk countries

SELECT
    l.country,
    CASE 
        WHEN l.is_high_risk_country = 1 THEN 'Yes'
        ELSE 'No' 
    END                                              AS is_high_risk_country,
    COUNT(*)                                         AS total_transactions,
    COUNT(DISTINCT t.customer_key)                   AS unique_customer,
    SUM(is_flagged)                                  AS flagged_count,
    ROUND(SUM(is_flagged) * 100.0 / COUNT(*), 2)     AS flagged_pct,
    ROUND(SUM(t.amount_usd), 2)                      AS total_volume,
    ROUND(
        SUM(t.amount_usd) * 100.0 
        / SUM(SUM(t.amount_usd)) OVER (), 2
    )                                                AS total_volume_pct
FROM fact_transactions AS t
LEFT JOIN dim_location AS l ON t.location_key = l.location_key
GROUP BY 
    l.country,
    l.is_high_risk_country
ORDER BY flagged_pct DESC;


/*===========================================================================
DELIVERABLE 5: Fraud Risk Scoring Model 
============================================================================*/

-- 5.1 Customer Fraud Risk Scoring & Watchlist (Temp Table Creation)
-- Builds a customer-level fraud scoring system using multiple behavioral signals
-- (off-hours activity, spend spikes, shell merchants, high-risk countries, KYC status),
-- assigns risk scores and tiers, and stores the result in a temporary table for reuse in downstream analysis

DROP TABLE IF EXISTS temp_customer_watchlist;

CREATE TEMP TABLE temp_customer_watchlist AS
WITH off_hours_signals AS (
    SELECT
        customer_key,
        COUNT(*) AS total_txns,
        SUM(is_off_hours) AS off_hours_txns,
        ROUND(SUM(is_off_hours) * 100.0 / NULLIF(COUNT(*),0), 2) AS off_hours_pct
    FROM fact_transactions
    GROUP BY customer_key
),
spend_spike_signal AS (
    SELECT
        customer_key,
        ROUND(AVG(CASE WHEN transaction_date < '2024-05-01' THEN amount_usd END), 2) AS early_spend_avg,
        ROUND(AVG(CASE WHEN transaction_date >= '2024-05-01' THEN amount_usd END), 2) AS late_spend_avg,
        ROUND(
            AVG(CASE WHEN transaction_date >= '2024-05-01' THEN amount_usd ELSE 0 END) /
            NULLIF(AVG(CASE WHEN transaction_date < '2024-05-01' THEN amount_usd ELSE 0 END), 0), 
        2) AS spend_multiplier
    FROM fact_transactions
    GROUP BY customer_key
    HAVING 
        COUNT(CASE WHEN transaction_date < '2024-05-01' THEN 1 END) > 3
        AND COUNT(CASE WHEN transaction_date >= '2024-05-01' THEN 1 END) > 3
),
shell_merchant_signal AS (
    SELECT
        t.customer_key,
        ROUND(SUM(m.is_shell_merchant) * 100.0 / NULLIF(COUNT(*), 0), 2) AS shell_rate_pct
    FROM fact_transactions AS t
    LEFT JOIN dim_merchant AS m ON t.merchant_key = m.merchant_key
    GROUP BY t.customer_key
),
high_risk_country_signal AS (
    SELECT
        t.customer_key,
        COUNT(*) AS total_txns,
        SUM(l.is_high_risk_country) AS shell_txns,
        ROUND(SUM(l.is_high_risk_country) * 100.0 / NULLIF(COUNT(*), 0), 2) AS high_risk_country_rate_pct
    FROM fact_transactions AS t
    LEFT JOIN dim_location AS l ON t.location_key = l.location_key
    GROUP BY t.customer_key
),
scoring_system AS (
    SELECT
        o.customer_key,
        c.full_name,
        c.country,
        c.kyc_status,
        c.customer_segment,
        o.off_hours_pct,
        sp.spend_multiplier,
        sh.shell_rate_pct,
        h.high_risk_country_rate_pct,

        CASE
            WHEN o.off_hours_pct >= 40 THEN 25
            WHEN o.off_hours_pct >= 25 THEN 18
            WHEN o.off_hours_pct >= 10 THEN 10
            ELSE 3
        END AS off_hours_score,

        CASE
            WHEN sp.spend_multiplier IS NULL THEN 0
            WHEN sp.spend_multiplier >= 10 THEN 25
            WHEN sp.spend_multiplier >= 5 THEN 18
            WHEN sp.spend_multiplier >= 2 THEN 10
            ELSE 2
        END AS spend_multiplier_score,

        CASE
            WHEN sh.shell_rate_pct >= 50 THEN 20
            WHEN sh.shell_rate_pct >= 25 THEN 13
            WHEN sh.shell_rate_pct >= 10 THEN 7
            ELSE 1
        END AS shell_merchant_score,

        CASE
            WHEN h.high_risk_country_rate_pct >= 50 THEN 20
            WHEN h.high_risk_country_rate_pct >= 20 THEN 13
            WHEN h.high_risk_country_rate_pct >= 5 THEN 6
            ELSE 0
        END AS high_risk_country_score,

        CASE
            WHEN c.kyc_status = 'Expired' THEN 10
            WHEN c.kyc_status = 'Pending' THEN 5
            ELSE 0
        END AS kyc_status_score

    FROM off_hours_signals AS o
    LEFT JOIN dim_customer AS c ON o.customer_key = c.customer_key
    LEFT JOIN spend_spike_signal AS sp ON o.customer_key = sp.customer_key
    LEFT JOIN shell_merchant_signal AS sh ON o.customer_key = sh.customer_key
    LEFT JOIN high_risk_country_signal AS h ON o.customer_key = h.customer_key
),
watchlist AS (
    SELECT
        *,
        off_hours_score + spend_multiplier_score + shell_merchant_score 
        + high_risk_country_score + kyc_status_score AS total_risk_score,

        CASE 
            WHEN off_hours_score + spend_multiplier_score + shell_merchant_score 
               + high_risk_country_score + kyc_status_score >= 70 THEN 'CRITICAL'
            WHEN off_hours_score + spend_multiplier_score + shell_merchant_score 
               + high_risk_country_score + kyc_status_score >= 50 THEN 'HIGH'
            WHEN off_hours_score + spend_multiplier_score + shell_merchant_score 
               + high_risk_country_score + kyc_status_score >= 30 THEN 'MEDUIM'
            ELSE 'LOW'
        END AS risk_tier,

        ROUND(
            (PERCENT_RANK() OVER (
                ORDER BY off_hours_score + spend_multiplier_score + shell_merchant_score 
                + high_risk_country_score + kyc_status_score
            ) * 100)::NUMERIC, 2
        ) AS risk_percentile

    FROM scoring_system
)
SELECT
    customer_key,
    full_name,
    country,
    kyc_status,
    customer_segment,
    off_hours_pct,
    spend_multiplier,
    shell_rate_pct,
    high_risk_country_rate_pct,
    off_hours_score,
    spend_multiplier_score,
    shell_merchant_score, 
    high_risk_country_score,
    kyc_status_score,
    total_risk_score,
    risk_tier,
    risk_percentile
FROM watchlist
ORDER BY total_risk_score DESC;



















