-- ============================================
-- DATA LOAD (DML) SCRIPT
-- Loads fresh data from CSV files into dimension and fact tables
-- WARNING: TRUNCATE TABLE will permanently delete all existing data before loading new data
-- ============================================


-- Load account data
TRUNCATE TABLE dim_account; 

COPY dim_account(
	account_key, 
	account_id, 
	customer_key, 
	account_type, 
	currency, 
	credit_limit, 
	current_balance, 
	account_status, 
	open_date, 
	balance_tier
)
FROM 'input your file path here'
DELIMITER ','
CSV HEADER;


-- Load customer data
TRUNCATE TABLE dim_customer;

COPY dim_customer(
	customer_key,
	customer_id,
	first_name,
	last_name,
	full_name,
	email,
	phone,
	country,
	city,
	location_key,
	kyc_status,
	preferred_channel,
	join_date,
	customer_segment,
	is_fraud_target
)
FROM 'input your file path here'
DELIMITER ','
CSV HEADER;


-- Load date data
TRUNCATE TABLE dim_date;

COPY dim_date(
    date_key,
    full_date,
    day_of_week,
    day_number,
    week_number,
    month_number,
    month_name,
    quarter_number,
    quarter_name,
    year_number,
    is_weekend,
    is_month_end
)
FROM 'input your file path here'
DELIMITER ','
CSV HEADER;


-- Load location data
TRUNCATE TABLE dim_location;

COPY dim_location(
    location_key,
    country,
    city,
    region,
    is_high_risk_country
)
FROM 'input your file path here'
DELIMITER ','
CSV HEADER;


-- Load merchant data
TRUNCATE TABLE dim_merchant;

COPY dim_merchant(
    merchant_key,
    merchant_name,
    merchant_category,
    is_shell_merchant,
    risk_rating,
    country
)
FROM 'input your file path here'
DELIMITER ','
CSV HEADER;


-- Load transaction data (fact table)
TRUNCATE TABLE fact_transactions;

COPY fact_transactions(
    transaction_key,
    transaction_id,
    customer_key,
    account_key,
    merchant_key,
    location_key,
    date_key,
    transaction_datetime,
    transaction_date,
    transaction_hour,
    transaction_type,
    channel,
    amount_usd,
    currency,
    is_flagged,
    fraud_type,
    is_off_hours,
    status
)
FROM 'input your file path here'
DELIMITER ','
CSV HEADER;
