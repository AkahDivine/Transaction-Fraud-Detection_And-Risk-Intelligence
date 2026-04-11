-- ============================================
-- DDL SCRIPT
-- Creates dimension and fact tables for a transaction/fraud analysis model
-- WARNING: DROP TABLE IF EXISTS will permanently delete existing tables and data
-- ============================================


-- Account dimension (stores account details)
DROP TABLE IF EXISTS dim_account;

CREATE TABLE dim_account (
    account_key INT,
    account_id VARCHAR(25),
    customer_key INT,
    account_type VARCHAR(25),
    currency VARCHAR(5),
    credit_limit DECIMAL(10,2),
    current_balance DECIMAL(10,2),
    account_status VARCHAR(25),
    open_date DATE,
    balance_tier VARCHAR(10)
);


-- Customer dimension (stores customer profile and segmentation info)
DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
	customer_key INT,
	customer_id VARCHAR(25),
	first_name VARCHAR(25),
	last_name VARCHAR(25),
	full_name VARCHAR(50),
	email VARCHAR(100),
	phone VARCHAR(20),
	country VARCHAR(50),
	city VARCHAR(50),
	location_key INT,
	kyc_status VARCHAR(25),
	preferred_channel VARCHAR(50),
	join_date DATE,
	customer_segment VARCHAR(50),
	is_fraud_target INT	
);


-- Date dimension (supports time-based analysis)
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
	date_key INT,
	full_date DATE,
	day_of_week VARCHAR(15),
	day_number INT,
	week_number INT,
	month_number INT,
	month_name VARCHAR(15),
	quarter_number INT,
	quarter_name VARCHAR(5),
	year_number INT,
	is_weekend INT,
	is_month_end INT
);


-- Location dimension (geographical and risk info)
DROP TABLE IF EXISTS dim_location;

CREATE TABLE dim_location (
	location_key INT,
	country VARCHAR(50),
	city VARCHAR(50),
	region VARCHAR(50),
	is_high_risk_country INT
);


-- Merchant dimension (merchant details and risk indicators)
DROP TABLE IF EXISTS dim_merchant;

CREATE TABLE dim_merchant (
    merchant_key INT,
    merchant_name VARCHAR(50),
    merchant_category VARCHAR(50),
    is_shell_merchant INT,
    risk_rating VARCHAR(10),
    country VARCHAR(50)
);


-- Fact table (stores transaction records and fraud flags)
DROP TABLE IF EXISTS fact_transactions;

CREATE TABLE fact_transactions (
    transaction_key INT,
    transaction_id VARCHAR(25),
    customer_key INT,
    account_key INT,
    merchant_key INT,
    location_key INT,
    date_key INT,
    transaction_datetime TIMESTAMP,
    transaction_date DATE,
    transaction_hour INT,
    transaction_type VARCHAR(30),
    channel VARCHAR(30),
    amount_usd DECIMAL(12,2),
    currency VARCHAR(5),
    is_flagged INT,
    fraud_type VARCHAR(50),
    is_off_hours INT,
    status VARCHAR(20)
);
