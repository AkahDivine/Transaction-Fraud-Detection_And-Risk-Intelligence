# Operation Clearwater: Bank Fraud Detection and Risk Intelligence

## Scenario

NorthAxis Bank noticed a sudden increase in fraud complaints and suspicious transactions in 2024, with over **$2.3 million** potentially lost. As a **Junior Data Analyst** in the Risk Intelligence team, the goal of this project is to use **SQL** to analyze transaction data, detect unusual patterns, identify high-risk customers, and generate insights to help the bank reduce fraud risks.

## Key Fraud Intelligence Metrics

| Metric | Value |
|------|------|
| Total transaction review | 195,276 |
| Flagged transactions | 19,741 |
| Total portfolio volume | $355,057,585 |
| Estimated fraud volume | $267,303,004 |
| % of portfolio exposed | 75.28% |
| Fraud rate | 10.11% |
| Accounts recommended to freeze | 268 |
| Shell merchants (100% flag rate) | 8 |
| High-risk countries (100% flagged) | 10 |

## The Investigation

### Deliverable 1: Transaction Overview & Baseline Key Performance Indicators

Before investigating anomalies, it was important to establish what “normal” transaction behavior looks like across the system.

Mobile banking was the leading channel, accounting for **39.93% of total transactions** and **41.10% of total transaction value**, making it the dominant channel for customer activity.

KYC status analysis showed that **80.50% of customers were KYC verified**, with an average transaction amount of **$1,799**. Customers with pending KYC status made up **11.36%**, with an average transaction amount of **$1,790**, while **7.98% had expired KYC status**, recording a higher average transaction amount of **$2,045**, indicating potential elevated risk.

Monthly trends showed steady growth in transaction volume. From January to April, the average monthly volume was approximately **$20 million**. In May, volume surged by **129.23%** to about **$46 million**, followed by consistent growth from June through September.
