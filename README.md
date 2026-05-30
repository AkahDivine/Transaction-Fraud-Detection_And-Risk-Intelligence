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

### Deliverable 2: Anomaly Detection & Velocity Checks

With a baseline established, the investigation moved into anomaly detection to uncover behaviors that deviated from normal transaction patterns. To do this, four key anomaly checks were performed.

The first check focused on **rapid transactions** using the `LAG` window function to identify customers making repeated transactions within short periods. The analysis highlighted the **top 50 customers with five or more transactions within 60 minutes**. The highest offender recorded **101 transactions**, with a minimum transaction gap of just **0.03 minutes**, a maximum of **3.58 minutes**, and an average transaction amount of **$5,941**, indicating unusually aggressive activity.

The second check examined **transaction behavior by time window**. Transactions between **1:00 AM and 4:00 AM** were classified as off-hours, **8:00 AM to 6:00 PM** as business hours, and all remaining periods as other hours. Off-hour transactions recorded the highest average volume at **$4,896**, compared to **$1,295 during business hours** and **$2,157 during other hours**, suggesting suspiciously high-value activity during periods of reduced monitoring.

The third check focused on **high-value outliers** using the **z-score method** to identify transactions significantly above normal behavior. After calculating the average and standard deviation of transaction amounts, the analysis surfaced the **top 100 extreme transactions (z-score > 3)**. Nearly all were **wire transfers processed through shell merchants**, many originating from **high-risk countries**, with z-scores ranging between **8.10 and 8.20**, making them strong candidates for suspicious activity.

Finally, a **daily spike detection analysis** compared daily transaction volume against a rolling **7-day average**. The findings aligned with the earlier month-on-month trend, showing **April 30** as the only day to exceed the anomaly threshold, reaching **2.04 times the rolling average**, signaling the beginning of unusual transaction growth.

### Deliverable 3: Customer Risk Profiling

The focus of this stage was to identify the customers behind the unusual transaction patterns and understand who was driving the suspicious activity.

To detect possible account takeover signals, customer transaction history was divided into two periods: an early period (January to April) and a late period (May to September). Spending behavior across both periods was then compared. The findings were significant — several high-risk customers showed a spending increase of 76 to 105 times in the late period, meaning their transaction activity grew dramatically compared to earlier months. One of the highest-risk accounts also had an expired KYC status, while transactions expanded from a single country in the early period to nearly 15 countries later in the year, suggesting unusual account behavior.

Geographic anomalies revealed another pattern. A number of customers began transacting in multiple foreign high-risk countries, with some accounts showing activity across up to 10 high-risk locations. Notably, this behavior started around 30th April and continued through September, aligning with the period when suspicious activity across the portfolio increased.

### Deliverable 4: Merchant and Channel Risk Scoring

At this stage of the investigation, the focus shifted to understanding **where the money was flowing and through which channels suspicious activity was concentrated**.

Merchant risk profiling revealed a concerning pattern. The **top eight merchants recorded a 100% flagged transaction rate**, meaning every transaction associated with them was marked suspicious. Around **40% of these transactions occurred during off-hours**, further increasing their risk profile. More importantly, all eight merchants belonged to the **Shell Merchant category**, immediately making them a priority for deeper investigation.

A closer review of these shell merchants exposed a stronger pattern. The entities — **PrimeFin Corp (Iran), ClearPath Remit (Sudan), SwiftFunds Inc (Syria), TrustEx Global (Iran), GlobalTrade Ltd (North Korea), FastBridge Finance (Venezuela), Apex Transfers (Cuba), and NovaPay Solutions (Sudan)** — all recorded entirely flagged activity. Rather than appearing to be legitimate businesses being exploited, the evidence suggests these merchants were likely acting as the **mechanism through which suspicious funds moved**.

Channel-level analysis showed that the fraud activity was largely digital. **Mobile banking recorded the highest flagged transaction rate at 14.36%** of its total, while **web banking accounted for the highest flagged transaction volume at 80.64%** of its total. Mobile banking also recorded the highest number of off-hours transactions, with **more than 8,500 transactions occurring between unusual hours**, reinforcing the concentration of suspicious activity on digital channels.

Country-level analysis revealed an even stronger signal. **Ten high-risk countries — Venezuela, Cuba, Iran, Myanmar, North Korea, Russia, Somalia, Sudan, Syria, and Belarus — recorded a 100% flagged transaction rate**, with no legitimate transactions observed. Combined, these countries accounted for **$153.3 million in transaction volume, representing 43% of the total portfolio**, making them a major source of the bank’s fraud exposure.

