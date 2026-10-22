USE banking_data;
SELECT 'H02 total clients and first 10 rows' AS question;
SELECT COUNT(*) AS total_clients FROM client_info;
SELECT * FROM client_info LIMIT 10;

SELECT 'H03 married with personal loan' AS question;
SELECT * FROM client_info WHERE marital = 'married' AND loan = 'yes';

SELECT 'H04 highest balances' AS question;
SELECT job, marital, balance FROM client_info
ORDER BY balance DESC, job, marital LIMIT 10;

SELECT 'H05 average age by job' AS question;
SELECT job, COUNT(*) AS clients, AVG(age) AS avg_age FROM client_info GROUP BY job ORDER BY job;

SELECT 'H06 credit defaults by education' AS question;
SELECT education, COUNT(*) AS defaulted_clients FROM client_info
WHERE `default` = 'yes' GROUP BY education ORDER BY education;

SELECT 'H07 top 5 jobs by mean balance and subscription percent' AS question;
SELECT job, AVG(balance) AS avg_balance, COUNT(*) AS clients,
       100.0 * AVG(CASE WHEN y='yes' THEN 1.0 ELSE 0.0 END) AS subscription_pct
FROM client_info GROUP BY job ORDER BY avg_balance DESC, job LIMIT 5;

SELECT 'H08 busiest contact month and success rate' AS question;
SELECT month, COUNT(*) AS contacts,
       100.0 * AVG(CASE WHEN y='yes' THEN 1.0 ELSE 0.0 END) AS success_pct
FROM client_info GROUP BY month ORDER BY contacts DESC, month LIMIT 1;

SELECT 'H09 age balance correlation' AS question;
SELECT CORR(age, balance) AS age_balance_pearson_r FROM client_info;

SELECT 'H10 year-over-year contacts: not identifiable' AS question;
SELECT 'bank.csv has day and month but no year. Supply observed contact_year/contact_date before computing YoY; do not infer year from row order.' AS limitation;

SELECT 'H11 education balance anomaly candidates (cross-sectional, not yearly trend)' AS question;
WITH education_summary AS (
  SELECT education, COUNT(*) AS clients, AVG(balance) AS avg_balance,
         STDDEV_POP(balance) AS balance_stddev, MIN(balance) AS min_balance,
         MAX(balance) AS max_balance
  FROM client_info GROUP BY education
), across_groups AS (
  SELECT AVG(avg_balance) AS mean_of_group_means,
         STDDEV_POP(avg_balance) AS std_of_group_means
  FROM education_summary
)
SELECT e.*, CASE WHEN a.std_of_group_means > 0
       THEN (e.avg_balance - a.mean_of_group_means)/a.std_of_group_means
       ELSE NULL END AS descriptive_z_score,
       CASE WHEN a.std_of_group_means > 0 AND
       ABS(e.avg_balance-a.mean_of_group_means) > 2*a.std_of_group_means
       THEN 'review' ELSE 'within_descriptive_threshold' END AS review_flag
FROM education_summary e CROSS JOIN across_groups a
ORDER BY avg_balance DESC;
-- Small category counts and skew can mask anomalies. These are review candidates,
-- not fraud findings; account balance is described as average yearly balance by UCI.

SELECT 'H12 previous outcome and current success' AS question;
SELECT poutcome, COUNT(*) AS clients,
       100.0 * AVG(CASE WHEN y='yes' THEN 1.0 ELSE 0.0 END) AS subscription_pct
FROM client_info GROUP BY poutcome ORDER BY subscription_pct DESC, poutcome;

SELECT 'H13 contact duration by subscription' AS question;
SELECT y, COUNT(*) AS clients, AVG(duration) AS avg_contact_seconds
FROM client_info GROUP BY y ORDER BY y;
