# Exploratory Data Analysis (EDA) with Spark

Status: implementation ready; full-stack execution and screenshots pending.

## 01. Load the bank.csv

src/eda.py via spark_job.py eda; 01_preview.csv and printed schema. Use real bank.csv, not bank-additional.csv.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 02. Perform basic data inspection

01_preview.csv, 01_numeric_summary.csv, and 01_quality.json show rows, numeric summaries, and missing values. Capture printed schema.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 03. Filter the dataset

02_balance_over_threshold.csv contains a bounded preview of balance>1000. Change --balance-threshold if needed; source DataFrame filtering processes all rows.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 04. Extract the month

02_04_06_engineered_columns.csv maps month abbreviations to month_number and ceil(month_number/3) as quarter. There is no full contact date/year.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 05. Group the data by job

03_job_balance_age.csv contains average balance and approximate median age (percentile_approx accuracy 10000). Explain the approximation.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 06. Find the total number

03_subscribers_by_marital.csv counts y=yes by marital status.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 07. Write a UDF

banklib.age_group is registered as a Spark Python UDF: age<30, 30<=age<=60, age>60; null age becomes unknown. Engineered output demonstrates its use.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 08. Calculate the subscription rate

05_education_subscription.csv contains record counts and subscription percentages by education.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 09. Identify the top 3

05_top3_credit_default_jobs.csv ranks observed existing-credit-default rates. Unknown default statuses are excluded from the rate denominator. Loan-specific defaults cannot be established from this field.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 10. Concatenate the

02_04_06_engineered_columns.csv includes job_marital using concat_ws.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 11. Use string functions

02_04_06_engineered_columns.csv includes contact_upper using upper().

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 12. Convert the Spark DataFrame

export() calls toPandas only on bounded samples and small aggregates, rather than collecting an unbounded dataset. This fulfills conversion while limiting driver memory.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 13. Create a bar plot

07_job_counts.png and corresponding CSV show clients by job.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 14. Analyze which month

08_month_contacts.csv and 08_busiest_month.csv include contacts and success percentage.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 15. Find the average duration

08_duration_by_subscription.csv reports mean contact seconds, grouped by y.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 16. Calculate the correlation coefficient

09_age_balance_correlation.json reports Pearson r on complete age/balance pairs. Discuss direction and strength from the actual output.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 17. Analyze the proportion

10_credit_default_counts.csv contains default counts and proportions; 10_credit_default_counts.png plots counts. Keep unknown as a separate category.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 18. Investigate which contact method

11_contact_success.csv ranks subscription success percent by contact, with sample sizes. Observational differences are not causal effects.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 19. Calculate the average account balance

12_sql_age_balance.csv comes from a Spark temporary view and SQL GROUP BY age_group. Include the SQL and observed table.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.
