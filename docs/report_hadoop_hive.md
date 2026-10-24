# Data Analysis and Management using Hadoop & Hive

Status: implementation ready; full-stack execution and screenshots pending.

For the first six Hadoop questions, follow the [exact Step 5 screenshot checklist](EXECUTION.md#step-5-screenshot-checklist-exactly-what-to-put-under-each-question). It specifies code, saved output commands, screenshot filenames and interpretation for each answer.

## 01. Create a directory in HDFS

scripts/run_hadoop.sh creates a run-specific HDFS raw directory and uploads bank.csv with hdfs dfs -put. Capture mkdir/put/ls and record the RAW_PATH printed at the end. Validate the CSV first.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 02. Write a MapReduce program in Python

MR01: mapper.py --task balance_by_job; reducer output is key, sum, nonmissing count, mean. Explain why sum/count combiners preserve weighted means. Capture code and all reducer partitions.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 03. Write another MapReduce program

MR02: --task housing_by_education groups by education|housing. Count is column 3; unknown housing is a separate bucket. Capture counters and output.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 04. Perform a MapReduce job

MR03: --task contacts_by_month groups by month|y. Sum/count are contact-record counts, not unique-person counts; no client ID is available.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 05. Analyze the average duration

MR04: --task duration_by_poutcome calculates mean contact seconds for each previous campaign outcome. This is not transaction duration.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 06. Examine the relationship

MR05: --task balance_by_age summarizes average balance for <30, 30-60, >60 age groups. Complement with H09/EDA09 Pearson correlation. Report observed values without claiming causality.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 07. Create a Hive database

H01: hive/01_tables.hql creates banking_data. Run using beeline and the RAW_PATH printed by run_hadoop.sh. Capture database/table creation.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 08. Define and create a Hive table

H01: client_info_raw uses OpenCSVSerde strings to parse quoted CSV; client_info stores typed INT/BIGINT/STRING columns as ORC. Capture DESCRIBE banking_data.client_info.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 09. Load the data

H01: external raw table points at the uploaded HDFS directory; INSERT OVERWRITE TABLE client_info casts into ORC. This refreshes the project's derived table; compare raw/typed row counts.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 10. Write a HiveQL query to count

H02 in hive/02_analysis.hql: total row count. Include query and actual count.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 11. Display the first 10

H02: SELECT * LIMIT 10; order is unspecified because no unique row ID exists.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 12. Retrieve all records

H03: marital='married' AND loan='yes'. Capture query/output; retain complete results remotely if longer than the screenshot.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 13. List the top 10

H04: descending balance with job/marital fields. These are records, not necessarily 10 distinct clients.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 14. Calculate the average age

H05: average age and record count by job.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 15. Find the total number

H06: existing credit default='yes', grouped by education. This is not a future-default prediction.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 16. Identify the top 5

H07: rank average balance and report subscription percentage for those job categories, with category counts.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 17. Determine the month

H08: contact count descending and current subscription success percentage; ties break alphabetically.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 18. Calculate the correlation

H09: CORR(age,balance), Pearson correlation. Undefined for constant variables; association is not causation.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 19. Analyze the year-over-year

H10 is explicitly not identifiable: bank.csv lacks year or contact_date. Do not invent years or infer them from row order. Obtain an observed dated dataset before completing a real YoY analysis.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 20. Identify any unusual patterns

H11: education balance summaries and descriptive z-scores of group means. Bank balance is described as average yearly balance, but no longitudinal annual series exists. Flag candidates for review, not proven anomalies.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 21. Analyze the impact

H12: current subscription percentage for each poutcome, including unknown. Discuss observed association and counts.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 22. Compare the average contact duration

H13: mean contact seconds for y=yes versus y=no. Duration is post-call information and is excluded from pre-call prediction by default.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.
