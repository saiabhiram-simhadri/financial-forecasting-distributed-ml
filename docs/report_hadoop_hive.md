# Banking Analysis Implementation

The analysis uses bank-marketing contact records. This document describes implemented calculations; measured results are stored with individual execution runs.

## MapReduce summaries

| Task | Grouping | Calculation |
|---|---|---|
| `balance_by_job` | Job | Mean balance and nonmissing count |
| `housing_by_education` | Education and housing-loan status | Record count, including unknown housing status |
| `contacts_by_month` | Month and subscription outcome | Contact-record count |
| `duration_by_poutcome` | Previous campaign outcome | Mean contact duration in seconds |
| `balance_by_age` | Under 30, 30-60, over 60 | Mean balance and nonmissing count |

The mapper emits grouping keys, sums, and counts. Combiners merge sums and counts; reducers calculate means from the combined totals. Output columns are key, sum, nonmissing count, and mean. Count tasks use the count column.

Implementation: [mapper](../mapreduce/mapper.py), [reducer](../mapreduce/reducer.py), and [aggregation logic](../src/banklib.py).

## Hive storage

The `banking_data` database contains the external CSV staging table `client_info_raw` and the typed ORC table `client_info`. OpenCSVSerde parses quoted fields. An `INSERT OVERWRITE` operation casts and normalizes staging data into the derived table.

Implementation: [table definitions](../hive/01_tables.hql).

## Hive analysis

| Query | Calculation |
|---|---|
| H02 | Total records and a ten-record sample |
| H03 | Married records with a personal loan |
| H04 | Ten records with the highest balances |
| H05 | Average age and record count by job |
| H06 | Existing credit-default counts by education |
| H07 | Five job categories with the highest mean balance, category counts, and subscription percentages |
| H08 | Busiest contact month and subscription percentage |
| H09 | Pearson correlation between age and balance |
| H10 | Missing-year limitation for year-over-year analysis |
| H11 | Education-level balance summaries and descriptive z-scores of group means |
| H12 | Subscription percentage and record count by previous campaign outcome |
| H13 | Mean contact duration by subscription outcome |

Implementation: [analytical queries](../hive/02_analysis.hql).

## Interpretation

The records lack unique customer identifiers, so contact counts and top-balance records cannot establish distinct customer counts. The sample query has no defined ordering. Credit-default counts describe an existing attribute rather than predicted future defaults.

The dataset lacks a year or full contact date. Monthly comparisons do not establish year-over-year trends. Balance screening is descriptive and does not establish fraud. Correlation and campaign-outcome comparisons describe association rather than causation. Completed-call duration is unavailable for prediction before a call.
