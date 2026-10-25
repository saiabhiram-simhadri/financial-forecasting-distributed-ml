# Banking Analytics with Hadoop and Spark

A banking analytics project covering customer and campaign analysis, term-deposit prediction, and simulated streaming using Hadoop, Hive, and Apache Spark.

## Overview

- Hadoop/Hive implementation includes five MapReduce summaries, typed ORC tables, and analytical SQL queries.
- Four Spark notebooks cover exploratory analysis, predictive modeling, streaming predictions, and data parallelism.
- Hadoop runs in single-host pseudo-distributed mode. Spark notebooks use local execution. The latest script cleanup has not been rerun against live Hadoop/Hive services.

## What the project covers

| Area | Work |
|---|---|
| Hadoop and Hive | Store records in HDFS and analyze balances, loans, and campaign outcomes |
| Spark EDA | Explore customer groups, subscription rates, and contact patterns |
| Spark ML | Compare Logistic Regression, Decision Tree, and Random Forest for subscription prediction |
| Structured Streaming | Simulate arriving records, update summaries, and apply a trained model |
| Data parallelism | Explore partitioning, parallel processing, and resource usage |

Streaming uses a fixed trained model, retraining is separate. These components are separate workflows rather than one deployed production pipeline.

## Repository

- [notebooks/](notebooks/) — Spark analysis and experiments
- [mapreduce/](mapreduce/) and [hive/](hive/) — batch processing and SQL
- [src/](src/) and [scripts/](scripts/) — shared logic, setup, and execution
- [Analysis reference](docs/report_hadoop_hive.md) — implemented calculations

## Data

Bank-marketing contact records include customer attributes, balances, campaign details, and subscription outcomes. The Hadoop downloader uses [UCI Bank Marketing](https://archive.ics.uci.edu/dataset/222/bank+marketing).

Counts represent records, and the missing year field prevents annual trend analysis. Models using call duration require completed-call information. Runtime data and Hadoop/Hive results are excluded from Git.
