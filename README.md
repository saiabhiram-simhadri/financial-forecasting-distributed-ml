# Hadoop and Hive Banking Analysis

Bank-marketing analysis using HDFS, Python MapReduce, and Hive SQL. The implementation covers customer balance summaries, loan distributions, campaign activity, and subscription outcomes.

## Implementation

- Five MapReduce jobs with sum/count combiners and grouped summaries.
- CSV staging in Hive and conversion to a typed ORC table.
- HiveQL queries for customer segments, campaign outcomes, and descriptive statistics.
- Codespaces runtime configuration, input validation, output verification, and result packaging.
- Offline tests for aggregation, ingestion guards, downloads, and runtime configuration.

## Project files

| Directory | Content |
|---|---|
| [mapreduce](mapreduce/) | Python mapper and reducer |
| [src](src/) | CSV parsing and aggregation logic |
| [hive](hive/) | Table definitions and analytical queries |
| [scripts](scripts/) | Validation, execution, and packaging utilities |
| [tests](tests/) | Offline tests |

[Architecture](docs/project_architecture.md) describes the processing flow. [Analysis](docs/report_hadoop_hive.md) summarizes the implemented calculations.

The runtime uses Hadoop 3.3.6, Hive 4.0.1, Java 8, and standard-library Python in a single-host pseudo-distributed environment. Spark work is maintained separately and is outside this repository.

The dataset contains contact records without unique customer identifiers or a year field. Counts represent records, and year-over-year analysis is unavailable. Numerical findings require the corresponding execution outputs; this checkout contains implementation code rather than a completed results report.
