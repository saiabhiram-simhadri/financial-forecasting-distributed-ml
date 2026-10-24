# Hadoop and Hive banking analysis

This repository contains only the Hadoop/Hive assignment. The other four assignments were completed separately in notebooks and are not executed, checked or packaged here.

Read [the overview](docs/PROJECT_OVERVIEW.md), then follow [the execution and screenshot guide](docs/EXECUTION.md).

For a new GitHub Codespace only, run bash scripts/codespaces/setup.sh. Place the instructor's semicolon-delimited bank.csv at data/bank.csv. In that remote terminal:

```bash
bash scripts/codespaces/run.sh prepare
bash scripts/codespaces/run.sh hadoop
bash scripts/codespaces/run.sh hive
bash scripts/codespaces/run.sh verify
bash scripts/codespaces/run.sh package
```

For your completed run, resume rather than repeating setup or successful jobs (Step 9).

- [MapReduce](mapreduce/): five summaries with Python mapper, combiner and reducer.
- [Hive](hive/): string CSV staging, typed ORC table and SQL questions.
- [Question guide](docs/report_hadoop_hive.md): 22 assignment prompts.
- [Validation status](docs/VALIDATION.md): checks and execution limits.

Setup uses Java 8, Hadoop 3.3.6, Hive 4.0.1 and standard-library Python. No Spark or pip packages are needed. Existing persistent paths (.codespaces-state and artifacts/codespaces) are unchanged; source cleanup does not erase remote results.

This is single-host pseudo-distributed Hadoop. No year field exists, so year-over-year contacts cannot be computed. Complete screenshots and measured interpretations; the annotated DOCX is a template, not finished evidence. Submit your four separate notebooks independently.