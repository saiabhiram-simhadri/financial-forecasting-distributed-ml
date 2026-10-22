# Verification status

## Checked on this workstation

- Thirteen standard-library tests pass. They cover CSV schema/domains/dialects, age boundaries, five MapReduce tasks and combiner equivalence, missing-value means, bounded replay, UCI direct/nested ZIP layouts and size bounds, persistent Codespaces XML/resource settings, checksum parsing, input class/dialect checks, and safe tar extraction.
- Python sources, including scripts/codespaces/, and notebook code cells pass AST parsing.
- Six shell scripts and 26 Bash command blocks in the current Codespaces runbook pass bash -n syntax checks without executing the commands.
- Five annotated DOCX archives/XML and local Markdown links pass structural checks. There are 68 mapped task prompts and three unexecuted reference notebooks.
- The devcontainer JSON parses successfully.
- No real data, Java/Hadoop/Hive/Spark distributions or third-party Python packages were downloaded/installed for these checks. No distributed service or training job ran locally.

The small configured Python environment runs standard-library tests. Temporary test archives/configuration files are tiny and cleaned up.

```powershell
& .\.venv\Scripts\python.exe -B -m unittest discover -s tests -v
& .\.venv\Scripts\python.exe -B scripts/check_project.py
```

## Codespaces execution is not yet verified

The supplied devcontainer, explicit installer, generated Hadoop/Hive configuration, service lifecycle scripts and staged runner are implemented, but no Codespace was provisioned or operated during these checks.

The following still require actual verification inside Codespaces:

- Availability of installation downloads, package wheels and account quota.
- HDFS/DataNode and YARN startup, real MapReduce task execution and counters.
- Hive embedded catalog initialization, HiveServer2 readiness and all Hive queries.
- Spark EDA/plans/plots, CV fitting, model persistence/loading and scored output.
- Streaming query progress, late-event behavior and prediction-count agreement.
- Real dataset metrics, resource observations and submission screenshots.

Static checks and offline tests do not guarantee a full runtime succeeds on a particular host.

Follow [EXECUTION.md](EXECUTION.md) in order. It records readiness gates and troubleshooting for each stage. The prepare command runs lightweight tests; the stage commands perform the real experiments, and verify checks their actual outputs.

## Evidence remains user-completed

Templates and notebooks contain no invented results. Complete measured interpretations, code/output screenshots and required documents after execution. Explain the absent year/loan-amount fields.

tests/test_spark_remote.py remains an optional synthetic integration suite requiring RUN_SPARK_TESTS=1 on a prepared runtime. It is skipped by default; running all normal real-data stages already performs the essential execution and output checks.
