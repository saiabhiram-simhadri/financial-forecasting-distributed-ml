# Distributed banking analytics with Hadoop, Hive, and Spark

Implements [project_rubric.txt](project_rubric.txt) and the detailed questions from all five supplied DOCX assignments. The prediction target is **term-deposit subscription** (`y`), rather than price forecasting. Streaming replays marketing contact records with simulated timestamps.

**Read [the project overview](docs/PROJECT_OVERVIEW.md) first** to understand the tools, data flow and common obstacles. Then follow [the execution guide](docs/EXECUTION.md). The default route is now GitHub Codespaces: the supplied dev container performs read-only host checks, and an explicit setup command installs the runtimes inside the codespace. No large installation or processing is performed on the Windows workstation.


Use the Codespaces browser terminal, after committing this checkout and creating the supplied dev container:

```bash
bash scripts/codespaces/setup.sh
bash scripts/codespaces/run.sh prepare
bash scripts/codespaces/run.sh hadoop
bash scripts/codespaces/run.sh hive
bash scripts/codespaces/run.sh eda
bash scripts/codespaces/run.sh train
bash scripts/codespaces/run.sh parallel
bash scripts/codespaces/run.sh stream
bash scripts/codespaces/run.sh verify
```

Follow the execution guide for screenshots, history UI, packaging and shutdown. Setup downloads are substantial and happen only in Codespaces. This is one host; full Codespaces execution still requires runtime verification.

| Assignment | Implementation | Question-by-question guide |
|---|---|---|
| Hadoop and Hive | [MapReduce](mapreduce/), [Hadoop launcher](scripts/run_hadoop.sh), [Hive](hive/) | [Hadoop/Hive](docs/report_hadoop_hive.md) |
| Spark EDA | [EDA](src/eda.py), [notebook](notebooks/01_eda.ipynb) | [EDA](docs/report_eda.md) |
| Spark ML | [Modeling](src/modeling.py), [notebook](notebooks/02_modeling.ipynb) | [Modeling](docs/report_modeling.md) |
| Real-time analysis | [Replay](scripts/replay.py), [streaming](src/streaming.py) | [Streaming](docs/report_streaming.md) |
| Data parallelism | [Experiments](src/parallelism.py), [notebook](notebooks/03_parallelism.ipynb) | [Parallelism](docs/report_parallelism.md) |

The [five annotated DOCX copies](submissions/) preserve the original questions and add implementation pointers and evidence placeholders under **68 task prompts**. Originals are unchanged. Notebooks are unexecuted; they become submission evidence after running and saving their real outputs.

Two requested analyses cannot be computed from the specified dataset: **year-over-year contacts** (no year) and **loan amounts by age** (no loan-amount field). The project documents those limits and supplies a clearly labeled loan-holder-count alternative. No years, amounts, performance measurements, or screenshots are fabricated.

The model handles missing values, reduces outlier influence with fixed log transforms, encodes categories, uses a duplicate-safe deterministic train/test split, tunes a complete pipeline with cross-validation, reports imbalance-aware metrics, and saves a PipelineModel for streaming. Call duration is excluded by default because it is unavailable before the call ends.

See [design and data limits](docs/DESIGN.md), [submission checklist](docs/SUBMISSION.md), and [verification status](docs/VALIDATION.md).

Sources: [UCI Bank Marketing](https://archive.ics.uci.edu/dataset/222/bank+marketing), [Spark 3.5.7 streaming](https://spark.apache.org/docs/3.5.7/structured-streaming-programming-guide.html), [Spark ML tuning](https://spark.apache.org/docs/3.5.7/ml-tuning.html), [Hive SerDe](https://hive.apache.org/docs/latest/user/serde/).
