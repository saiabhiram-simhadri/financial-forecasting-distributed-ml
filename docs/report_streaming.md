# Real-Time Transaction Analysis

Status: implementation ready; full-stack execution and screenshots pending.

## 01. Implement a Spark Streaming application

scripts/replay.py atomically publishes bounded chunks of bank.csv as JSON with unique event_id and simulated UTC event_time. Run spark_job.py stream with the saved pipeline. This replays marketing/contact records, not actual payments.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 02. Use Spark Streaming

job_means console query reports cumulative event counts, average account balance, and average contact duration by job. State is bounded by job categories; duration is not actual transaction duration.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 03. Develop a machine learning model

Historical training is provided by spark_job.py train. Use the resulting pipeline directory; pre-call features exclude duration by default.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 04. Integrate the trained model

PipelineModel.load transforms every valid incoming event. predictions/ contains append-only Parquet with event_id, timestamp, source fields, prediction, and probability. Labels are optional and are never a feature.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 05. Implement window operations

window_10s and window_60s console queries use tumbling event-time windows, event counts, and mean balances, with a 20-second watermark and update output mode.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 06. Write a brief analysis

Capture several successive windows and query_progress.jsonl. Report observed count/balance changes, compare 10s versus 60s smoothing, and note that timestamps/arrival ordering are synthetic. No factual trends are prefilled.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 07. Modify your Spark Streaming

Replay emits 8-second out-of-order and 90-second-late events after the first two chunks. withWatermark(event_time,20 seconds) bounds window state. The watermark follows observed event-time progress, not wall time; >20s-late records may be dropped by windows but remain in raw predictions/all-time job aggregates. Inspect eventTime.watermark and stateOperators.numRowsDroppedByWatermark in progress logs; actual drops depend on microbatch ordering.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.
