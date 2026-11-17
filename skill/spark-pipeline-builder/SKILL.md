---
name: spark-pipeline-builder
description: Build a Spark Structured Streaming pipeline config from just a source, transformation rules and a sink. Use it when the user wants to create, configure or change a streaming pipeline (Kafka, files, S3/ADLS, JDBC, Parquet), describes one in plain words, or hands over a filled pipeline template. Covers data quality rules or Great Expectations suites, dbt models as transformation rules, and output for the Java (spark-streaming-pipeline) or PySpark (spark-streaming-pipeline-py) engine.
---

# Spark pipeline builder

Turn "source + transformation rules + sink" into a validated pipeline config for this repo (Java engine) or for
the PySpark repo. The user can describe the pipeline in plain words, or fill in `templates/pipeline.yaml`.

## 1. Collect the inputs

Ask only for what's missing. Put all the questions in one message, and offer sensible defaults.

| Need | Ask for |
|---|---|
| Source | Kafka (servers, topic, JSON schema of the value), a folder of files (path, format, schema), or rate for a demo |
| Rules | Filters, derived columns, SQL, lookups (reference file or table and join keys), dbt model (project and model name), dedup keys |
| Event time | The timestamp column and allowed lateness. **Required** if they dedupe, aggregate or use windows |
| Quality | Row checks (not null, ranges, allowed values, regex), or a Great Expectations suite; what happens to bad rows (quarantine/drop/flag/fail) |
| Sink | Kafka topic, file path and format, JDBC table, or console; **checkpoint path** (durable, one per sink); trigger |
| Engine | Java (default; this repo) or PySpark (when they need Python libraries, real Great Expectations, ML or pandas UDFs) |

Schema format is Spark DDL: `order_id STRING, amount DOUBLE, created_at TIMESTAMP`. If the user pastes a sample
event, work out the DDL from it and show it to them for confirmation.

Never put secrets in the template. Use `${ENV_VAR}` values, e.g. `password: ${DB_PASSWORD}`.

## 2. Fill in the template

Copy `templates/pipeline.yaml` to the user's project (e.g. `pipelines/<name>.yaml`) and fill it in. Delete the
sections they don't need. The comments in the template list every allowed value.

## 3. Render and validate

```bash
pip install pyyaml   # once
python3 <skill-dir>/scripts/render_pipeline.py pipelines/<name>.yaml --engine java --out pipelines/<name>.conf
mvn -q -Plocal compile exec:exec -Dpipeline="pipelines/<name>.conf --validate"
```

- For the PySpark repo, use `--engine pyspark --out pipelines/<name>.yaml` and validate with
  `python -m spark_pipeline pipelines/<name>.yaml --validate`.
- Fix any `template error:` or `Invalid pipeline:` message, then render again. Errors name the template item or
  step involved.

## 4. Optionally, a quick local run

- **Kafka or files available:** run without `--validate` for 30–60 seconds, then show the console output or files
  written.
- **Otherwise:** say what you could and couldn't verify.

## 5. Hand over

Give the user:
1. **Plan:** the step list from `--validate`.
2. **Config file:** the generated file and where it is.
3. **Run command:**
   - **Java:** `spark-submit --class io.github.satyaprakashdwivedi.streaming.StreamingApp target/spark-streaming-pipeline-1.0.0-all.jar <name>.conf`
   - **PySpark:** `spark-submit --py-files dist/spark_pipeline.zip -m spark_pipeline <name>.yaml`, or the
     README's command for their platform.
4. **Things to check:**
   - the checkpoint paths are durable;
   - an event-time column exists if they dedupe or aggregate;
   - where quarantined rows go;
   - the JDBC driver is on the classpath.

## Notes

- **Transforms run in order.** In a `sql:` item, `input` means the result of the item before it.
- **dbt models:** read the data flowing into the step with `{{ source('stream', 'input') }}`. Declare the
  source in `sources.yml` with `schema: stream` and a table named `input`. dbt compiles the model, and the stream
  runs the compiled SQL. Only streaming-compatible SQL works: no ORDER BY or LIMIT without aggregation, and windows
  need event time.
- **Great Expectations suites:** row-level expectations become rules. Table-level ones (row counts, means,
  uniqueness) are skipped with a warning. In the PySpark engine they can run per micro-batch with real Great
  Expectations instead.
- **Unsupported needs** (other sources, sinks or custom logic): the Java engine accepts plug-in components
  (README, "Adding your own component"), and the PySpark engine accepts registered Python functions.
