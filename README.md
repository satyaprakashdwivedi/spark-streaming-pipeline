# Spark Structured Streaming Pipelines

Build Spark Structured Streaming jobs from a **config file instead of code**. You describe a pipeline as a list of
steps (a source, any number of transforms, one or more sinks) in HOCON or JSON. The app wires the steps together,
starts the streaming queries, and handles checkpoints, triggers and shutdown.

```
source.kafka ──► transform.json ──► transform.lookup ──► transform.sql ──► sink.kafka
                                         │
                                         └──────────────────────────────► sink.console
```

- **Built-in components:**
  - **Sources:** Kafka, files (S3/ADLS/GCS/HDFS/local) and rate.
  - **Transforms:** JSON parsing, SQL, filter, select, derived columns, watermark, dedup, and lookup joins.
  - **Sinks:** Kafka, files (Parquet/JSON/CSV/ORC/Delta), JDBC, console and memory.
- **Branching:** any step can read any earlier step, so one source can feed several outputs.
- **Exactly-once file and Kafka output through checkpoints.** Restarts resume where they stopped.
- **Secrets from the environment** (`password = ${?DB_PASSWORD}`), never in the file.
- **Validation before Spark starts:** unknown types, missing options and broken wiring fail fast, with an error that
  names the step.
- **Pluggable:** add your own sources, transforms or sinks without touching this code.
- **Versions:** Spark 3.5.3, Scala 2.12, Java 11 or 17.

---

## Quick start (no infrastructure needed)

Requirements: **JDK 11 or 17** and **Maven 3.8+**.

```bash
git clone https://github.com/satyaprakashdwivedi/spark-streaming-pipeline.git
cd spark-streaming-pipeline
mvn -Plocal compile exec:exec -Dpipeline=examples/rate-to-console.conf
```

You'll see a micro-batch printed every 5 seconds. Stop it with Ctrl+C.

```
-------------------------------------------
Batch: 1
-------------------------------------------
+-----------------------+-----+------+
|timestamp              |value|parity|
+-----------------------+-----+------+
|2026-11-17 18:58:30.146|0    |even  |
|2026-11-17 18:58:30.346|1    |odd   |
```

Check a config without starting anything:

```bash
mvn -Plocal compile exec:exec -Dpipeline="examples/kafka-json-enrich-to-kafka.conf --validate"
# Pipeline 'orders-enrichment' is valid:
#   orders_raw [source.kafka]
#   orders [transform.json] <- orders_raw
#   enriched [transform.lookup] <- orders
#   ...
```

The `local` profile bundles Spark so the app runs straight from Maven or your IDE. Without the profile, Spark is
`provided`, as `spark-submit` expects.

---

## Run on a cluster

```bash
mvn clean package            # -> target/spark-streaming-pipeline-1.0.0-all.jar (about 6 MB)

spark-submit \
  --class io.github.satyaprakashdwivedi.streaming.StreamingApp \
  --master yarn --deploy-mode cluster \
  --files orders.conf \
  target/spark-streaming-pipeline-1.0.0-all.jar orders.conf
```

- **What the jar contains:** this app, the Kafka connector, the Kafka client and Typesafe Config (relocated so it
  can't clash with other versions). It doesn't contain Spark itself, which the cluster provides.
- **Kubernetes:** use `--master k8s://...` with the jar and config baked into the image or mounted.
- **Databricks:** create a JAR job with this main class and pass the config path, e.g. `/dbfs/...` or a workspace
  file.
- **Extra drivers:** add JDBC drivers with `--packages`, e.g. `org.postgresql:postgresql:42.7.4`.

The app keeps running until every query stops or the process is killed. It stops its queries cleanly on
SIGTERM/Ctrl+C.

---

## Writing a pipeline

```hocon
app {
  name     = orders-enrichment          # Spark app name
  master   = "local[2]"                 # optional; spark-submit sets it on a cluster (default local[*] otherwise)
  logLevel = WARN
  sparkConf { "spark.sql.shuffle.partitions" = 8 }
}

steps = [
  { name = orders_raw, type = source.kafka,  options { bootstrapServers = "localhost:9092", subscribe = orders } }
  { name = orders,     type = transform.json, options { schema = "order_id STRING, amount DOUBLE", dropMalformed = true } }
  { name = big,        type = transform.filter, options { condition = "amount >= 1000" } }
  { name = out,        type = sink.console }
]
```

**Rules**
- **Order:** steps run top to bottom. Each step reads the **previous** step, unless it sets `input = <step name>`.
- **First and last:** the first step must be a source. There must be at least one sink.
- **Views:** every source and transform is also a SQL temp view under its name, so a `transform.sql` query can use
  `FROM orders` or join two earlier steps.
- **Names:** must be unique. If you leave `name` out, the step is called `step1`, `step2`, and so on.
- **Environment values:** `key = ${?ENV_VAR}` reads an environment variable. Write `key = "default"` first to give
  it a fallback.
- **Overrides:** a system property such as `-Dapp.logLevel=INFO` overrides the file.

---

## Component reference

### Sources

| Type | What it does | Key options |
|---|---|---|
| `source.kafka` | Reads topics. Output: `key`, `value` (strings), `topic`, `partition`, `offset`, `timestamp` | `bootstrapServers`; one of `subscribe` / `subscribePattern` / `assign`; `startingOffsets` (`latest`); `maxOffsetsPerTrigger`; `failOnDataLoss` (`false`); `castToString` (`true`); `kafka { ... }` passes any Kafka client setting through (for example security) |
| `source.file` | Picks up new files in a folder | `path`; `format` (`json`); `schema` (Spark DDL, required for json/csv); `maxFilesPerTrigger`; `readerOptions { header = true, ... }` |
| `source.rate` | Generates `timestamp`, `value` rows, for demos and tests | `rowsPerSecond` (5); `numPartitions` (1) |

Kafka with SASL/SSL:

```hocon
{ type = source.kafka, options {
    bootstrapServers = "broker:9093", subscribe = orders
    kafka {
      "security.protocol" = SASL_SSL
      "sasl.mechanism"    = PLAIN
      "sasl.jaas.config"  = ${KAFKA_JAAS}      # from the environment
    } } }
```

### Transforms

| Type | What it does | Key options |
|---|---|---|
| `transform.json` | Parses a JSON string column into columns | `schema` (DDL, required); `column` (`value`); `keep` (columns to carry over; defaults to `key`, `topic`, `partition`, `offset`, `timestamp` where present); `dropMalformed` (`false`); `jsonOptions { ... }` |
| `transform.sql` | Runs Spark SQL. The input is a view named after the input step (or `view`) | `query`; `view` |
| `transform.filter` | Keeps the rows that match | `condition`, e.g. `"amount > 100 AND country = 'IN'"` |
| `transform.select` | Projects or derives columns | `expressions = ["id", "amount * 2 AS double_amount"]` |
| `transform.withColumns` | Adds or replaces columns | `columns { gross = "amount * 1.18", ingested_at = "current_timestamp()" }` |
| `transform.watermark` | Bounds lateness for stateful steps | `column`; `delay` (`"10 minutes"`) |
| `transform.dropDuplicates` | Removes repeated events | `columns = [event_id, event_time]`; `withinWatermark` (`false`) |
| `transform.lookup` | Joins reference data from any batch format (CSV, Parquet, JDBC, Delta, ...) | `format`; `path`; `readerOptions`; `schema`; `on = [key]` or `condition`; `joinType` (`left`); `select`; `broadcast` (`true`); `cache` (`false`) |

**Watermark before state.** Aggregations, `dropDuplicates` and stream-stream joins keep state. Put a
`transform.watermark` step first, or the state grows forever.

**Lookup freshness.** The lookup side is re-evaluated for every micro-batch:
- **JDBC tables** always reflect their latest rows.
- **Files:** the set of files is fixed when the query starts.
- **`cache = true`:** reads the lookup once and keeps it in memory.

```hocon
{ type = transform.lookup, options {
    format = jdbc
    readerOptions { url = "jdbc:postgresql://db:5432/ref", dbtable = "public.customers",
                    user = ${?DB_USER}, password = ${?DB_PASSWORD} }
    on = [customer_id], select = [customer_id, segment] } }
```

### Sinks

Every sink accepts these options:

| Option | Meaning |
|---|---|
| `checkpointLocation` | **Required** except for console and memory. Use a durable path (HDFS/S3/ADLS), one per query, and never share or delete it. It is what makes restarts exactly-once. |
| `trigger` | `{ processingTime = "30 seconds" }`, or `{ availableNow = true }` to process what's there and stop (good for scheduled jobs). Default: as fast as possible. |
| `outputMode` | `append` (default), `update` or `complete`. |
| `queryName` | Defaults to the step name. Shown in the Spark UI. |
| `writerOptions { ... }` | Passed straight to the writer. |

| Type | What it does | Key options |
|---|---|---|
| `sink.kafka` | Publishes rows. Uses an existing `value` column (and `key`) if present; otherwise the whole row is sent as JSON | `bootstrapServers`; `topic`; `keyColumn`; `kafka { acks = all, ... }` |
| `sink.file` | Writes Parquet, JSON, CSV or ORC, or Delta with the Delta jar. Exactly-once | `path`; `format` (`parquet`); `partitionBy = [col]` |
| `sink.jdbc` | Appends each micro-batch to a table | `url`; `table`; `user`; `password`; `driver`; `jdbcOptions { batchsize = 5000 }` |
| `sink.console` | Prints batches, for development | `numRows` (20); `truncate` (`false`) |
| `sink.memory` | In-memory table named after the query, for tests | none |

**Delivery guarantees**
- **`sink.file`:** exactly-once.
- **`sink.kafka`:** at-least-once. Consumers should de-duplicate by key, or read with
  `isolation.level=read_committed` on idempotent pipelines.
- **`sink.jdbc`:** at-least-once. A retried batch is written again, so give the table a unique key and upsert.

---

## Examples

| File | What it shows | Needs |
|---|---|---|
| [`examples/rate-to-console.conf`](examples/rate-to-console.conf) | The smallest pipeline | nothing |
| [`examples/kafka-json-enrich-to-kafka.conf`](examples/kafka-json-enrich-to-kafka.conf) | Kafka JSON → parse → enrich from CSV → filter with SQL → Kafka, plus a debug branch | Kafka |
| [`examples/files-to-parquet.conf`](examples/files-to-parquet.conf) | Landing folder → watermark → dedup → partitioned Parquet | a folder |
| [`examples/kafka-to-postgres.conf`](examples/kafka-to-postgres.conf) | Kafka → per-minute windowed totals → PostgreSQL | Kafka, PostgreSQL |

### Kafka end-to-end, locally

```bash
docker compose up -d                                   # Kafka on :9092, PostgreSQL on :5432

# send sample orders (one is deliberately malformed)
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders < examples/data/orders.jsonl

mvn -Plocal compile exec:exec -Dpipeline=examples/kafka-json-enrich-to-kafka.conf

# in another terminal: read the enriched high-value orders
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders-high-value --from-beginning
```

What happens to the sample data:
- `o-1001` and `o-1003` come out enriched with segment and city.
- `o-1004` comes out with no segment, because its customer isn't in the CSV and the join is a left join.
- `o-1002` is below 1,000, so it's filtered out.
- The malformed line is dropped.

---

## Adding your own component

Implement `Source`, `Transform` or `Sink`, then register it in one of two ways.

**From code**, when embedding the runner:

```java
ComponentRegistry registry = ComponentRegistry.withDefaults()
    .transform("transform.mask", (spark, in, step) ->
        in.withColumn(step.string("column"), functions.sha2(functions.col(step.string("column")), 256)));
RunningPipeline running = new PipelineRunner(registry).start(spark, PipelineConfig.fromFile("pipeline.conf"));
```

**As a plug-in jar**, with no changes to this repo:

```java
public class MyComponents implements ComponentProvider {
    public void register(ComponentRegistry r) {
        r.sink("sink.elasticsearch", new MyElasticSink());
    }
}
```

1. List the class in `META-INF/services/io.github.satyaprakashdwivedi.streaming.api.ComponentProvider`.
2. Add the jar with `--jars`.
3. Use `type = sink.elasticsearch` in any config.

**Inside a component**, read options with `step.string("key")`, `step.string("key", default)`, `step.integer(...)`,
`step.bool(...)`, `step.list(...)` and `step.map(...)`. Raise problems with `throw step.error("...")`, so messages
always name the step.

---

## Project layout

```
src/main/java/io/github/satyaprakashdwivedi/streaming/
  StreamingApp.java            entry point (spark-submit main class), --validate
  config/                      PipelineConfig, StepConfig (HOCON/JSON loading, typed options)
  api/                         Source, Transform, Sink, ComponentProvider
  registry/ComponentRegistry   type name -> implementation, built-ins and plug-ins
  runtime/                     PipelineRunner (validate, wire, start), RunningPipeline
  components/sources|transforms|sinks/   the built-in components
src/test/java/...              config/validation tests and end-to-end streaming tests (local Spark)
examples/                      ready-to-run pipelines and sample data
docker-compose.yml             local Kafka and PostgreSQL
```

## Tests

```bash
mvn test
```

- **What runs:** 21 tests. Config parsing and validation, plus real streaming queries on a local Spark session.
- **What they cover:**
  - JSON parsing, including malformed rows;
  - filters, derived columns and SQL;
  - lookup joins;
  - branching to two sinks;
  - the Parquet sink resuming from its checkpoint;
  - dedup with a watermark;
  - custom components.
- **No broker needed:** the Kafka source is checked for its wiring and output columns.
- **CI:** the GitHub Actions workflow runs the suite on Java 11 and 17.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `InaccessibleObjectException` / `IllegalAccessError` on Java 17 | Run through `mvn -Plocal ... exec:exec`, which adds the `--add-opens` flags, or pass the flags in the pom's `java17.opens` property to `java` yourself. |
| `Failed to find data source: kafka` | Use the `-all.jar`, which contains the Kafka connector, or add `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`. |
| Query restarts reprocess everything | `checkpointLocation` changed or was deleted. Keep one stable path per query. |
| State keeps growing | Add `transform.watermark` before aggregations or `dropDuplicates`. |
| `No suitable driver` (JDBC) | Add the driver jar with `--packages` or `--jars`, and set `driver`. |
| Parsed columns are all null | The `schema` in `transform.json` doesn't match the payload. Check field names and types; timestamps need ISO-8601 or `jsonOptions { timestampFormat = ... }`. |
