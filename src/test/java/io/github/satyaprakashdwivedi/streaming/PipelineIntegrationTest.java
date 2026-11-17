package io.github.satyaprakashdwivedi.streaming;

import io.github.satyaprakashdwivedi.streaming.config.PipelineConfig;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import io.github.satyaprakashdwivedi.streaming.registry.ComponentRegistry;
import io.github.satyaprakashdwivedi.streaming.runtime.PipelineRunner;
import io.github.satyaprakashdwivedi.streaming.runtime.RunningPipeline;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.upper;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end pipelines on a local Spark session, using file sources (no Kafka broker needed). */
class PipelineIntegrationTest {
    private static SparkSession spark;
    private RunningPipeline running;

    @TempDir
    Path tmp;

    @BeforeAll
    static void startSpark() {
        spark = new PipelineRunner().createSession(PipelineConfig.fromString(
                "app { name = tests, master = \"local[2]\", logLevel = ERROR,"
                        + " sparkConf { \"spark.sql.shuffle.partitions\" = 2, \"spark.ui.enabled\" = false } }\n"
                        + "steps = [ { type = source.rate }, { type = sink.console } ]"));
    }

    @AfterAll
    static void stopSpark() {
        spark.stop();
    }

    @AfterEach
    void stopQueries() {
        if (running != null) {
            running.stop();
        }
    }

    private RunningPipeline run(String steps) {
        return run(new PipelineRunner(), steps);
    }

    private RunningPipeline run(PipelineRunner runner, String steps) {
        running = runner.start(spark, PipelineConfig.fromString("steps = [" + steps + "]"));
        running.processAllAvailable();
        return running;
    }

    private Path write(String dir, String file, String... lines) throws IOException {
        Path d = Files.createDirectories(tmp.resolve(dir));
        return Files.write(d.resolve(file), (String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8));
    }

    private String p(String dir) {
        return tmp.resolve(dir).toString().replace("\\", "/");
    }

    private List<Row> table(String name, String orderBy) {
        return spark.sql("SELECT * FROM " + name + " ORDER BY " + orderBy).collectAsList();
    }

    @Test
    void jsonFilesFilterAndDerivedColumns() throws IOException {
        write("orders", "a.json",
                "{\"order_id\":\"o1\",\"amount\":50.0,\"country\":\"IN\"}",
                "{\"order_id\":\"o2\",\"amount\":150.0,\"country\":\"US\"}",
                "{\"order_id\":\"o3\",\"amount\":300.0,\"country\":\"IN\"}");
        run("{ name = orders, type = source.file, options { format = json, path = \"" + p("orders") + "\","
                + "    schema = \"order_id STRING, amount DOUBLE, country STRING\" } },"
                + "{ type = transform.filter, options { condition = \"amount > 100\" } },"
                + "{ type = transform.withColumns, options { columns { gross = \"amount * 2\", cc = \"lower(country)\" } } },"
                + "{ name = big_orders, type = sink.memory }");
        List<Row> rows = table("big_orders", "order_id");
        assertEquals(2, rows.size());
        assertEquals("o2", rows.get(0).getAs("order_id"));
        assertEquals(300.0, (double) rows.get(0).getAs("gross"));
        assertEquals("in", rows.get(1).getAs("cc"));
    }

    @Test
    void kafkaStyleValueParsingDropsMalformed() throws IOException {
        // a text source gives a single 'value' column, exactly like a Kafka value cast to string
        write("raw", "events.txt",
                "{\"event_id\":\"e1\",\"user\":\"u1\",\"clicks\":3}",
                "this is not json",
                "{\"event_id\":\"e2\",\"user\":\"u2\",\"clicks\":7}");
        run("{ name = raw, type = source.file, options { format = text, path = \"" + p("raw") + "\" } },"
                + "{ name = parsed, type = transform.json, options { schema = \"event_id STRING, user STRING, clicks INT\","
                + "    dropMalformed = true } },"
                + "{ name = events, type = sink.memory }");
        List<Row> rows = table("events", "event_id");
        assertEquals(2, rows.size());
        assertEquals(Arrays.asList("event_id", "user", "clicks"), Arrays.asList(rows.get(0).schema().fieldNames()));
        assertEquals(7, (int) rows.get(1).getAs("clicks"));
    }

    @Test
    void malformedRowsKeptAsNullsByDefault() throws IOException {
        write("raw2", "events.txt", "{\"event_id\":\"e1\"}", "garbage");
        run("{ name = raw2, type = source.file, options { format = text, path = \"" + p("raw2") + "\" } },"
                + "{ type = transform.json, options { schema = \"event_id STRING\" } },"
                + "{ name = all_events, type = sink.memory }");
        List<Row> rows = spark.sql("SELECT * FROM all_events").collectAsList();
        assertEquals(2, rows.size());
        assertTrue(rows.stream().anyMatch(r -> r.getAs("event_id") == null));
    }

    @Test
    void lookupJoinEnrichesFromCsv() throws IOException {
        write("tx", "t.json",
                "{\"tx_id\":\"t1\",\"customer_id\":\"c1\",\"amount\":10.0}",
                "{\"tx_id\":\"t2\",\"customer_id\":\"c9\",\"amount\":20.0}");
        write("ref", "customers.csv", "customer_id,segment,city", "c1,gold,Pune", "c2,silver,Delhi");
        run("{ name = tx, type = source.file, options { format = json, path = \"" + p("tx") + "\","
                + "    schema = \"tx_id STRING, customer_id STRING, amount DOUBLE\" } },"
                + "{ type = transform.lookup, options { format = csv, path = \"" + p("ref") + "/customers.csv\","
                + "    readerOptions { header = true }, on = [customer_id], select = [customer_id, segment] } },"
                + "{ name = enriched, type = sink.memory }");
        List<Row> rows = table("enriched", "tx_id");
        assertEquals("gold", rows.get(0).getAs("segment"));
        assertNull(rows.get(1).getAs("segment")); // left join keeps unmatched rows
        assertEquals(Arrays.asList("customer_id", "tx_id", "amount", "segment"),
                Arrays.asList(rows.get(0).schema().fieldNames()));
    }

    @Test
    void sqlStepAndTwoBranchesFromOneSource() throws IOException {
        write("clicks", "c.json",
                "{\"page\":\"home\",\"ms\":120}", "{\"page\":\"cart\",\"ms\":900}", "{\"page\":\"home\",\"ms\":80}");
        run("{ name = clicks, type = source.file, options { format = json, path = \"" + p("clicks") + "\","
                + "    schema = \"page STRING, ms INT\" } },"
                + "{ name = slow, type = transform.sql, options { query = \"SELECT page, ms FROM clicks WHERE ms > 500\" } },"
                + "{ name = slow_out, type = sink.memory },"
                + "{ name = home, type = transform.filter, input = clicks, options { condition = \"page = 'home'\" } },"
                + "{ name = home_out, type = sink.memory }");
        assertEquals(1, spark.sql("SELECT * FROM slow_out").count());
        assertEquals(2, spark.sql("SELECT * FROM home_out").count());
        assertEquals(2, running.queries().size());
    }

    @Test
    void parquetSinkWithCheckpointIsReadableAndResumes() throws Exception {
        write("in", "1.json", "{\"id\":1,\"v\":\"a\"}", "{\"id\":2,\"v\":\"b\"}");
        String steps = "{ name = in, type = source.file, options { format = json, path = \"" + p("in") + "\","
                + "    schema = \"id INT, v STRING\" } },"
                + "{ name = lake, type = sink.file, options { format = parquet, path = \"" + p("out") + "\","
                + "    checkpointLocation = \"" + p("chk") + "\", trigger { availableNow = true } } }";
        running = new PipelineRunner().start(spark, PipelineConfig.fromString("steps = [" + steps + "]"));
        running.awaitTermination();
        assertEquals(2, spark.read().parquet(p("out")).count());

        // restart with one new file: the checkpoint means only the new rows are written
        write("in", "2.json", "{\"id\":3,\"v\":\"c\"}");
        running = new PipelineRunner().start(spark, PipelineConfig.fromString("steps = [" + steps + "]"));
        running.awaitTermination();
        List<Integer> ids = spark.read().parquet(p("out")).select("id").collectAsList().stream()
                .map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
        assertEquals(Arrays.asList(1, 2, 3), ids);
    }

    @Test
    void fileSinkRequiresCheckpoint() throws IOException {
        write("in3", "1.json", "{\"id\":1}");
        PipelineException e = assertThrows(PipelineException.class, () -> run(
                "{ type = source.file, options { format = json, path = \"" + p("in3") + "\", schema = \"id INT\" } },"
                        + "{ name = nochk, type = sink.file, options { path = \"" + p("out3") + "\" } }"));
        assertTrue(e.getMessage().contains("checkpointLocation"));
    }

    @Test
    void dropDuplicatesWithWatermark() throws IOException {
        write("dups", "d.json",
                "{\"id\":\"a\",\"ts\":\"2026-01-01 10:00:00\"}",
                "{\"id\":\"a\",\"ts\":\"2026-01-01 10:00:00\"}",
                "{\"id\":\"b\",\"ts\":\"2026-01-01 10:01:00\"}");
        run("{ name = dups, type = source.file, options { format = json, path = \"" + p("dups") + "\","
                + "    schema = \"id STRING, ts TIMESTAMP\" } },"
                + "{ type = transform.watermark, options { column = ts, delay = \"10 minutes\" } },"
                + "{ type = transform.dropDuplicates, options { columns = [id, ts] } },"
                + "{ name = unique_ids, type = sink.memory }");
        assertEquals(2, spark.sql("SELECT * FROM unique_ids").count());
    }

    @Test
    void rateSourceAndSelect() {
        run("{ type = source.rate, options { rowsPerSecond = 50 } },"
                + "{ type = transform.select, options { expressions = [\"value\", \"value % 2 = 0 AS even\"] } },"
                + "{ name = rate_out, type = sink.memory }");
        running.processAllAvailable();
        Dataset<Row> out = spark.sql("SELECT * FROM rate_out");
        assertEquals(Arrays.asList("value", "even"), Arrays.asList(out.columns()));
    }

    @Test
    void kafkaSourceBuildsWithoutConnecting() {
        StepConfig step = new StepConfig("orders", "source.kafka", null, ConfigFactory.parseString(
                "bootstrapServers = \"localhost:9092\", subscribe = orders, kafka { \"security.protocol\" = PLAINTEXT }"));
        Dataset<Row> df = ComponentRegistry.withDefaults().source("source.kafka").read(spark, step);
        assertTrue(df.isStreaming());
        assertEquals(Arrays.asList("key", "value", "topic", "partition", "offset", "timestamp"), Arrays.asList(df.columns()));

        StepConfig noTopic = new StepConfig("orders", "source.kafka", null,
                ConfigFactory.parseString("bootstrapServers = \"localhost:9092\""));
        PipelineException e = assertThrows(PipelineException.class,
                () -> ComponentRegistry.withDefaults().source("source.kafka").read(spark, noTopic));
        assertTrue(e.getMessage().contains("subscribe"));
    }

    @Test
    void customComponentsCanBeRegistered() throws IOException {
        write("names", "n.json", "{\"name\":\"asha\"}");
        ComponentRegistry registry = ComponentRegistry.withDefaults()
                .transform("transform.upper", (s, in, step) -> in.withColumn(step.string("column"), upper(col(step.string("column")))));
        run(new PipelineRunner(registry),
                "{ name = names, type = source.file, options { format = json, path = \"" + p("names") + "\", schema = \"name STRING\" } },"
                        + "{ type = transform.upper, options { column = name } },"
                        + "{ name = upper_out, type = sink.memory }");
        assertEquals("ASHA", spark.sql("SELECT name FROM upper_out").first().getString(0));
    }
}
