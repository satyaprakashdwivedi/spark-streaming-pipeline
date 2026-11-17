package io.github.satyaprakashdwivedi.streaming;

import io.github.satyaprakashdwivedi.streaming.components.dbt.DbtModelTransformTestAccess;
import io.github.satyaprakashdwivedi.streaming.config.PipelineConfig;
import io.github.satyaprakashdwivedi.streaming.runtime.PipelineRunner;
import io.github.satyaprakashdwivedi.streaming.runtime.RunningPipeline;
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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QualityAndDbtTest {
    private static SparkSession spark;
    private RunningPipeline running;

    @TempDir
    Path tmp;

    private static final String[] ORDERS = {
        "{\"order_id\":\"o1\",\"customer_id\":\"c-001\",\"amount\":2500.0,\"currency\":\"INR\"}",
        "{\"order_id\":null,\"customer_id\":\"c-002\",\"amount\":10.0,\"currency\":\"INR\"}",
        "{\"order_id\":\"o3\",\"customer_id\":\"c-003\",\"amount\":-5.0,\"currency\":\"GBP\"}",
        "{\"order_id\":\"o4\",\"customer_id\":\"BAD\",\"amount\":99.0,\"currency\":\"usd\"}"
    };

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

    private String ordersSource() throws IOException {
        Path dir = Files.createDirectories(tmp.resolve("orders"));
        Files.write(dir.resolve("o.json"), (String.join("\n", ORDERS) + "\n").getBytes(StandardCharsets.UTF_8));
        return "{ name = orders, type = source.file, options { format = json, path = \"" + dir.toString().replace("\\", "/")
                + "\", schema = \"order_id STRING, customer_id STRING, amount DOUBLE, currency STRING\" } }";
    }

    private void run(String steps) {
        running = new PipelineRunner().start(spark, PipelineConfig.fromString("steps = [" + steps + "]"));
        running.processAllAvailable();
    }

    private static final String RULES = "rules = ["
            + "{ column = order_id, check = not_null },"
            + "{ column = amount, check = between, min = 0 },"
            + "{ column = currency, check = in_set, values = [INR, USD] },"
            + "{ name = customer_format, column = customer_id, check = regex, pattern = \"^c-[0-9]{3}$\" } ]";

    @Test
    void quarantineSplitsGoodAndBadRowsWithReasons() throws IOException {
        run(ordersSource() + ","
                + "{ name = checked, type = transform.quality, options { " + RULES + " } },"
                + "{ name = good_orders, type = sink.memory },"
                + "{ name = bad_orders, type = sink.memory, input = checked_quarantine }");
        List<Row> good = spark.sql("SELECT * FROM good_orders").collectAsList();
        assertEquals(1, good.size());
        assertEquals("o1", good.get(0).getAs("order_id"));
        assertTrue(!List.of(good.get(0).schema().fieldNames()).contains("_dq_failed_rules"));

        Map<String, String> reasons = new java.util.HashMap<>();
        for (Row r : spark.sql("SELECT customer_id, concat_ws('|', _dq_failed_rules) AS why FROM bad_orders").collectAsList()) {
            reasons.put(r.getString(0), r.getString(1));
        }
        assertEquals("order_id_not_null", reasons.get("c-002"));
        assertEquals("amount_between|currency_in_set", reasons.get("c-003"));
        assertEquals("currency_in_set|customer_format", reasons.get("BAD"));
    }

    @Test
    void flagKeepsEveryRowAndDropRemovesBadOnes() throws IOException {
        run(ordersSource() + ","
                + "{ name = flagged, type = transform.quality, options { onFailure = flag, " + RULES + " } },"
                + "{ name = flagged_out, type = sink.memory },"
                + "{ name = dropped, type = transform.quality, input = orders, options { onFailure = drop, " + RULES + " } },"
                + "{ name = dropped_out, type = sink.memory }");
        assertEquals(4, spark.sql("SELECT * FROM flagged_out").count());
        assertEquals(1, spark.sql("SELECT * FROM flagged_out WHERE _dq_passed").count());
        assertEquals(1, spark.sql("SELECT * FROM dropped_out").count());
    }

    @Test
    void failModeStopsTheQuery() throws IOException {
        running = new PipelineRunner().start(spark, PipelineConfig.fromString("steps = [" + ordersSource() + ","
                + "{ name = strict, type = transform.quality, options { onFailure = fail, " + RULES + " } },"
                + "{ name = strict_out, type = sink.memory } ]"));
        Exception e = assertThrows(Exception.class, () -> running.processAllAvailable());
        assertTrue(String.valueOf(e.getMessage()).contains("Data quality failure in step 'strict'"), e.getMessage());
    }

    @Test
    void importsGreatExpectationsSuite() throws IOException {
        run(ordersSource() + ","
                + "{ name = gx, type = transform.quality, options { expectationSuite = \"examples/quality/orders_suite.json\" } },"
                + "{ name = gx_good, type = sink.memory },"
                + "{ name = gx_bad, type = sink.memory, input = gx_quarantine }");
        assertEquals(1, spark.sql("SELECT * FROM gx_good").count());
        List<Row> bad = spark.sql("SELECT concat_ws('|', _dq_failed_rules) FROM gx_bad WHERE customer_id = 'BAD'").collectAsList();
        assertEquals("be_in_set_currency|match_regex_customer_id", bad.get(0).getString(0));
    }

    @Test
    void strictSuiteImportRejectsTableLevelExpectations() throws IOException {
        PipelineException e = assertThrows(PipelineException.class, () -> run(ordersSource() + ","
                + "{ type = transform.quality, options { expectationSuite = \"examples/quality/orders_suite.json\","
                + "  strictSuiteImport = true } }, { name = s, type = sink.memory }"));
        assertTrue(e.getMessage().contains("expect_table_row_count_to_be_between"));
    }

    @Test
    void validationKnowsAboutQuarantineOutput() {
        List<String> plan = new PipelineRunner().validate(PipelineConfig.fromString("steps = ["
                + "{ name = src, type = source.rate },"
                + "{ name = checked, type = transform.quality, options { rules = [ { column = value, check = not_null } ] } },"
                + "{ type = sink.console, input = checked_quarantine } ]"));
        assertTrue(plan.stream().anyMatch(line -> line.startsWith("checked_quarantine")));
    }

    // ------------------------------------------------------------------ dbt

    private Path compiledModel(String project) throws IOException {
        Path model = Files.createDirectories(tmp.resolve(project).resolve("target/compiled/orders_rules/models"))
                .resolve("clean_orders.sql");
        Files.writeString(model, "select order_id, upper(currency) as currency, round(amount * (1 + 0.18), 2) as gross_amount\n"
                + "from `stream`.`orders`\nwhere amount > 0;\n");
        return model;
    }

    @Test
    void dbtModelRunsPrecompiledSql() throws IOException {
        compiledModel("dbtproj");
        run(ordersSource() + ","
                + "{ type = transform.dbt, options { projectDir = \"" + tmp.resolve("dbtproj") + "\", model = clean_orders,"
                + "  compile = false } },"
                + "{ name = dbt_out, type = sink.memory }");
        List<Row> rows = spark.sql("SELECT * FROM dbt_out ORDER BY order_id").collectAsList();
        assertEquals(3, rows.size()); // amount -5 filtered out by the model
        assertEquals(2950.0, (double) rows.get(1).getAs("gross_amount"));
        assertEquals("USD", rows.get(2).getAs("currency"));
    }

    @Test
    void dbtCompileIsInvokedWithModelAndVars() throws IOException {
        // stand-in for the dbt CLI: records its arguments and "compiles" the model
        Path project = Files.createDirectories(tmp.resolve("dbtcli"));
        Path fakeDbt = project.resolve("fake-dbt.sh");
        Files.writeString(fakeDbt, "#!/bin/sh\necho \"$@\" > args.txt\nmkdir -p target/compiled/p/models\n"
                + "printf 'select order_id from stream.orders' > target/compiled/p/models/clean_orders.sql\n");
        fakeDbt.toFile().setExecutable(true);
        run(ordersSource() + ","
                + "{ type = transform.dbt, options { projectDir = \"" + project + "\", model = clean_orders,"
                + "  dbtExecutable = \"" + fakeDbt + "\", target = ci, vars { min_amount = 5 } } },"
                + "{ name = dbt_cli_out, type = sink.memory }");
        assertEquals("compile --select clean_orders --target ci --vars {min_amount: '5'}",
                Files.readString(project.resolve("args.txt")).trim());
        assertEquals(4, spark.sql("SELECT * FROM dbt_cli_out").count());
    }

    @Test
    void dbtCompileFailureIsReported() throws IOException {
        Path project = Files.createDirectories(tmp.resolve("dbtbad"));
        Path fakeDbt = project.resolve("fail.sh");
        Files.writeString(fakeDbt, "#!/bin/sh\necho 'Compilation Error in model clean_orders'\nexit 2\n");
        fakeDbt.toFile().setExecutable(true);
        PipelineException e = assertThrows(PipelineException.class, () -> run(ordersSource() + ","
                + "{ type = transform.dbt, options { projectDir = \"" + project + "\", model = clean_orders,"
                + "  dbtExecutable = \"" + fakeDbt + "\" } }, { name = x, type = sink.memory }"));
        assertTrue(e.getMessage().contains("Compilation Error in model clean_orders"));
    }

    @Test
    void streamSourceReferencesAreRewritten() {
        assertEquals("select * from orders o join customers c on o.id = c.id",
                DbtModelTransformTestAccess.rewrite("select * from `spark_catalog`.`stream`.`orders` o "
                        + "join analytics.customers c on o.id = c.id;", "stream", Map.of("analytics.customers", "customers")));
        assertEquals("select a from orders", DbtModelTransformTestAccess.rewrite("select a from stream.orders", "stream", Map.of()));
    }
}
