package io.github.satyaprakashdwivedi.streaming.components.transforms;

import io.github.satyaprakashdwivedi.streaming.api.Transform;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameReader;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.expr;

/**
 * {@code transform.lookup}: enriches the stream with reference data (a stream-static join).
 * <pre>
 * options { format = csv, path = "/data/ref/customers.csv", readerOptions { header = true },
 *           schema = "customer_id STRING, segment STRING",   # optional
 *           on = [customer_id], joinType = left, broadcast = true,
 *           select = ["customer_id", "segment"] }            # columns to keep from the lookup side
 * </pre>
 * Any batch format works, including JDBC: {@code format = jdbc, readerOptions { url = ..., dbtable = ... }}.
 * The lookup side is re-evaluated for every micro-batch: a JDBC table reflects its latest rows, while for files
 * the file list is fixed when the query starts. Set {@code cache = true} to read it once and keep it in memory.
 * Use {@code condition} instead of {@code on} for non-equi joins (the lookup side is aliased {@code lookup}).
 */
public class LookupJoinTransform implements Transform {
    @Override
    public Dataset<Row> apply(SparkSession spark, Dataset<Row> input, StepConfig step) {
        DataFrameReader reader = spark.read().format(step.string("format", "parquet"));
        if (step.has("schema")) {
            reader = reader.schema(step.string("schema"));
        }
        for (Map.Entry<String, String> e : step.map("readerOptions").entrySet()) {
            reader = reader.option(e.getKey(), e.getValue());
        }
        Dataset<Row> lookup = step.has("path") ? reader.load(step.string("path")) : reader.load();
        List<String> select = step.list("select");
        if (!select.isEmpty()) {
            lookup = lookup.selectExpr(select.toArray(new String[0]));
        }
        if (step.bool("cache", false)) {
            lookup = lookup.cache();
        }
        if (step.bool("broadcast", true)) {
            lookup = broadcast(lookup);
        }
        String joinType = step.string("joinType", "left");
        List<String> on = step.list("on");
        if (!on.isEmpty()) {
            return input.join(lookup, JavaConverters.asScalaBuffer(on).toSeq(), joinType);
        }
        if (step.has("condition")) {
            Column condition = expr(step.string("condition"));
            return input.as("stream").join(lookup.as("lookup"), condition, joinType);
        }
        throw step.error("set 'on' (list of key columns present on both sides) or 'condition'");
    }
}
