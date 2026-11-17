package io.github.satyaprakashdwivedi.streaming.components.sinks;

import io.github.satyaprakashdwivedi.streaming.api.Sink;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;

/**
 * {@code sink.kafka}: publishes each row to a topic.
 * If the input already has a {@code value} column it is sent as is; otherwise the whole row becomes a JSON value.
 * <pre>
 * options { bootstrapServers = "localhost:9092", topic = "orders-enriched", keyColumn = order_id,
 *           checkpointLocation = "/chk/orders-enriched", kafka { acks = all } }
 * </pre>
 */
public class KafkaSink implements Sink {
    @Override
    public StreamingQuery start(SparkSession spark, Dataset<Row> input, StepConfig step) throws TimeoutException {
        List<String> cols = Arrays.asList(input.columns());
        Dataset<Row> out;
        if (cols.contains("value")) {
            out = cols.contains("key")
                    ? input.select(col("key").cast("string"), col("value").cast("string"))
                    : input.select(col("value").cast("string"));
        } else {
            out = input.select(to_json(struct(col("*"))).as("value"));
            if (step.has("keyColumn")) {
                String keyCol = step.string("keyColumn");
                if (!cols.contains(keyCol)) {
                    throw step.error("keyColumn '" + keyCol + "' is not in the input columns " + cols);
                }
                out = input.select(col(keyCol).cast("string").as("key"), to_json(struct(col("*"))).as("value"));
            }
        }
        DataStreamWriter<Row> writer = SinkSupport.common(out.writeStream().format("kafka"), step, true)
                .option("kafka.bootstrap.servers", step.string("bootstrapServers"))
                .option("topic", step.string("topic"));
        for (Map.Entry<String, String> e : step.map("kafka").entrySet()) {
            writer = writer.option("kafka." + e.getKey(), e.getValue());
        }
        return writer.start();
    }
}
