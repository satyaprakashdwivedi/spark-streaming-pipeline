package io.github.satyaprakashdwivedi.streaming.components.sources;

import io.github.satyaprakashdwivedi.streaming.api.Source;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;

import java.util.Map;

/**
 * {@code source.kafka}: reads a Kafka topic. Output columns: key, value (strings unless castToString=false),
 * topic, partition, offset, timestamp.
 * <pre>
 * options { bootstrapServers = "localhost:9092", subscribe = "orders", startingOffsets = latest,
 *           maxOffsetsPerTrigger = 10000, kafka { "security.protocol" = SASL_SSL } }
 * </pre>
 */
public class KafkaSource implements Source {
    @Override
    public Dataset<Row> read(SparkSession spark, StepConfig step) {
        DataStreamReader reader = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", step.string("bootstrapServers"))
                .option("startingOffsets", step.string("startingOffsets", "latest"))
                .option("failOnDataLoss", String.valueOf(step.bool("failOnDataLoss", false)));

        int subscriptions = 0;
        for (String mode : new String[] {"subscribe", "subscribePattern", "assign"}) {
            if (step.has(mode)) {
                reader = reader.option(mode, step.string(mode));
                subscriptions++;
            }
        }
        if (subscriptions != 1) {
            throw step.error("set exactly one of 'subscribe', 'subscribePattern' or 'assign'");
        }
        if (step.has("maxOffsetsPerTrigger")) {
            reader = reader.option("maxOffsetsPerTrigger", step.string("maxOffsetsPerTrigger"));
        }
        // anything Kafka-specific (security, SASL, timeouts) is passed through with the "kafka." prefix
        for (Map.Entry<String, String> e : step.map("kafka").entrySet()) {
            reader = reader.option("kafka." + e.getKey(), e.getValue());
        }
        for (Map.Entry<String, String> e : step.map("extraOptions").entrySet()) {
            reader = reader.option(e.getKey(), e.getValue());
        }
        Dataset<Row> df = reader.load();
        if (step.bool("castToString", true)) {
            df = df.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value",
                    "topic", "partition", "offset", "timestamp");
        }
        return df;
    }
}
