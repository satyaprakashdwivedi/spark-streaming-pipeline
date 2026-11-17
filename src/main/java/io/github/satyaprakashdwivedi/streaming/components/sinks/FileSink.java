package io.github.satyaprakashdwivedi.streaming.components.sinks;

import io.github.satyaprakashdwivedi.streaming.api.Sink;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * {@code sink.file}: writes files (parquet, json, csv, orc; or delta if the Delta jar is on the classpath).
 * Exactly-once via the checkpoint. Append mode only, except formats like delta that support more.
 * <pre>
 * options { format = parquet, path = "s3a://lake/orders", checkpointLocation = "s3a://lake/_chk/orders",
 *           partitionBy = [order_date], trigger { processingTime = "1 minute" } }
 * </pre>
 */
public class FileSink implements Sink {
    @Override
    public StreamingQuery start(SparkSession spark, Dataset<Row> input, StepConfig step) throws TimeoutException {
        DataStreamWriter<Row> writer = SinkSupport.common(
                input.writeStream().format(step.string("format", "parquet")), step, true);
        List<String> partitionBy = step.list("partitionBy");
        if (!partitionBy.isEmpty()) {
            writer = writer.partitionBy(partitionBy.toArray(new String[0]));
        }
        return writer.option("path", step.string("path")).start();
    }
}
