package io.github.satyaprakashdwivedi.streaming.components.sinks;

import io.github.satyaprakashdwivedi.streaming.api.Sink;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.concurrent.TimeoutException;

/** {@code sink.memory}: keeps results in an in-memory table named after queryName. For tests and debugging only. */
public class MemorySink implements Sink {
    @Override
    public StreamingQuery start(SparkSession spark, Dataset<Row> input, StepConfig step) throws TimeoutException {
        return SinkSupport.common(input.writeStream().format("memory"), step, false).start();
    }
}
