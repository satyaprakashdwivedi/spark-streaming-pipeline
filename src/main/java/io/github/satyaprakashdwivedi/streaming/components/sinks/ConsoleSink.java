package io.github.satyaprakashdwivedi.streaming.components.sinks;

import io.github.satyaprakashdwivedi.streaming.api.Sink;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.concurrent.TimeoutException;

/** {@code sink.console}: prints each micro-batch. For development. {@code options { numRows = 20, truncate = false }} */
public class ConsoleSink implements Sink {
    @Override
    public StreamingQuery start(SparkSession spark, Dataset<Row> input, StepConfig step) throws TimeoutException {
        return SinkSupport.common(input.writeStream().format("console"), step, false)
                .option("numRows", step.integer("numRows", 20))
                .option("truncate", step.bool("truncate", false))
                .start();
    }
}
