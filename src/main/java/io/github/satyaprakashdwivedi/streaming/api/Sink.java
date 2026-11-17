package io.github.satyaprakashdwivedi.streaming.api;

import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.concurrent.TimeoutException;

/** Starts a streaming query that writes the input somewhere (writeStream ... start()). */
@FunctionalInterface
public interface Sink {
    StreamingQuery start(SparkSession spark, Dataset<Row> input, StepConfig step) throws TimeoutException;
}
