package io.github.satyaprakashdwivedi.streaming.api;

import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Creates a streaming DataFrame (readStream). */
@FunctionalInterface
public interface Source {
    Dataset<Row> read(SparkSession spark, StepConfig step);
}
