package io.github.satyaprakashdwivedi.streaming.api;

import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Turns one DataFrame into another (parse, filter, SQL, join, ...). Must stay lazy: no actions. */
@FunctionalInterface
public interface Transform {
    Dataset<Row> apply(SparkSession spark, Dataset<Row> input, StepConfig step);
}
