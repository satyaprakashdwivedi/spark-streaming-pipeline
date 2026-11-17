package io.github.satyaprakashdwivedi.streaming.components.sources;

import io.github.satyaprakashdwivedi.streaming.api.Source;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** {@code source.rate}: generates (timestamp, value) rows. Needs no infrastructure: good for demos and tests. */
public class RateSource implements Source {
    @Override
    public Dataset<Row> read(SparkSession spark, StepConfig step) {
        return spark.readStream().format("rate")
                .option("rowsPerSecond", step.integer("rowsPerSecond", 5))
                .option("numPartitions", step.integer("numPartitions", 1))
                .load();
    }
}
