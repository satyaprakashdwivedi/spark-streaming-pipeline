package io.github.satyaprakashdwivedi.streaming.components.transforms;

import io.github.satyaprakashdwivedi.streaming.api.Transform;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * {@code transform.watermark}: declares how late data may arrive, so stateful steps (aggregations, dedup, joins)
 * can drop old state. {@code options { column = event_time, delay = "10 minutes" }}
 */
public class WatermarkTransform implements Transform {
    @Override
    public Dataset<Row> apply(SparkSession spark, Dataset<Row> input, StepConfig step) {
        return input.withWatermark(step.string("column"), step.string("delay"));
    }
}
