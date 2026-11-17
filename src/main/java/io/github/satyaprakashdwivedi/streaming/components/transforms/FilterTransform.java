package io.github.satyaprakashdwivedi.streaming.components.transforms;

import io.github.satyaprakashdwivedi.streaming.api.Transform;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** {@code transform.filter}: keeps rows matching a SQL condition. {@code options { condition = "amount > 100" }} */
public class FilterTransform implements Transform {
    @Override
    public Dataset<Row> apply(SparkSession spark, Dataset<Row> input, StepConfig step) {
        try {
            return input.where(step.string("condition"));
        } catch (Exception e) {
            throw step.error("invalid condition: " + e.getMessage());
        }
    }
}
