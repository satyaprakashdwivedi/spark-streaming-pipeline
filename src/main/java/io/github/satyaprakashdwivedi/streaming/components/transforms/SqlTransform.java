package io.github.satyaprakashdwivedi.streaming.components.transforms;

import io.github.satyaprakashdwivedi.streaming.api.Transform;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * {@code transform.sql}: runs Spark SQL over the input, registered as a temp view.
 * <pre>
 * options { view = orders, query = "SELECT order_id, amount * 1.18 AS gross FROM orders WHERE amount > 0" }
 * </pre>
 * The view name defaults to the input step's name. Every earlier step is also available as a view by its name.
 */
public class SqlTransform implements Transform {
    @Override
    public Dataset<Row> apply(SparkSession spark, Dataset<Row> input, StepConfig step) {
        String view = step.string("view", step.input().orElse("input"));
        input.createOrReplaceTempView(view);
        try {
            return spark.sql(step.string("query"));
        } catch (Exception e) {
            throw step.error("query failed to analyse: " + e.getMessage());
        }
    }
}
