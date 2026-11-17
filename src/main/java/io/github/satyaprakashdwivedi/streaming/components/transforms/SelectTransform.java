package io.github.satyaprakashdwivedi.streaming.components.transforms;

import io.github.satyaprakashdwivedi.streaming.api.Transform;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/** {@code transform.select}: SQL expressions to keep/derive. {@code options { expressions = ["id", "amount * 2 AS double_amount"] }} */
public class SelectTransform implements Transform {
    @Override
    public Dataset<Row> apply(SparkSession spark, Dataset<Row> input, StepConfig step) {
        List<String> exprs = step.list("expressions");
        if (exprs.isEmpty()) {
            throw step.error("option 'expressions' must list at least one column or expression");
        }
        try {
            return input.selectExpr(exprs.toArray(new String[0]));
        } catch (Exception e) {
            throw step.error("invalid expression: " + e.getMessage());
        }
    }
}
