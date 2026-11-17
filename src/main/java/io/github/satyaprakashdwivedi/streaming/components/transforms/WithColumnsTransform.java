package io.github.satyaprakashdwivedi.streaming.components.transforms;

import io.github.satyaprakashdwivedi.streaming.api.Transform;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

import static org.apache.spark.sql.functions.expr;

/** {@code transform.withColumns}: adds or replaces columns. {@code options { columns { gross = "amount * 1.18", ingested_at = "current_timestamp()" } }} */
public class WithColumnsTransform implements Transform {
    @Override
    public Dataset<Row> apply(SparkSession spark, Dataset<Row> input, StepConfig step) {
        Map<String, String> columns = step.map("columns");
        if (columns.isEmpty()) {
            throw step.error("option 'columns' must map at least one column name to an expression");
        }
        Dataset<Row> out = input;
        for (Map.Entry<String, String> e : columns.entrySet()) {
            try {
                out = out.withColumn(e.getKey(), expr(e.getValue()));
            } catch (Exception ex) {
                throw step.error("column '" + e.getKey() + "': invalid expression: " + ex.getMessage());
            }
        }
        return out;
    }
}
