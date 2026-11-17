package io.github.satyaprakashdwivedi.streaming.components.transforms;

import io.github.satyaprakashdwivedi.streaming.api.Transform;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * {@code transform.dropDuplicates}: removes repeated events by key. {@code options { columns = [order_id] }}
 * Put a transform.watermark before it (and include the event-time column) or the dedup state grows forever.
 * With {@code withinWatermark = true} (Spark 3.5+) duplicates are dropped within the watermark delay only.
 */
public class DropDuplicatesTransform implements Transform {
    @Override
    public Dataset<Row> apply(SparkSession spark, Dataset<Row> input, StepConfig step) {
        List<String> cols = step.list("columns");
        if (cols.isEmpty()) {
            throw step.error("option 'columns' must list the key columns");
        }
        String first = cols.get(0);
        String[] rest = cols.subList(1, cols.size()).toArray(new String[0]);
        return step.bool("withinWatermark", false)
                ? input.dropDuplicatesWithinWatermark(first, rest)
                : input.dropDuplicates(first, rest);
    }
}
