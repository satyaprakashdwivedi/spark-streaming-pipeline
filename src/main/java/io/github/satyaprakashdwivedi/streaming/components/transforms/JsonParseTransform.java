package io.github.satyaprakashdwivedi.streaming.components.transforms;

import io.github.satyaprakashdwivedi.streaming.api.Transform;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

/**
 * {@code transform.json}: parses a JSON string column (Kafka {@code value} by default) into columns.
 * <pre>
 * options { column = value, schema = "order_id STRING, amount DOUBLE, created_at TIMESTAMP",
 *           keep = [topic, partition, offset, timestamp], dropMalformed = true }
 * </pre>
 * Rows that are not valid JSON parse to nulls; dropMalformed removes them.
 */
public class JsonParseTransform implements Transform {
    @Override
    public Dataset<Row> apply(SparkSession spark, Dataset<Row> input, StepConfig step) {
        String column = step.string("column", "value");
        if (!Arrays.asList(input.columns()).contains(column)) {
            throw step.error("input has no column '" + column + "'; columns are " + Arrays.toString(input.columns()));
        }
        StructType schema;
        try {
            schema = StructType.fromDDL(step.string("schema"));
        } catch (Exception e) {
            throw step.error("option 'schema' is not valid Spark DDL: " + e.getMessage());
        }
        List<String> keep = step.has("keep") ? step.list("keep")
                : Arrays.asList("key", "topic", "partition", "offset", "timestamp");
        List<Column> select = new ArrayList<>();
        select.add(from_json(col(column).cast("string"), (DataType) schema, step.map("jsonOptions")).as("_json"));
        List<String> existing = Arrays.asList(input.columns());
        List<String> kept = new ArrayList<>();
        for (String k : keep) {
            if (existing.contains(k) && !k.equals(column) && schema.getFieldIndex(k).isEmpty()) {
                select.add(col(k));
                kept.add(k);
            }
        }
        Dataset<Row> parsed = input.select(select.toArray(new Column[0]));
        if (step.bool("dropMalformed", false)) {
            // from_json yields a struct of all-null fields for bad input: require at least one parsed field
            Column anyField = null;
            for (String f : schema.fieldNames()) {
                Column c = col("_json." + f).isNotNull();
                anyField = anyField == null ? c : anyField.or(c);
            }
            parsed = parsed.where(col("_json").isNotNull().and(anyField));
        }
        List<Column> out = new ArrayList<>();
        out.add(col("_json.*"));
        kept.forEach(k -> out.add(col(k)));
        return parsed.select(out.toArray(new Column[0]));
    }
}
