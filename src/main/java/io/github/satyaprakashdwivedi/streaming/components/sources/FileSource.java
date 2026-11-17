package io.github.satyaprakashdwivedi.streaming.components.sources;

import io.github.satyaprakashdwivedi.streaming.api.Source;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;

import java.util.Map;

/**
 * {@code source.file}: picks up new files in a folder (local, HDFS, S3, ADLS, GCS).
 * <pre>
 * options { format = json, path = "/data/in", schema = "id STRING, amount DOUBLE, ts TIMESTAMP",
 *           maxFilesPerTrigger = 10, readerOptions { header = true } }
 * </pre>
 * A schema (Spark DDL) is required for json and csv; parquet/orc can infer it from existing files.
 */
public class FileSource implements Source {
    @Override
    public Dataset<Row> read(SparkSession spark, StepConfig step) {
        String format = step.string("format", "json");
        DataStreamReader reader = spark.readStream().format(format);
        if (step.has("schema")) {
            reader = reader.schema(step.string("schema"));
        } else if (format.equals("json") || format.equals("csv")) {
            throw step.error("option 'schema' (Spark DDL, e.g. \"id STRING, amount DOUBLE\") is required for "
                    + format + " files");
        } else if (!format.equals("text")) {
            spark.conf().set("spark.sql.streaming.schemaInference", "true");
        }
        if (step.has("maxFilesPerTrigger")) {
            reader = reader.option("maxFilesPerTrigger", step.integer("maxFilesPerTrigger", 1));
        }
        for (Map.Entry<String, String> e : step.map("readerOptions").entrySet()) {
            reader = reader.option(e.getKey(), e.getValue());
        }
        return reader.load(step.string("path"));
    }
}
