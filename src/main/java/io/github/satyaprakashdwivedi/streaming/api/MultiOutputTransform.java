package io.github.satyaprakashdwivedi.streaming.api;

import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;

/**
 * A transform that also produces extra named outputs (e.g. a quarantine stream of rejected rows).
 * Each extra output is available to later steps as {@code <step name>_<output name>}.
 */
public interface MultiOutputTransform extends Transform {

    /** Names of the extra outputs this step will produce with the given options (may be empty). */
    List<String> extraOutputNames(StepConfig step);

    /** The main output under key {@code ""}, plus one entry per extra output name. */
    Map<String, Dataset<Row>> applyWithExtraOutputs(SparkSession spark, Dataset<Row> input, StepConfig step);

    @Override
    default Dataset<Row> apply(SparkSession spark, Dataset<Row> input, StepConfig step) {
        return applyWithExtraOutputs(spark, input, step).get("");
    }
}
