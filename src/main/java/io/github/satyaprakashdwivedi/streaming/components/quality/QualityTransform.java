package io.github.satyaprakashdwivedi.streaming.components.quality;

import io.github.satyaprakashdwivedi.streaming.api.MultiOutputTransform;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_compact;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.raise_error;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.when;

/**
 * {@code transform.quality}: row-level data quality checks on a stream.
 * Adds {@code _dq_failed_rules} (array of failed rule names) and then, depending on {@code onFailure}:
 * <ul>
 *   <li>{@code quarantine} (default): good rows continue; bad rows go to the extra output {@code <step>_quarantine}
 *       with {@code _dq_failed_rules} and {@code _dq_checked_at}, ready for any sink.</li>
 *   <li>{@code drop}: bad rows are removed.</li>
 *   <li>{@code flag}: all rows continue, with {@code _dq_failed_rules} and {@code _dq_passed} columns.</li>
 *   <li>{@code fail}: the query stops on the first bad row, naming the failed rules.</li>
 * </ul>
 * See {@link QualityRuleParser} for the rule syntax and the Great Expectations suite import.
 */
public class QualityTransform implements MultiOutputTransform {
    static final String FAILED_RULES = "_dq_failed_rules";

    @Override
    public List<String> extraOutputNames(StepConfig step) {
        return "quarantine".equals(onFailure(step)) ? List.of("quarantine") : Collections.emptyList();
    }

    @Override
    public Map<String, Dataset<Row>> applyWithExtraOutputs(SparkSession spark, Dataset<Row> input, StepConfig step) {
        List<QualityRule> rules = QualityRuleParser.parse(step);
        List<Column> failedRuleNames = new ArrayList<>();
        for (QualityRule rule : rules) {
            failedRuleNames.add(when(rule.passesWhen().equalTo(lit(false)), lit(rule.name())));
        }
        Column failedRules = array_compact(array(failedRuleNames.toArray(new Column[0])));
        Dataset<Row> checked = input.withColumn(FAILED_RULES, failedRules);
        Column passed = size(checked.col(FAILED_RULES)).equalTo(0);

        Map<String, Dataset<Row>> outputs = new LinkedHashMap<>();
        String mode = onFailure(step);
        switch (mode) {
            case "quarantine":
                outputs.put("", checked.where(passed).drop(FAILED_RULES));
                outputs.put("quarantine", checked.where(passed.unary_$bang())
                        .withColumn("_dq_checked_at", current_timestamp()));
                break;
            case "drop":
                outputs.put("", checked.where(passed).drop(FAILED_RULES));
                break;
            case "flag":
                outputs.put("", checked.withColumn("_dq_passed", passed));
                break;
            case "fail":
                // a filter, not a dropped column: the optimizer can't prune it away, so every row is checked
                Column passOrStop = when(passed, lit(true)).otherwise(raise_error(
                        concat_ws("", lit("Data quality failure in step '" + step.name() + "': "),
                                concat_ws(", ", checked.col(FAILED_RULES)))));
                outputs.put("", checked.where(passOrStop).drop(FAILED_RULES));
                break;
            default:
                throw step.error("onFailure must be quarantine, drop, flag or fail (got '" + mode + "')");
        }
        return outputs;
    }

    private static String onFailure(StepConfig step) {
        return step.string("onFailure", "quarantine");
    }
}
