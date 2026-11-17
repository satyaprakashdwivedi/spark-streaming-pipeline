package io.github.satyaprakashdwivedi.streaming.components.quality;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;

/**
 * Builds {@link QualityRule}s from the step's {@code rules} list and, optionally, from a Great Expectations
 * suite file. Null values pass every check except {@code not_null} (the same convention Great Expectations uses).
 *
 * <pre>
 * rules = [
 *   { column = order_id, check = not_null }
 *   { column = amount,   check = between, min = 0, max = 1000000 }
 *   { column = currency, check = in_set, values = [INR, USD, EUR] }
 *   { column = email,    check = regex, pattern = "^[^@]+@[^@]+$" }
 *   { column = sku,      check = length_between, min = 6, max = 12 }
 *   { name = refund_sign, check = sql, condition = "amount >= 0 OR status = 'REFUND'" }
 * ]
 * expectationSuite = "quality/orders_suite.json"
 * </pre>
 */
public final class QualityRuleParser {
    private static final Logger LOG = LoggerFactory.getLogger(QualityRuleParser.class);

    private QualityRuleParser() {}

    public static List<QualityRule> parse(StepConfig step) {
        List<QualityRule> rules = new ArrayList<>();
        if (step.has("rules")) {
            int position = 0;
            for (Config rule : step.raw().getConfigList("rules")) {
                position++;
                rules.add(fromRuleConfig(step, rule, position));
            }
        }
        if (step.has("expectationSuite")) {
            rules.addAll(fromExpectationSuite(step, step.string("expectationSuite"),
                    step.bool("strictSuiteImport", false)));
        }
        if (rules.isEmpty()) {
            throw step.error("define at least one entry in 'rules' or an 'expectationSuite' file");
        }
        return rules;
    }

    private static QualityRule fromRuleConfig(StepConfig step, Config rule, int position) {
        String check = required(step, rule, "check", position);
        String column = rule.hasPath("column") ? rule.getString("column") : null;
        String name = rule.hasPath("name") ? rule.getString("name")
                : (column == null ? check + "_" + position : column + "_" + check);
        try {
            switch (check) {
                case "not_null":
                    return new QualityRule(name, col(needColumn(step, column, check)).isNotNull());
                case "between": {
                    Column value = col(needColumn(step, column, check));
                    return new QualityRule(name, nullPasses(between(value,
                            rule.hasPath("min") ? rule.getAnyRef("min") : null,
                            rule.hasPath("max") ? rule.getAnyRef("max") : null,
                            rule.hasPath("strictMin") && rule.getBoolean("strictMin"),
                            rule.hasPath("strictMax") && rule.getBoolean("strictMax"))));
                }
                case "in_set":
                    return new QualityRule(name, nullPasses(col(needColumn(step, column, check))
                            .isin(rule.getAnyRefList("values").toArray())));
                case "not_in_set":
                    return new QualityRule(name, nullPasses(col(needColumn(step, column, check))
                            .isin(rule.getAnyRefList("values").toArray()).unary_$bang()));
                case "regex":
                    return new QualityRule(name, nullPasses(col(needColumn(step, column, check))
                            .rlike(rule.getString("pattern"))));
                case "not_regex":
                    return new QualityRule(name, nullPasses(col(needColumn(step, column, check))
                            .rlike(rule.getString("pattern")).unary_$bang()));
                case "length_between": {
                    Column textLength = length(col(needColumn(step, column, check)));
                    return new QualityRule(name, nullPasses(between(textLength,
                            rule.hasPath("min") ? rule.getAnyRef("min") : null,
                            rule.hasPath("max") ? rule.getAnyRef("max") : null, false, false)));
                }
                case "sql":
                    return new QualityRule(name, nullPasses(expr(rule.getString("condition"))));
                default:
                    throw step.error("rule #" + position + " has unknown check '" + check + "'. Use one of: not_null, "
                            + "between, in_set, not_in_set, regex, not_regex, length_between, sql");
            }
        } catch (ConfigException e) {
            throw step.error("rule #" + position + " (" + check + "): " + e.getMessage());
        }
    }

    /**
     * Translates the row-level expectations of a Great Expectations suite (0.x {@code expectation_type} or
     * 1.x {@code type} format). Table-level expectations (row counts, means, uniqueness) can't be checked one row
     * at a time; they are skipped with a warning, or rejected when {@code strictSuiteImport = true}.
     */
    static List<QualityRule> fromExpectationSuite(StepConfig step, String path, boolean strict) {
        File file = new File(path);
        if (!file.isFile()) {
            throw step.error("expectationSuite file not found: " + file.getAbsolutePath());
        }
        Config suite = ConfigFactory.parseFile(file);
        List<QualityRule> rules = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        int position = 0;
        for (Config expectation : suite.getConfigList("expectations")) {
            position++;
            String type = expectation.hasPath("expectation_type") ? expectation.getString("expectation_type")
                    : expectation.getString("type");
            Config kwargs = expectation.hasPath("kwargs") ? expectation.getConfig("kwargs") : ConfigFactory.empty();
            if (kwargs.hasPath("mostly")) {
                LOG.warn("Expectation '{}' uses 'mostly'; row-level checks apply it to every row", type);
            }
            String column = kwargs.hasPath("column") ? kwargs.getString("column") : null;
            String name = type.replace("expect_column_values_to_", "").replace("expect_column_value_", "")
                    + (column == null ? "_" + position : "_" + column);
            Column condition = translateExpectation(type, kwargs, column);
            if (condition == null) {
                skipped.add(type);
            } else {
                rules.add(new QualityRule(name, condition));
            }
        }
        if (!skipped.isEmpty()) {
            String message = "expectations that need the whole table, not one row: " + skipped;
            if (strict) {
                throw step.error("cannot import " + message);
            }
            LOG.warn("Skipped {} (check them in batch, e.g. with Great Expectations on the landed data)", message);
        }
        return rules;
    }

    private static Column translateExpectation(String type, Config kwargs, String column) {
        switch (type) {
            case "expect_column_values_to_not_be_null":
                return col(column).isNotNull();
            case "expect_column_values_to_be_null":
                return col(column).isNull();
            case "expect_column_values_to_be_between":
                return nullPasses(between(col(column),
                        kwargs.hasPath("min_value") ? kwargs.getAnyRef("min_value") : null,
                        kwargs.hasPath("max_value") ? kwargs.getAnyRef("max_value") : null,
                        kwargs.hasPath("strict_min") && kwargs.getBoolean("strict_min"),
                        kwargs.hasPath("strict_max") && kwargs.getBoolean("strict_max")));
            case "expect_column_values_to_be_in_set":
                return nullPasses(col(column).isin(kwargs.getAnyRefList("value_set").toArray()));
            case "expect_column_values_to_not_be_in_set":
                return nullPasses(col(column).isin(kwargs.getAnyRefList("value_set").toArray()).unary_$bang());
            case "expect_column_values_to_match_regex":
                return nullPasses(col(column).rlike(kwargs.getString("regex")));
            case "expect_column_values_to_not_match_regex":
                return nullPasses(col(column).rlike(kwargs.getString("regex")).unary_$bang());
            case "expect_column_value_lengths_to_be_between":
                return nullPasses(between(length(col(column)),
                        kwargs.hasPath("min_value") ? kwargs.getAnyRef("min_value") : null,
                        kwargs.hasPath("max_value") ? kwargs.getAnyRef("max_value") : null, false, false));
            case "expect_column_value_lengths_to_equal":
                return nullPasses(length(col(column)).equalTo(kwargs.getInt("value")));
            default:
                return null;
        }
    }

    private static Column between(Column value, Object min, Object max, boolean strictMin, boolean strictMax) {
        Column condition = lit(true);
        if (min != null) {
            condition = condition.and(strictMin ? value.gt(min) : value.geq(min));
        }
        if (max != null) {
            condition = condition.and(strictMax ? value.lt(max) : value.leq(max));
        }
        return condition;
    }

    /** Null input passes (a separate not_null rule decides whether nulls are allowed). */
    private static Column nullPasses(Column condition) {
        return org.apache.spark.sql.functions.coalesce(condition, lit(true));
    }

    private static String needColumn(StepConfig step, String column, String check) {
        if (column == null) {
            throw step.error("check '" + check + "' needs a 'column'");
        }
        return column;
    }

    private static String required(StepConfig step, Config rule, String key, int position) {
        if (!rule.hasPath(key)) {
            throw step.error("rule #" + position + " is missing '" + key + "'");
        }
        return rule.getString(key);
    }
}
