package io.github.satyaprakashdwivedi.streaming.components.dbt;

import io.github.satyaprakashdwivedi.streaming.api.Transform;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@code transform.dbt}: runs the SQL of a dbt model inside the stream, so business rules live in your dbt project.
 * <pre>
 * options {
 *   projectDir = "dbt/orders_rules"       # the dbt project
 *   model = clean_orders                  # model to run
 *   compile = true                        # run "dbt compile --select clean_orders" first (needs dbt-spark installed)
 *   # profilesDir = "~/.dbt", target = dev, vars { min_amount = 1 }, dbtExecutable = dbt
 *   # compiledSqlPath = "dbt/orders_rules/target/compiled/.../clean_orders.sql"  # use SQL compiled in CI instead
 *   streamSchema = stream                 # {{ source('stream', 'orders') }} -> the pipeline step named orders
 *   relations { "analytics.customers" = customers }   # any other compiled relation -> a step/view name
 * }
 * </pre>
 * In the model, read pipeline steps through a source named {@code stream} (declare it in sources.yml with
 * {@code schema: stream}). Only streaming-compatible SQL works: no ORDER BY/LIMIT without aggregation, and
 * aggregations need a watermark. dbt tests are not run on the stream; use transform.quality for that.
 */
public class DbtModelTransform implements Transform {
    private static final Logger LOG = LoggerFactory.getLogger(DbtModelTransform.class);

    @Override
    public Dataset<Row> apply(SparkSession spark, Dataset<Row> input, StepConfig step) {
        String view = step.string("view", step.input().orElse("input"));
        input.createOrReplaceTempView(view);
        String compiledSql = loadCompiledSql(step);
        String runnableSql = rewriteRelations(compiledSql, step.string("streamSchema", "stream"), step.map("relations"));
        LOG.info("Step '{}' runs dbt model '{}':\n{}", step.name(), step.string("model"), runnableSql);
        try {
            return spark.sql(runnableSql);
        } catch (Exception e) {
            throw step.error("dbt model '" + step.string("model") + "' failed to analyse as Spark SQL: "
                    + e.getMessage() + "\nSQL:\n" + runnableSql);
        }
    }

    String loadCompiledSql(StepConfig step) {
        if (step.has("compiledSqlPath")) {
            return read(step, Paths.get(expandHome(step.string("compiledSqlPath"))));
        }
        Path projectDir = Paths.get(expandHome(step.string("projectDir"))).toAbsolutePath();
        String model = step.string("model");
        if (step.bool("compile", true)) {
            runDbtCompile(step, projectDir, model);
        }
        return read(step, findCompiledModel(step, projectDir, model));
    }

    private void runDbtCompile(StepConfig step, Path projectDir, String model) {
        List<String> command = new ArrayList<>(List.of(step.string("dbtExecutable", "dbt"), "compile", "--select", model));
        // dbt runs inside the project folder, so relative paths from the config must be made absolute first
        step.optString("profilesDir").ifPresent(dir -> command.addAll(
                List.of("--profiles-dir", Paths.get(expandHome(dir)).toAbsolutePath().toString())));
        step.optString("target").ifPresent(target -> command.addAll(List.of("--target", target)));
        Map<String, String> vars = step.map("vars");
        if (!vars.isEmpty()) {
            command.addAll(List.of("--vars", toYamlInline(vars)));
        }
        LOG.info("Compiling dbt model: {} (in {})", String.join(" ", command), projectDir);
        try {
            Process process = new ProcessBuilder(command).directory(projectDir.toFile()).redirectErrorStream(true).start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!process.waitFor(step.integer("compileTimeoutSeconds", 300), TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw step.error("dbt compile timed out");
            }
            if (process.exitValue() != 0) {
                throw step.error("dbt compile failed (exit " + process.exitValue() + "):\n" + lastLines(output, 30));
            }
        } catch (IOException e) {
            throw step.error("could not run '" + command.get(0) + "'. Install dbt with a Spark adapter "
                    + "(pip install dbt-spark), or compile in CI and set compiledSqlPath. (" + e.getMessage() + ")");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw step.error("interrupted while running dbt compile");
        }
    }

    private Path findCompiledModel(StepConfig step, Path projectDir, String model) {
        Path compiledRoot = projectDir.resolve("target").resolve("compiled");
        if (!Files.isDirectory(compiledRoot)) {
            throw step.error("no compiled models in " + compiledRoot + "; run dbt compile or set compile = true");
        }
        try (Stream<Path> files = Files.walk(compiledRoot)) {
            List<Path> matches = files.filter(p -> p.getFileName().toString().equals(model + ".sql"))
                    .collect(Collectors.toList());
            if (matches.isEmpty()) {
                throw step.error("compiled SQL for model '" + model + "' not found under " + compiledRoot);
            }
            if (matches.size() > 1) {
                throw step.error("several compiled files named " + model + ".sql: " + matches + "; set compiledSqlPath");
            }
            return matches.get(0);
        } catch (IOException e) {
            throw step.error("cannot search " + compiledRoot + ": " + e.getMessage());
        }
    }

    /**
     * Points the compiled SQL at pipeline views: {@code `stream`.`orders`} (optionally with a catalog prefix)
     * becomes {@code orders}, and every entry of {@code relations} is replaced as written.
     */
    static String rewriteRelations(String sql, String streamSchema, Map<String, String> relations) {
        String rewritten = sql.trim().replaceAll(";\\s*$", "");
        for (Map.Entry<String, String> relation : relations.entrySet()) {
            rewritten = rewritten.replace(relation.getKey(), relation.getValue());
        }
        String quoted = "[`\"]?";
        Pattern streamReference = Pattern.compile(
                "(?:" + quoted + "\\w+" + quoted + "\\s*\\.\\s*)?" + quoted + Pattern.quote(streamSchema) + quoted
                        + "\\s*\\.\\s*" + quoted + "(\\w+)" + quoted, Pattern.CASE_INSENSITIVE);
        Matcher matcher = streamReference.matcher(rewritten);
        return matcher.replaceAll("$1");
    }

    private static String read(StepConfig step, Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw step.error("cannot read compiled SQL " + path + ": " + e.getMessage());
        }
    }

    private static String toYamlInline(Map<String, String> vars) {
        return vars.entrySet().stream().map(e -> e.getKey() + ": '" + e.getValue().replace("'", "''") + "'")
                .collect(Collectors.joining(", ", "{", "}"));
    }

    private static String lastLines(String text, int count) {
        String[] lines = text.split("\\R");
        return String.join("\n", java.util.Arrays.copyOfRange(lines, Math.max(0, lines.length - count), lines.length));
    }

    private static String expandHome(String path) {
        return path.startsWith("~/") ? System.getProperty("user.home") + path.substring(1) : path;
    }
}
