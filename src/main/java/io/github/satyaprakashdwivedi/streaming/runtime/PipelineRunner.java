package io.github.satyaprakashdwivedi.streaming.runtime;

import io.github.satyaprakashdwivedi.streaming.PipelineException;
import io.github.satyaprakashdwivedi.streaming.api.MultiOutputTransform;
import io.github.satyaprakashdwivedi.streaming.api.Transform;
import io.github.satyaprakashdwivedi.streaming.config.PipelineConfig;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import io.github.satyaprakashdwivedi.streaming.registry.ComponentRegistry;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns a {@link PipelineConfig} into running streaming queries.
 * Steps run in order; each step reads the previous step's output unless it names another with {@code input},
 * so one source can feed several branches and several sinks. Every step's output is also registered as a temp
 * view under its name, so later SQL steps can refer to it.
 */
public final class PipelineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineRunner.class);

    private final ComponentRegistry registry;

    public PipelineRunner() {
        this(ComponentRegistry.withDefaults());
    }

    public PipelineRunner(ComponentRegistry registry) {
        this.registry = registry;
    }

    /** Checks the config without touching Spark: types exist, inputs resolve, there is a source and a sink. */
    public List<String> validate(PipelineConfig config) {
        List<String> plan = new ArrayList<>();
        Map<String, String> kinds = new LinkedHashMap<>();
        String previous = null;
        boolean sawSink = false;
        for (StepConfig step : config.steps()) {
            String kind = step.kind();
            if (!registry.knows(step.type())) {
                throw new PipelineException("Step '" + step.name() + "' has unknown type '" + step.type()
                        + "'. Available: " + registry.describe());
            }
            String input = null;
            if (!kind.equals("source")) {
                input = step.input().orElse(previous);
                if (input == null) {
                    throw new PipelineException("Step '" + step.name() + "' (" + step.type()
                            + ") has nothing to read from: the first step must be a source");
                }
                if (!kinds.containsKey(input)) {
                    throw new PipelineException("Step '" + step.name() + "' reads input '" + input
                            + "', which is not an earlier step. Earlier steps: " + kinds.keySet());
                }
                if ("sink".equals(kinds.get(input))) {
                    throw new PipelineException("Step '" + step.name() + "' reads from sink '" + input
                            + "'; sinks produce no data. Point 'input' at a source or transform");
                }
            }
            kinds.put(step.name(), kind);
            plan.add(step.name() + " [" + step.type() + "]" + (input == null ? "" : " <- " + input));
            if (kind.equals("transform") && registry.transform(step.type()) instanceof MultiOutputTransform) {
                for (String extra : ((MultiOutputTransform) registry.transform(step.type())).extraOutputNames(step)) {
                    kinds.put(step.name() + "_" + extra, kind);
                    plan.add(step.name() + "_" + extra + " [extra output of " + step.name() + "]");
                }
            }
            if (kind.equals("sink")) {
                sawSink = true;
            } else {
                previous = step.name();
            }
        }
        if (!sawSink) {
            throw new PipelineException("Pipeline has no sink step: nothing would run. Add e.g. type = sink.console");
        }
        return plan;
    }

    public SparkSession createSession(PipelineConfig config) {
        SparkConf conf = new SparkConf().setAppName(config.appName());
        config.master().ifPresent(conf::setMaster);
        if (!conf.contains("spark.master")) {
            // spark-submit always sets a master; when run from an IDE/Maven fall back to local mode
            conf.setMaster("local[*]");
        }
        config.sparkConf().forEach(conf::set);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        spark.sparkContext().setLogLevel(config.logLevel());
        return spark;
    }

    /** Builds and starts every sink. Returns immediately; use {@link RunningPipeline} to wait or stop. */
    public RunningPipeline start(SparkSession spark, PipelineConfig config) {
        List<String> plan = validate(config);
        LOG.info("Pipeline '{}' plan:\n  {}", config.appName(), String.join("\n  ", plan));
        Map<String, Dataset<Row>> outputs = new LinkedHashMap<>();
        Map<String, StreamingQuery> queries = new LinkedHashMap<>();
        String previous = null;
        try {
            for (StepConfig step : config.steps()) {
                switch (step.kind()) {
                    case "source": {
                        Dataset<Row> df = registry.source(step.type()).read(spark, step);
                        register(step.name(), df, outputs);
                        previous = step.name();
                        break;
                    }
                    case "transform": {
                        String in = step.input().orElse(previous);
                        Transform transform = registry.transform(step.type());
                        if (transform instanceof MultiOutputTransform) {
                            Map<String, Dataset<Row>> results = ((MultiOutputTransform) transform)
                                    .applyWithExtraOutputs(spark, outputs.get(in), step);
                            for (Map.Entry<String, Dataset<Row>> result : results.entrySet()) {
                                String outputName = result.getKey().isEmpty() ? step.name()
                                        : step.name() + "_" + result.getKey();
                                register(outputName, result.getValue(), outputs);
                            }
                        } else {
                            register(step.name(), transform.apply(spark, outputs.get(in), step), outputs);
                        }
                        previous = step.name();
                        break;
                    }
                    case "sink": {
                        String in = step.input().orElse(previous);
                        StreamingQuery q = registry.sink(step.type()).start(spark, outputs.get(in), step);
                        queries.put(step.name(), q);
                        LOG.info("Started query '{}' (sink {}) id={}", q.name(), step.name(), q.id());
                        break;
                    }
                    default:
                        throw new PipelineException("Step '" + step.name() + "': type must start with source., "
                                + "transform. or sink. (got '" + step.type() + "')");
                }
            }
        } catch (PipelineException e) {
            queries.values().forEach(PipelineRunner::stopQuietly);
            throw e;
        } catch (Exception e) {
            queries.values().forEach(PipelineRunner::stopQuietly);
            throw new PipelineException("Failed to start pipeline: " + e.getMessage(), e);
        }
        return new RunningPipeline(spark, queries);
    }

    private static void register(String name, Dataset<Row> df, Map<String, Dataset<Row>> outputs) {
        outputs.put(name, df);
        df.createOrReplaceTempView(name.replaceAll("[^A-Za-z0-9_]", "_"));
    }

    private static void stopQuietly(StreamingQuery q) {
        try {
            q.stop();
        } catch (Exception ignored) {
            // best effort
        }
    }
}
