package io.github.satyaprakashdwivedi.streaming.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;
import io.github.satyaprakashdwivedi.streaming.PipelineException;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A whole pipeline loaded from HOCON or JSON:
 * <pre>
 * app   { name = "orders", master = "local[2]", logLevel = WARN, sparkConf { "spark.sql.shuffle.partitions" = 8 } }
 * steps = [ { name = orders, type = source.kafka, options { ... } }, ... ]
 * </pre>
 * Values can come from environment variables with HOCON substitution: {@code password = ${?DB_PASSWORD}}.
 */
public final class PipelineConfig {
    private final String appName;
    private final String master;
    private final String logLevel;
    private final boolean awaitTermination;
    private final Map<String, String> sparkConf;
    private final List<StepConfig> steps;

    private PipelineConfig(String appName, String master, String logLevel, boolean awaitTermination,
                           Map<String, String> sparkConf, List<StepConfig> steps) {
        this.appName = appName;
        this.master = master;
        this.logLevel = logLevel;
        this.awaitTermination = awaitTermination;
        this.sparkConf = Collections.unmodifiableMap(sparkConf);
        this.steps = Collections.unmodifiableList(steps);
    }

    public static PipelineConfig fromFile(String path) {
        File file = new File(path);
        if (!file.isFile()) {
            throw new PipelineException("Pipeline config not found: " + file.getAbsolutePath());
        }
        try {
            Config parsed = ConfigFactory.parseFile(file, ConfigParseOptions.defaults().setAllowMissing(false));
            return fromConfig(parsed);
        } catch (ConfigException e) {
            throw new PipelineException("Cannot read pipeline config " + path + ": " + e.getMessage(), e);
        }
    }

    public static PipelineConfig fromString(String hocon) {
        try {
            return fromConfig(ConfigFactory.parseString(hocon));
        } catch (ConfigException e) {
            throw new PipelineException("Cannot parse pipeline config: " + e.getMessage(), e);
        }
    }

    public static PipelineConfig fromConfig(Config unresolved) {
        Config root;
        try {
            // resolve against the environment so ${?VAR} works; system properties override file values
            root = ConfigFactory.systemProperties().withFallback(unresolved)
                    .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(false));
        } catch (ConfigException e) {
            throw new PipelineException("Cannot resolve pipeline config: " + e.getMessage(), e);
        }
        Config app = root.hasPath("app") ? root.getConfig("app") : ConfigFactory.empty();
        String appName = app.hasPath("name") ? app.getString("name") : "spark-streaming-pipeline";
        String master = app.hasPath("master") ? app.getString("master") : null;
        String logLevel = app.hasPath("logLevel") ? app.getString("logLevel") : "WARN";
        boolean await = !app.hasPath("awaitTermination") || app.getBoolean("awaitTermination");
        Map<String, String> sparkConf = new LinkedHashMap<>();
        if (app.hasPath("sparkConf")) {
            for (Map.Entry<String, ConfigValue> e : app.getObject("sparkConf").entrySet()) {
                sparkConf.put(e.getKey(), String.valueOf(e.getValue().unwrapped()));
            }
        }

        if (!root.hasPath("steps")) {
            throw new PipelineException("Pipeline config has no 'steps' list");
        }
        List<StepConfig> steps = new ArrayList<>();
        Set<String> names = new HashSet<>();
        int i = 0;
        for (Config s : root.getConfigList("steps")) {
            i++;
            if (!s.hasPath("type")) {
                throw new PipelineException("Step #" + i + " has no 'type' (e.g. source.kafka, transform.sql, sink.console)");
            }
            String type = s.getString("type");
            String name = s.hasPath("name") ? s.getString("name") : "step" + i;
            if (!names.add(name)) {
                throw new PipelineException("Duplicate step name '" + name + "'; step names must be unique");
            }
            String input = s.hasPath("input") ? s.getString("input") : null;
            Config options = s.hasPath("options") ? s.getConfig("options") : ConfigFactory.empty();
            steps.add(new StepConfig(name, type, input, options));
        }
        if (steps.isEmpty()) {
            throw new PipelineException("Pipeline config has an empty 'steps' list");
        }
        return new PipelineConfig(appName, master, logLevel, await, sparkConf, steps);
    }

    public String appName() { return appName; }
    public Optional<String> master() { return Optional.ofNullable(master); }
    public String logLevel() { return logLevel; }
    public boolean awaitTermination() { return awaitTermination; }
    public Map<String, String> sparkConf() { return sparkConf; }
    public List<StepConfig> steps() { return steps; }
}
