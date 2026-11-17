package io.github.satyaprakashdwivedi.streaming.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import io.github.satyaprakashdwivedi.streaming.PipelineException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * One step of a pipeline: {@code name}, {@code type} (e.g. {@code source.kafka}), an optional {@code input}
 * (defaults to the previous step) and its {@code options}. Typed getters fail with a message that names the step.
 */
public final class StepConfig {
    private final String name;
    private final String type;
    private final String input;
    private final Config options;

    public StepConfig(String name, String type, String input, Config options) {
        this.name = name;
        this.type = type;
        this.input = input;
        this.options = options == null ? ConfigFactory.empty() : options;
    }

    public String name() { return name; }
    public String type() { return type; }
    public Optional<String> input() { return Optional.ofNullable(input); }
    public Config raw() { return options; }

    /** The component kind: the part of the type before the first dot (source, transform or sink). */
    public String kind() {
        int dot = type.indexOf('.');
        return dot < 0 ? type : type.substring(0, dot);
    }

    public boolean has(String key) {
        return options.hasPath(key);
    }

    public String string(String key) {
        if (!options.hasPath(key)) {
            throw missing(key);
        }
        try {
            return options.getString(key);
        } catch (ConfigException e) {
            throw invalid(key, "a string", e);
        }
    }

    public String string(String key, String defaultValue) {
        return options.hasPath(key) ? string(key) : defaultValue;
    }

    public Optional<String> optString(String key) {
        return options.hasPath(key) ? Optional.of(string(key)) : Optional.empty();
    }

    public boolean bool(String key, boolean defaultValue) {
        if (!options.hasPath(key)) {
            return defaultValue;
        }
        try {
            return options.getBoolean(key);
        } catch (ConfigException e) {
            throw invalid(key, "true or false", e);
        }
    }

    public int integer(String key, int defaultValue) {
        if (!options.hasPath(key)) {
            return defaultValue;
        }
        try {
            return options.getInt(key);
        } catch (ConfigException e) {
            throw invalid(key, "a whole number", e);
        }
    }

    public List<String> list(String key) {
        if (!options.hasPath(key)) {
            return Collections.emptyList();
        }
        try {
            if (options.getValue(key).valueType() == ConfigValueType.STRING) {
                List<String> out = new ArrayList<>();
                for (String part : options.getString(key).split(",")) {
                    if (!part.trim().isEmpty()) {
                        out.add(part.trim());
                    }
                }
                return out;
            }
            return options.getStringList(key);
        } catch (ConfigException e) {
            throw invalid(key, "a list of strings", e);
        }
    }

    /** A nested object of plain key/values, e.g. extra reader options. Keys may contain dots if quoted. */
    public Map<String, String> map(String key) {
        Map<String, String> out = new LinkedHashMap<>();
        if (!options.hasPath(key)) {
            return out;
        }
        try {
            for (Map.Entry<String, ConfigValue> e : options.getObject(key).entrySet()) {
                out.put(e.getKey(), String.valueOf(e.getValue().unwrapped()));
            }
        } catch (ConfigException e) {
            throw invalid(key, "an object of key/value pairs", e);
        }
        return out;
    }

    public Optional<Config> object(String key) {
        return options.hasPath(key) ? Optional.of(options.getConfig(key)) : Optional.empty();
    }

    public PipelineException error(String message) {
        return new PipelineException("Step '" + name + "' (" + type + "): " + message);
    }

    private PipelineException missing(String key) {
        return error("missing required option '" + key + "'");
    }

    private PipelineException invalid(String key, String expected, Exception cause) {
        return new PipelineException("Step '" + name + "' (" + type + "): option '" + key + "' must be " + expected, cause);
    }

    @Override
    public String toString() {
        return name + " [" + type + "]" + (input == null ? "" : " <- " + input);
    }
}
