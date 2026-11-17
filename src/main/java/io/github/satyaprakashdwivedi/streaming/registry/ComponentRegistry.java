package io.github.satyaprakashdwivedi.streaming.registry;

import io.github.satyaprakashdwivedi.streaming.PipelineException;
import io.github.satyaprakashdwivedi.streaming.api.ComponentProvider;
import io.github.satyaprakashdwivedi.streaming.api.Sink;
import io.github.satyaprakashdwivedi.streaming.api.Source;
import io.github.satyaprakashdwivedi.streaming.api.Transform;
import io.github.satyaprakashdwivedi.streaming.components.dbt.DbtModelTransform;
import io.github.satyaprakashdwivedi.streaming.components.quality.QualityTransform;
import io.github.satyaprakashdwivedi.streaming.components.sinks.ConsoleSink;
import io.github.satyaprakashdwivedi.streaming.components.sinks.FileSink;
import io.github.satyaprakashdwivedi.streaming.components.sinks.JdbcSink;
import io.github.satyaprakashdwivedi.streaming.components.sinks.KafkaSink;
import io.github.satyaprakashdwivedi.streaming.components.sinks.MemorySink;
import io.github.satyaprakashdwivedi.streaming.components.sources.FileSource;
import io.github.satyaprakashdwivedi.streaming.components.sources.KafkaSource;
import io.github.satyaprakashdwivedi.streaming.components.sources.RateSource;
import io.github.satyaprakashdwivedi.streaming.components.transforms.DropDuplicatesTransform;
import io.github.satyaprakashdwivedi.streaming.components.transforms.FilterTransform;
import io.github.satyaprakashdwivedi.streaming.components.transforms.JsonParseTransform;
import io.github.satyaprakashdwivedi.streaming.components.transforms.LookupJoinTransform;
import io.github.satyaprakashdwivedi.streaming.components.transforms.SelectTransform;
import io.github.satyaprakashdwivedi.streaming.components.transforms.SqlTransform;
import io.github.satyaprakashdwivedi.streaming.components.transforms.WatermarkTransform;
import io.github.satyaprakashdwivedi.streaming.components.transforms.WithColumnsTransform;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;

/** Maps step types (e.g. {@code source.kafka}) to implementations. Built-ins plus any ServiceLoader plug-ins. */
public final class ComponentRegistry {
    private final Map<String, Source> sources = new TreeMap<>();
    private final Map<String, Transform> transforms = new TreeMap<>();
    private final Map<String, Sink> sinks = new TreeMap<>();

    /** Registry with the built-in components and every ComponentProvider found on the classpath. */
    public static ComponentRegistry withDefaults() {
        ComponentRegistry r = new ComponentRegistry();
        r.source("source.kafka", new KafkaSource());
        r.source("source.rate", new RateSource());
        r.source("source.file", new FileSource());

        r.transform("transform.json", new JsonParseTransform());
        r.transform("transform.sql", new SqlTransform());
        r.transform("transform.filter", new FilterTransform());
        r.transform("transform.select", new SelectTransform());
        r.transform("transform.withColumns", new WithColumnsTransform());
        r.transform("transform.watermark", new WatermarkTransform());
        r.transform("transform.dropDuplicates", new DropDuplicatesTransform());
        r.transform("transform.lookup", new LookupJoinTransform());
        r.transform("transform.quality", new QualityTransform());
        r.transform("transform.dbt", new DbtModelTransform());

        r.sink("sink.console", new ConsoleSink());
        r.sink("sink.kafka", new KafkaSink());
        r.sink("sink.file", new FileSink());
        r.sink("sink.jdbc", new JdbcSink());
        r.sink("sink.memory", new MemorySink());

        for (ComponentProvider provider : ServiceLoader.load(ComponentProvider.class)) {
            provider.register(r);
        }
        return r;
    }

    public ComponentRegistry source(String type, Source source) {
        sources.put(check(type, "source."), source);
        return this;
    }

    public ComponentRegistry transform(String type, Transform transform) {
        transforms.put(check(type, "transform."), transform);
        return this;
    }

    public ComponentRegistry sink(String type, Sink sink) {
        sinks.put(check(type, "sink."), sink);
        return this;
    }

    public Source source(String type) {
        return find(sources, type, "source");
    }

    public Transform transform(String type) {
        return find(transforms, type, "transform");
    }

    public Sink sink(String type) {
        return find(sinks, type, "sink");
    }

    public boolean knows(String type) {
        return sources.containsKey(type) || transforms.containsKey(type) || sinks.containsKey(type);
    }

    public String describe() {
        return "sources " + sources.keySet() + ", transforms " + transforms.keySet() + ", sinks " + sinks.keySet();
    }

    private static String check(String type, String prefix) {
        if (type == null || !type.startsWith(prefix) || type.length() == prefix.length()) {
            throw new PipelineException("Component type '" + type + "' must look like '" + prefix + "<name>'");
        }
        return type;
    }

    private <T> T find(Map<String, T> map, String type, String kind) {
        T found = map.get(type);
        if (found == null) {
            throw new PipelineException("Unknown " + kind + " type '" + type + "'. Available: " + describe());
        }
        return found;
    }
}
