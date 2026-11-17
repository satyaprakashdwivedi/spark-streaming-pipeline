package io.github.satyaprakashdwivedi.streaming.runtime;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Collections;
import java.util.Map;

/** The started queries of a pipeline, keyed by sink step name. */
public final class RunningPipeline {
    private final SparkSession spark;
    private final Map<String, StreamingQuery> queries;

    RunningPipeline(SparkSession spark, Map<String, StreamingQuery> queries) {
        this.spark = spark;
        this.queries = Collections.unmodifiableMap(queries);
    }

    public SparkSession spark() { return spark; }
    public Map<String, StreamingQuery> queries() { return queries; }

    public StreamingQuery query(String sinkName) {
        StreamingQuery q = queries.get(sinkName);
        if (q == null) {
            throw new IllegalArgumentException("No sink named '" + sinkName + "'; sinks are " + queries.keySet());
        }
        return q;
    }

    /** Process everything currently available in every source (tests, availableNow-style runs). */
    public void processAllAvailable() {
        queries.values().forEach(StreamingQuery::processAllAvailable);
    }

    /** Block until every query stops; rethrows the first failure. */
    public void awaitTermination() throws StreamingQueryException {
        for (StreamingQuery q : queries.values()) {
            q.awaitTermination();
        }
    }

    public void stop() {
        for (StreamingQuery q : queries.values()) {
            try {
                q.stop();
            } catch (Exception ignored) {
                // already stopped or failed
            }
        }
    }
}
