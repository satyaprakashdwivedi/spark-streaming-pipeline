package io.github.satyaprakashdwivedi.streaming.components.sinks;

import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Map;

/**
 * Options every sink understands:
 * <pre>
 * queryName = orders_to_kafka
 * outputMode = append            # append | update | complete
 * checkpointLocation = "/chk/orders_to_kafka"   # required for every sink except console and memory
 * trigger { processingTime = "30 seconds" }     # or { availableNow = true } or { once = true }
 * writerOptions { ... }          # passed straight to the writer
 * </pre>
 */
final class SinkSupport {
    private SinkSupport() {}

    static DataStreamWriter<Row> common(DataStreamWriter<Row> writer, StepConfig step, boolean checkpointRequired) {
        writer = writer.queryName(step.string("queryName", step.name()))
                .outputMode(step.string("outputMode", "append"));
        if (step.has("checkpointLocation")) {
            writer = writer.option("checkpointLocation", step.string("checkpointLocation"));
        } else if (checkpointRequired) {
            throw step.error("option 'checkpointLocation' is required: it is how the query resumes exactly where it "
                    + "stopped after a restart (use a durable path such as HDFS/S3, one per query)");
        }
        writer = applyTrigger(writer, step);
        for (Map.Entry<String, String> e : step.map("writerOptions").entrySet()) {
            writer = writer.option(e.getKey(), e.getValue());
        }
        return writer;
    }

    static DataStreamWriter<Row> applyTrigger(DataStreamWriter<Row> writer, StepConfig step) {
        if (step.has("trigger.processingTime")) {
            return writer.trigger(Trigger.ProcessingTime(step.string("trigger.processingTime")));
        }
        if (step.bool("trigger.availableNow", false)) {
            return writer.trigger(Trigger.AvailableNow());
        }
        if (step.bool("trigger.once", false)) {
            return writer.trigger(Trigger.AvailableNow()); // Trigger.Once is deprecated; AvailableNow is its replacement
        }
        return writer;
    }
}
