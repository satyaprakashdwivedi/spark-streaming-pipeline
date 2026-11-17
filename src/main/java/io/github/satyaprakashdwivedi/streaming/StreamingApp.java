package io.github.satyaprakashdwivedi.streaming;

import io.github.satyaprakashdwivedi.streaming.config.PipelineConfig;
import io.github.satyaprakashdwivedi.streaming.runtime.PipelineRunner;
import io.github.satyaprakashdwivedi.streaming.runtime.RunningPipeline;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Entry point.
 * <pre>
 * spark-submit --class io.github.satyaprakashdwivedi.streaming.StreamingApp app.jar pipeline.conf
 * java ... StreamingApp pipeline.conf --validate     # check the config only, no Spark
 * </pre>
 */
public final class StreamingApp {
    private StreamingApp() {}

    public static void main(String[] args) {
        List<String> argList = Arrays.asList(args);
        if (args.length < 1 || argList.contains("--help") || argList.contains("-h")) {
            System.err.println("Usage: StreamingApp <pipeline.conf> [--validate]");
            System.exit(args.length < 1 ? 2 : 0);
        }
        PipelineRunner runner = new PipelineRunner();
        PipelineConfig config;
        try {
            config = PipelineConfig.fromFile(args[0]);
            List<String> plan = runner.validate(config);
            System.out.println("Pipeline '" + config.appName() + "' is valid:\n  " + String.join("\n  ", plan));
        } catch (PipelineException e) {
            System.err.println("Invalid pipeline: " + e.getMessage());
            System.exit(2);
            return;
        }
        if (argList.contains("--validate")) {
            return;
        }

        SparkSession spark = runner.createSession(config);
        RunningPipeline running = null;
        int exitCode = 0;
        try {
            running = runner.start(spark, config);
            RunningPipeline toStop = running;
            Runtime.getRuntime().addShutdownHook(new Thread(toStop::stop, "pipeline-shutdown"));
            if (config.awaitTermination()) {
                running.awaitTermination();
            }
        } catch (Exception e) {
            System.err.println("Pipeline failed: " + e.getMessage());
            e.printStackTrace();
            exitCode = 1;
        } finally {
            if (running != null) {
                running.stop();
            }
            spark.stop();
        }
        System.exit(exitCode);
    }
}
