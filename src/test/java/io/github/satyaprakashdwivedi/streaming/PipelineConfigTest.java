package io.github.satyaprakashdwivedi.streaming;

import io.github.satyaprakashdwivedi.streaming.config.PipelineConfig;
import io.github.satyaprakashdwivedi.streaming.runtime.PipelineRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Config parsing and validation: no Spark needed. */
class PipelineConfigTest {
    private final PipelineRunner runner = new PipelineRunner();

    private static PipelineConfig cfg(String steps) {
        return PipelineConfig.fromString("app { name = t, master = \"local[1]\" }\nsteps = [" + steps + "]");
    }

    @Test
    void parsesAppAndSteps() {
        PipelineConfig c = PipelineConfig.fromString(
                "app { name = orders, logLevel = ERROR, sparkConf { \"spark.sql.shuffle.partitions\" = 4 } }\n"
                        + "steps = [ { name = src, type = source.rate },"
                        + "          { name = out, type = sink.console } ]");
        assertEquals("orders", c.appName());
        assertEquals("ERROR", c.logLevel());
        assertEquals("4", c.sparkConf().get("spark.sql.shuffle.partitions"));
        assertEquals(2, c.steps().size());
        List<String> plan = runner.validate(c);
        assertEquals("out [sink.console] <- src", plan.get(1));
    }

    @Test
    void environmentSubstitutionWithDefault() {
        PipelineConfig c = PipelineConfig.fromString(
                "servers = \"localhost:9092\"\nservers = ${?PIPELINE_TEST_UNSET_VAR}\n"
                        + "steps = [ { type = source.kafka, options { bootstrapServers = ${servers}, subscribe = t } },"
                        + "          { type = sink.console } ]");
        assertEquals("localhost:9092", c.steps().get(0).string("bootstrapServers"));
    }

    @Test
    void unknownTypeListsAvailableOnes() {
        PipelineException e = assertThrows(PipelineException.class,
                () -> runner.validate(cfg("{ type = source.rate }, { type = transform.nope }, { type = sink.console }")));
        assertTrue(e.getMessage().contains("transform.nope"));
        assertTrue(e.getMessage().contains("transform.sql"));
    }

    @Test
    void firstStepMustBeASource() {
        PipelineException e = assertThrows(PipelineException.class,
                () -> runner.validate(cfg("{ type = transform.filter }, { type = sink.console }")));
        assertTrue(e.getMessage().contains("first step must be a source"));
    }

    @Test
    void needsASink() {
        PipelineException e = assertThrows(PipelineException.class,
                () -> runner.validate(cfg("{ type = source.rate }")));
        assertTrue(e.getMessage().contains("no sink"));
    }

    @Test
    void inputMustBeAnEarlierStep() {
        PipelineException e = assertThrows(PipelineException.class,
                () -> runner.validate(cfg("{ name = a, type = source.rate }, { type = sink.console, input = later }")));
        assertTrue(e.getMessage().contains("not an earlier step"));
    }

    @Test
    void cannotReadFromASink() {
        PipelineException e = assertThrows(PipelineException.class,
                () -> runner.validate(cfg("{ name = a, type = source.rate }, { name = s, type = sink.console },"
                        + "{ type = sink.console, input = s }")));
        assertTrue(e.getMessage().contains("sinks produce no data"));
    }

    @Test
    void duplicateNamesRejected() {
        assertThrows(PipelineException.class,
                () -> cfg("{ name = a, type = source.rate }, { name = a, type = sink.console }"));
    }

    @Test
    void missingOptionNamesTheStep() {
        PipelineConfig c = cfg("{ name = orders, type = source.kafka, options { subscribe = t } }, { type = sink.console }");
        PipelineException e = assertThrows(PipelineException.class,
                () -> c.steps().get(0).string("bootstrapServers"));
        assertEquals("Step 'orders' (source.kafka): missing required option 'bootstrapServers'", e.getMessage());
    }

    @Test
    void exampleConfigsAreValid() {
        for (String f : new String[] {"examples/rate-to-console.conf", "examples/kafka-json-enrich-to-kafka.conf",
                "examples/files-to-parquet.conf", "examples/kafka-to-postgres.conf"}) {
            List<String> plan = runner.validate(PipelineConfig.fromFile(f));
            assertTrue(plan.size() >= 2, f);
        }
    }
}
