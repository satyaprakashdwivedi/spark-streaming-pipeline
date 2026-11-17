package io.github.satyaprakashdwivedi.streaming.components.sinks;

import io.github.satyaprakashdwivedi.streaming.api.Sink;
import io.github.satyaprakashdwivedi.streaming.config.StepConfig;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * {@code sink.jdbc}: appends every micro-batch to a database table (PostgreSQL, MySQL, SQL Server, ...).
 * Put the JDBC driver jar on the classpath (spark-submit --jars or --packages).
 * <pre>
 * options { url = "jdbc:postgresql://db:5432/analytics", table = "public.orders",
 *           user = "${?DB_USER}", password = "${?DB_PASSWORD}", driver = "org.postgresql.Driver",
 *           checkpointLocation = "/chk/orders_jdbc", jdbcOptions { batchsize = 5000 } }
 * </pre>
 * At-least-once: a batch retried after a failure is written again, so make the table tolerate duplicates
 * (e.g. a unique key plus upsert in a view/trigger) when exactness matters.
 */
public class JdbcSink implements Sink {
    @Override
    public StreamingQuery start(SparkSession spark, Dataset<Row> input, StepConfig step) throws TimeoutException {
        String url = step.string("url");
        String table = step.string("table");
        Properties props = new Properties();
        step.optString("user").ifPresent(u -> props.setProperty("user", u));
        step.optString("password").ifPresent(p -> props.setProperty("password", p));
        step.optString("driver").ifPresent(d -> props.setProperty("driver", d));
        for (Map.Entry<String, String> e : step.map("jdbcOptions").entrySet()) {
            props.setProperty(e.getKey(), e.getValue());
        }
        VoidFunction2<Dataset<Row>, Long> writeBatch = (batch, batchId) ->
                batch.write().mode(SaveMode.Append).jdbc(url, table, props);
        return SinkSupport.common(input.writeStream(), step, true).foreachBatch(writeBatch).start();
    }
}
