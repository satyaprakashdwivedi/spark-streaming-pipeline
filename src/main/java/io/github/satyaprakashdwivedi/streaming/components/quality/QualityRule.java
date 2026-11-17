package io.github.satyaprakashdwivedi.streaming.components.quality;

import org.apache.spark.sql.Column;

import java.io.Serializable;

/** One row-level check: a name (reported when it fails) and a condition that is true for good rows. */
public final class QualityRule implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final Column passesWhen;

    public QualityRule(String name, Column passesWhen) {
        this.name = name;
        this.passesWhen = passesWhen;
    }

    public String name() { return name; }
    public Column passesWhen() { return passesWhen; }
}
