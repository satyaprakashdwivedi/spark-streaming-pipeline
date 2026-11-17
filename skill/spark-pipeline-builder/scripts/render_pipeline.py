#!/usr/bin/env python3
"""Turn a filled pipeline template (templates/pipeline.yaml) into a full pipeline config.

    python3 render_pipeline.py my-pipeline.yaml --engine java    > my-pipeline.conf   (Java / HOCON)
    python3 render_pipeline.py my-pipeline.yaml --engine pyspark > my-pipeline.yaml   (PySpark / YAML)

The template is YAML (needs PyYAML: pip install pyyaml) or JSON. Values written as ${VAR} become environment
substitutions in the generated config, so secrets never land in the file.
Problems are reported as "template error: ..." with exit code 2.
"""
import argparse
import json
import re
import sys


class TemplateError(Exception):
    """A problem in the filled template, explained in plain words."""


def load_template(path):
    with open(path, encoding="utf-8") as template_file:
        text = template_file.read()
    if path.endswith(".json"):
        return json.loads(text)
    try:
        import yaml
    except ImportError:
        raise TemplateError("reading YAML needs PyYAML: pip install pyyaml (or pass a .json template)")
    return yaml.safe_load(text)


# --------------------------------------------------------------------------------------------- template -> steps

def build_steps(template):
    """Return the pipeline as an ordered list of steps: {name, type, input?, options}."""
    steps = []
    steps.extend(source_steps(template.get("source") or require(template, "source")))
    last_step = steps[-1]["name"]

    event_time = template.get("event_time")
    if event_time:
        steps.append(step("with_event_time", "transform.watermark", {
            "column": require(event_time, "column", "event_time"),
            "delay": event_time.get("allowed_lateness", "10 minutes"),
        }))
        last_step = "with_event_time"

    for position, item in enumerate(template.get("transform") or [], start=1):
        new_step = transform_step(item, position, previous_step=last_step, has_event_time=bool(event_time))
        steps.append(new_step)
        last_step = new_step["name"]

    quality = template.get("quality")
    if quality:
        steps.append(quality_step(quality))
        last_step = "quality_checked"

    sinks = template.get("sinks") or [template.get("sink") or require(template, "sink")]
    for position, sink in enumerate(sinks, start=1):
        steps.append(sink_step(sink, name="output" if len(sinks) == 1 else f"output_{position}", input_step=last_step))

    if quality and quality.get("on_failure", "quarantine") == "quarantine":
        quarantine_sink = quality.get("quarantine_sink") or {"kind": "console"}
        steps.append(sink_step(quarantine_sink, name="quarantine_output", input_step="quality_checked_quarantine"))
    return steps


def source_steps(source):
    kind = require(source, "kind", "source")
    if kind == "kafka":
        options = {
            "bootstrapServers": require(source, "servers", "source"),
            ("subscribePattern" if source.get("pattern") else "subscribe"): require(source, "topic", "source"),
            "startingOffsets": source.get("starting_offsets", "latest"),
        }
        if source.get("max_events_per_batch"):
            options["maxOffsetsPerTrigger"] = source["max_events_per_batch"]
        if source.get("kafka_options"):
            options["kafka"] = source["kafka_options"]
        steps = [step("raw_events", "source.kafka", options)]
        if source.get("payload", "json") == "json":
            steps.append(step("events", "transform.json", {
                "schema": require(source, "schema", "source (payload: json)"),
                "dropMalformed": source.get("drop_malformed", True),
            }))
        return steps
    if kind == "file":
        options = {"path": require(source, "path", "source"), "format": source.get("file_format", "json")}
        if source.get("schema"):
            options["schema"] = source["schema"]
        if source.get("reader_options"):
            options["readerOptions"] = source["reader_options"]
        return [step("events", "source.file", options)]
    if kind == "rate":
        return [step("events", "source.rate", {"rowsPerSecond": source.get("rows_per_second", 5)})]
    raise TemplateError(f"source kind must be kafka, file or rate (got '{kind}')")


def transform_step(item, position, previous_step, has_event_time):
    if not isinstance(item, dict) or len(item) != 1:
        raise TemplateError(f"transform item #{position} must be a single key such as 'filter: ...' (got {item!r})")
    action, value = next(iter(item.items()))
    name = f"{action}_{position}"
    if action == "filter":
        return step(name, "transform.filter", {"condition": value})
    if action == "sql":
        return step(name, "transform.sql", {"query": value, "view": "input"}, input_step=previous_step)
    if action == "columns":
        return step(name, "transform.withColumns", {"columns": value})
    if action == "select":
        return step(name, "transform.select", {"expressions": value})
    if action == "lookup":
        options = {
            "format": value.get("format", "parquet"),
            "path": require(value, "path", f"lookup #{position}"),
            "on": join_keys(value, position),
            "joinType": value.get("join_type", "left"),
        }
        if value.get("select"):
            options["select"] = value["select"]
        if value.get("header"):
            options["readerOptions"] = {"header": True}
        return step(name, "transform.lookup", options)
    if action == "dbt":
        options = {"projectDir": require(value, "project", f"dbt #{position}"),
                   "model": require(value, "model", f"dbt #{position}"), "view": "input"}
        for template_key, option_key in (("target", "target"), ("profiles_dir", "profilesDir"),
                                         ("compiled_sql", "compiledSqlPath"), ("vars", "vars"),
                                         ("dbt_executable", "dbtExecutable")):
            if value.get(template_key):
                options[option_key] = value[template_key]
        return step(name, "transform.dbt", options, input_step=previous_step)
    if action == "dedupe":
        if not has_event_time:
            raise TemplateError("dedupe needs an 'event_time' section, or its state would grow forever")
        return step(name, "transform.dropDuplicates", {"columns": value if isinstance(value, list) else [value]})
    raise TemplateError(f"unknown transform '{action}' in item #{position}; "
                        "use filter, sql, columns, select, lookup, dbt or dedupe")


def join_keys(lookup, position):
    # YAML 1.1 reads a bare `on:` key as the boolean True, so accept that spelling as well as join_on
    keys = lookup.get("join_on") or lookup.get("on") or lookup.get(True)
    if not keys:
        raise TemplateError(f"lookup #{position} is missing 'join_on' (the key columns to join on)")
    return keys


def quality_step(quality):
    options = {"onFailure": quality.get("on_failure", "quarantine")}
    if quality.get("rules"):
        options["rules"] = quality["rules"]
    if quality.get("suite"):
        options["expectationSuite"] = quality["suite"]
    if "rules" not in options and "expectationSuite" not in options:
        raise TemplateError("quality needs 'rules' or a Great Expectations 'suite'")
    return step("quality_checked", "transform.quality", options)


def sink_step(sink, name, input_step):
    kind = require(sink, "kind", "sink")
    options = {}
    if kind == "kafka":
        options.update(bootstrapServers=require(sink, "servers", "sink"), topic=require(sink, "topic", "sink"))
        if sink.get("key_column"):
            options["keyColumn"] = sink["key_column"]
    elif kind == "file":
        options.update(path=require(sink, "path", "sink"), format=sink.get("file_format", "parquet"))
        if sink.get("partition_by"):
            options["partitionBy"] = sink["partition_by"]
    elif kind == "jdbc":
        for key in ("url", "table"):
            options[key] = require(sink, key, "sink")
        for key in ("user", "password", "driver"):
            if sink.get(key):
                options[key] = sink[key]
    elif kind != "console":
        raise TemplateError(f"sink kind must be kafka, file, jdbc or console (got '{kind}')")

    if kind != "console":
        options["checkpointLocation"] = require(sink, "checkpoint", f"sink '{name}' ({kind})")
    trigger = sink.get("trigger")
    if trigger == "available_now":
        options["trigger"] = {"availableNow": True}
    elif trigger:
        options["trigger"] = {"processingTime": trigger}
    if sink.get("output_mode"):
        options["outputMode"] = sink["output_mode"]
    return step(name, f"sink.{kind}", options, input_step=input_step)


def step(name, step_type, options, input_step=None):
    new_step = {"name": name, "type": step_type}
    if input_step:
        new_step["input"] = input_step
    new_step["options"] = options
    return new_step


def require(section, key, where="template"):
    if not isinstance(section, dict) or section.get(key) in (None, ""):
        raise TemplateError(f"{where} is missing '{key}'")
    return section[key]


# --------------------------------------------------------------------------------------------- writers

ENV_REFERENCE = re.compile(r"^\$\{(\w+)\}$")


def to_hocon(template, steps):
    lines = ["# Generated by skill/spark-pipeline-builder from a filled template.",
             "app {", f"  name = {json.dumps(template.get('name', 'pipeline'))}",
             f"  logLevel = {json.dumps(template.get('log_level', 'WARN'))}", "}", "", "steps = ["]
    for each_step in steps:
        lines.append("  " + hocon_value(each_step, indent=2))
    lines.append("]")
    return "\n".join(lines) + "\n"


def hocon_value(value, indent):
    pad = " " * indent
    if isinstance(value, dict):
        inner = [f"{pad}  {json.dumps(str(key))} = {hocon_value(item, indent + 2)}" for key, item in value.items()]
        return "{\n" + "\n".join(inner) + f"\n{pad}}}"
    if isinstance(value, list):
        return "[" + ", ".join(hocon_value(item, indent) for item in value) + "]"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return json.dumps(value)
    environment_variable = ENV_REFERENCE.match(str(value))
    if environment_variable:
        return "${" + environment_variable.group(1) + "}"
    return json.dumps(str(value))


def to_pyspark_yaml(template, steps):
    document = {"app": {"name": template.get("name", "pipeline"), "log_level": template.get("log_level", "WARN")},
                "steps": steps}
    header = "# Generated by skill/spark-pipeline-builder from a filled template.\n"
    try:
        import yaml
        return header + yaml.safe_dump(document, sort_keys=False, width=120)
    except ImportError:
        return header + json.dumps(document, indent=2) + "\n"  # JSON is valid YAML


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("template", help="filled template (.yaml or .json)")
    parser.add_argument("--engine", choices=["java", "pyspark"], default="java",
                        help="java -> HOCON for spark-streaming-pipeline, pyspark -> YAML for the PySpark repo")
    parser.add_argument("--out", help="write here instead of stdout")
    arguments = parser.parse_args(argv)
    try:
        template = load_template(arguments.template)
        steps = build_steps(template)
    except (TemplateError, OSError, ValueError) as problem:
        print(f"template error: {problem}", file=sys.stderr)
        return 2
    rendered = to_hocon(template, steps) if arguments.engine == "java" else to_pyspark_yaml(template, steps)
    if arguments.out:
        with open(arguments.out, "w", encoding="utf-8") as output_file:
            output_file.write(rendered)
        print(f"wrote {arguments.out} ({len(steps)} steps)", file=sys.stderr)
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    sys.exit(main())
