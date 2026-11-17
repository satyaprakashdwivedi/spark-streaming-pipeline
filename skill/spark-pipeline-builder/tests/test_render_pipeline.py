"""Tests for render_pipeline.py: python3 -m unittest discover -s skill/spark-pipeline-builder/tests"""
import json
import os
import sys
import tempfile
import unittest

SKILL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(SKILL_DIR, "scripts"))
import render_pipeline as renderer  # noqa: E402


def steps_by_name(template):
    return {each["name"]: each for each in renderer.build_steps(template)}


class RenderPipelineTest(unittest.TestCase):
    def test_bundled_template_renders_for_both_engines(self):
        template = renderer.load_template(os.path.join(SKILL_DIR, "templates", "pipeline.yaml"))
        steps = renderer.build_steps(template)
        self.assertEqual(["raw_events", "events", "with_event_time", "filter_1", "columns_2", "lookup_3",
                          "quality_checked", "output", "quarantine_output"], [each["name"] for each in steps])
        hocon = renderer.to_hocon(template, steps)
        self.assertIn('"type" = "sink.kafka"', hocon)
        pyspark_yaml = renderer.to_pyspark_yaml(template, steps)
        self.assertIn("transform.quality", pyspark_yaml)

    def test_lookup_accepts_yaml_on_key_parsed_as_true(self):
        template = {"source": {"kind": "rate"}, "sink": {"kind": "console"},
                    "transform": [{"lookup": {"path": "c.csv", True: ["id"]}}]}
        self.assertEqual(["id"], steps_by_name(template)["lookup_1"]["options"]["on"])

    def test_environment_references_stay_unquoted_in_hocon(self):
        template = {"source": {"kind": "kafka", "servers": "${KAFKA}", "topic": "t", "payload": "raw"},
                    "sink": {"kind": "console"}}
        hocon = renderer.to_hocon(template, renderer.build_steps(template))
        self.assertIn('"bootstrapServers" = ${KAFKA}', hocon)

    def test_quarantine_sink_reads_the_quarantine_output(self):
        template = {"source": {"kind": "rate"}, "sink": {"kind": "console"},
                    "quality": {"rules": [{"column": "value", "check": "not_null"}]}}
        quarantine = steps_by_name(template)["quarantine_output"]
        self.assertEqual("quality_checked_quarantine", quarantine["input"])
        self.assertEqual("sink.console", quarantine["type"])

    def test_dedupe_requires_event_time(self):
        template = {"source": {"kind": "rate"}, "sink": {"kind": "console"}, "transform": [{"dedupe": ["id"]}]}
        with self.assertRaisesRegex(renderer.TemplateError, "event_time"):
            renderer.build_steps(template)

    def test_missing_checkpoint_is_reported(self):
        template = {"source": {"kind": "rate"}, "sink": {"kind": "file", "path": "/out"}}
        with self.assertRaisesRegex(renderer.TemplateError, "checkpoint"):
            renderer.build_steps(template)

    def test_json_template_and_cli_exit_codes(self):
        with tempfile.TemporaryDirectory() as folder:
            good = os.path.join(folder, "good.json")
            with open(good, "w") as handle:
                json.dump({"name": "demo", "source": {"kind": "rate"}, "sink": {"kind": "console"}}, handle)
            output = os.path.join(folder, "demo.conf")
            self.assertEqual(0, renderer.main([good, "--out", output]))
            with open(output) as handle:
                self.assertIn('name = "demo"', handle.read())

            bad = os.path.join(folder, "bad.json")
            with open(bad, "w") as handle:
                json.dump({"source": {"kind": "ftp"}, "sink": {"kind": "console"}}, handle)
            self.assertEqual(2, renderer.main([bad]))


if __name__ == "__main__":
    unittest.main()
