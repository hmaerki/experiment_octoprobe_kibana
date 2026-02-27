"""
Resolve field name collisions:

Field	context.json	context_testgroup.json
commandline	Full path + args	run-natmodtests.py
time_start	2026-02-26_20-54-50-CET	2026-02-27_01-15-42-CET
time_end	2026-02-27_09-03-47-CET	2026-02-27_01-17-22-CET
log_output

Build the hierarchy:
* commit
* testrun
* testgroup
* testresult
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
	with path.open("r", encoding="utf-8") as handle:
		return json.load(handle)


def write_json(path: Path, data: dict[str, Any]) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	with path.open("w", encoding="utf-8") as handle:
		json.dump(data, handle, indent=4, ensure_ascii=False)
		handle.write("\n")


def transform_dataset(dataset_dir: Path) -> None:
	context_path = dataset_dir / "context.json"
	if not context_path.exists():
		raise FileNotFoundError(f"Missing testrun file: {context_path}")

	transformed_root = dataset_dir / "transformed"
	if transformed_root.exists():
		shutil.rmtree(transformed_root)

	source_group_paths = list(dataset_dir.rglob("context_testgroup.json"))

	testrun_doc = read_json(context_path)
	testrun_time_start = testrun_doc["time_start"]

	transformed_testrun = dict(testrun_doc)
	transformed_testrun["test_run_id"] = testrun_time_start
	write_json(transformed_root / "context.json", transformed_testrun)

	for group_path in source_group_paths:
		relative_group_path = group_path.relative_to(dataset_dir)
		group_doc = read_json(group_path)

		test_group_id = f"{testrun_time_start}_{group_doc['testid']}"

		transformed_group = dict(group_doc)
		if "commandline" in transformed_group:
			transformed_group["group_commandline"] = transformed_group.pop("commandline")
		if "time_start" in transformed_group:
			transformed_group["group_time_start"] = transformed_group.pop("time_start")
		if "time_end" in transformed_group:
			transformed_group["group_time_end"] = transformed_group.pop("time_end")
		transformed_group["test_run_id"] = testrun_time_start
		transformed_group["test_group_id"] = test_group_id

		outcomes = transformed_group.pop("outcomes", [])

		out_group_path = transformed_root / relative_group_path
		write_json(out_group_path, transformed_group)

		group_dir = out_group_path.parent
		for index, outcome in enumerate(outcomes, start=1):
			outcome_doc = dict(outcome)
			outcome_doc["test_group_id"] = test_group_id
			outcome_path = group_dir / f"context_testgroup_{index}.json"
			write_json(outcome_path, outcome_doc)


if __name__ == "__main__":
	base_dir = Path(__file__).resolve().parent
	dataset_a = base_dir
	transform_dataset(dataset_a)
	print(f"Transformed data written to: {dataset_a / 'transformed'}")

