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

PREFIX_RUN = "r_"
PREFIX_GROUP = "g_"
PREFIX_TEST = "t_"


class Testgroup:
    def __init__(
        self,
        directory_elastic: Path,
        directory_reports: Path,
        directory_run: Path,
    ) -> None:
        self.directory_elastic = directory_elastic
        self.directory_reports = directory_reports
        self.directory_run = directory_run

    @staticmethod
    def prefix(doc_json: dict[str, Any], label: str) -> dict[str, Any]:
        return {f"{label}{k}": v for k, v in doc_json.items()}

    def read_json(self, path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def write_json(
        self,
        filename_json: Path,
        data: dict[str, Any],
    ) -> None:
        filename = directory_elastic / filename_json.relative_to(directory_reports)
        filename.parent.mkdir(parents=True, exist_ok=True)
        with filename.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=4, ensure_ascii=False)

    def transform_run(self) -> None:
        filename_run = self.directory_run / "context.json"
        if not filename_run.exists():
            raise FileNotFoundError(f"Missing testrun file: {filename_run}")

        dict_run = self.read_json(filename_run)
        dict_run = self.prefix(doc_json=dict_run, label=PREFIX_RUN)
        dict_id = {"id_run": dict_run[PREFIX_RUN + "time_start"]}
        dict_run.update(dict_id)
        self.write_json(
            filename_json=filename_run,
            data=dict_run,
        )

        for directory_testgroup in self.directory_run.iterdir():
            self.transform_group(
                directory_testgroup=directory_testgroup,
                run_json=dict_run,
                dict_id=dict_id,
            )

    def transform_group(
        self,
        directory_testgroup: Path,
        run_json: dict[str, Any],
        dict_id: dict[str, str],
    ) -> None:
        if not directory_testgroup.is_dir():
            return
        filename_group = directory_testgroup / "context_testgroup.json"
        if not filename_group.is_file():
            return
        dict_group = self.read_json(filename_group)
        dict_group = self.prefix(doc_json=dict_group, label=PREFIX_GROUP)

        dict_id["id_group"] = (
            f"{run_json[PREFIX_RUN + 'time_start']}/{dict_group[PREFIX_GROUP + 'testid']}"
        )

        # transformed_group = dict(group_doc)
        # if "commandline" in transformed_group:
        #     transformed_group["group_commandline"] = transformed_group.pop(
        #         "commandline"
        #     )
        # if "time_start" in transformed_group:
        #     transformed_group["group_time_start"] = transformed_group.pop(
        #         "time_start"
        #     )
        # if "time_end" in transformed_group:
        #     transformed_group["group_time_end"] = transformed_group.pop("time_end")
        # transformed_group["test_run_id"] = testrun_time_start
        # transformed_group["test_group_id"] = test_group_id

        outcomes = dict_group[PREFIX_GROUP + "outcomes"]
        del dict_group[PREFIX_GROUP + "outcomes"]

        self.write_json(filename_group, dict_group)

        for index, dict_outcome in enumerate(outcomes, start=1):
            dict_outcome = self.prefix(doc_json=dict_outcome, label=PREFIX_TEST)
            dict_outcome.update(dict_id)
            filename_outcome = directory_testgroup / f"testgroup_{index}.json"
            self.write_json(filename_outcome, dict_outcome)


if __name__ == "__main__":
    directory_reports = Path(__file__).parent / "reports"
    directory_elastic = Path(__file__).parent / "elastic"
    if directory_elastic.exists():
        shutil.rmtree(directory_elastic)

    for directory_run in directory_reports.iterdir():
        if not directory_run.is_dir():
            continue
        testrun = Testgroup(
            directory_elastic=directory_elastic,
            directory_reports=directory_reports,
            directory_run=directory_run,
        )
        testrun.transform_run()
