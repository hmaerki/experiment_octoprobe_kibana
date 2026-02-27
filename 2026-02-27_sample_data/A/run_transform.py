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

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

PREFIX_RUN = "r_"
PREFIX_GROUP = "g_"
PREFIX_TEST = "t_"

ES_HOST = "localhost:9200"
ES_USER = "elastic"
ES_PASSWORD = "91AwngFy"
INDEX_NAME = "octoprobe_a"

ES_WRITE = True


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

        self.documents: list[dict] = []

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
        filename = self.directory_elastic / filename_json.relative_to(
            self.directory_reports
        )
        filename.parent.mkdir(parents=True, exist_ok=True)
        with filename.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=4, ensure_ascii=False)

        self.documents.append(data)

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
                dict_id=dict_id.copy(),
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

        outcomes = dict_group[PREFIX_GROUP + "outcomes"]
        del dict_group[PREFIX_GROUP + "outcomes"]

        dict_id["id_group"] = (
            f"{run_json[PREFIX_RUN + 'time_start']}/{dict_group[PREFIX_GROUP + 'testid']}"
        )
        dict_group.update(dict_id)
        self.write_json(filename_group, dict_group)

        for index, dict_outcome in enumerate(outcomes, start=1):
            dict_outcome = self.prefix(doc_json=dict_outcome, label=PREFIX_TEST)
            dict_outcome.update(dict_id)
            filename_outcome = directory_testgroup / f"testgroup_{index}.json"
            self.write_json(filename_outcome, dict_outcome)


def write_elastic(documents: list[dict]) -> None:
    if not ES_WRITE:
        return

    if not documents:
        return

    client = Elasticsearch(
        f"http://{ES_HOST}",
        basic_auth=(ES_USER, ES_PASSWORD),
    )

    # Delete existing index
    try:
        client.indices.delete(index=INDEX_NAME, ignore_unavailable=True)
        print(f"Deleted index: {INDEX_NAME}")
    except Exception as exc:
        print(f"Failed to delete index: {exc}")

    # Apply index template
    template_path = Path(__file__).parent / "create_template.json"
    template_name = f"{INDEX_NAME}_template"
    try:
        client.indices.delete_index_template(name=template_name)
    except Exception as exc:
        print(f"Failed to delete template: {exc}")

    try:
        client.indices.delete_index_template(name=INDEX_NAME)
    except Exception as exc:
        print(f"Failed to delete template: {exc}")

    try:        
        with template_path.open("r", encoding="utf-8") as f:
            template_body = json.load(f)
        client.indices.put_index_template(
            name=template_name,
            body=template_body,
        )
        print(f"Applied index template: {template_name}")
    except Exception as exc:
        print(f"Failed to apply template: {exc}")

    actions = ({"_index": INDEX_NAME, "_source": document} for document in documents)

    try:
        success, errors = bulk(
            client=client,
            actions=actions,
            raise_on_error=False,
            stats_only=False,
        )
    except Exception as exc:
        print(f"Elastic bulk write failed: {exc}")
        return

    failed = errors if isinstance(errors, int) else len(errors)

    print(f"Elastic upload {success}/{len(documents)}")
    if failed:
        print(f"Elastic upload failures: {failed}")


def main() -> None:
    directory_reports = Path(__file__).parent / "reports"
    directory_elastic = Path(__file__).parent / "elastic"
    if directory_elastic.exists():
        shutil.rmtree(directory_elastic)

    documents: list[dict] = []

    for directory_run in directory_reports.iterdir():
        if not directory_run.is_dir():
            continue
        testrun = Testgroup(
            directory_elastic=directory_elastic,
            directory_reports=directory_reports,
            directory_run=directory_run,
        )
        testrun.transform_run()
        documents.extend(testrun.documents)

    write_elastic(documents)


if __name__ == "__main__":
    main()
