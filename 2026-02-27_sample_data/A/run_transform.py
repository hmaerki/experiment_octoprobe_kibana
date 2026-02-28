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

import dataclasses
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

ID_DELIMITER = " | "


@dataclasses.dataclass(frozen=True)
class Document:
    id: str
    dict_doc: dict[str, Any]

    def __post_init__(self) -> None:
        assert isinstance(self.id, str), id
        assert self.id.find("/") == -1, f"id='{id}' should not contain a '/'!"


@dataclasses.dataclass(frozen=True)
class Testgroup:
    directory_elastic: Path
    directory_reports: Path
    directory_run: Path
    documents: list[Document] = dataclasses.field(default_factory=list)

    @staticmethod
    def prefix(doc_json: dict[str, Any], label: str) -> dict[str, Any]:
        return {f"{label}{k}": v for k, v in doc_json.items()}

    def read_json(self, path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def write_json(
        self,
        filename_json: Path,
        document: Document,
    ) -> None:
        assert isinstance(document, Document)

        filename = self.directory_elastic / filename_json.relative_to(
            self.directory_reports
        )
        filename.parent.mkdir(parents=True, exist_ok=True)
        with filename.open("w", encoding="utf-8") as handle:
            json.dump(document.dict_doc, handle, indent=4, ensure_ascii=False)

        self.documents.append(document)

    def transform_run(self) -> None:
        filename_run = self.directory_run / "context.json"
        if not filename_run.exists():
            raise FileNotFoundError(f"Missing testrun file: {filename_run}")

        dict_run = self.read_json(filename_run)
        dict_run = self.prefix(doc_json=dict_run, label=PREFIX_RUN)
        id_run = dict_run[PREFIX_RUN + "time_start"]
        # "id_run": "run_001",
        # "join_run_group": {
        #     "name": "run"
        # }
        dict_run["id_run"] = id_run
        dict_run["join_multiple"] = {"name": "run"}
        self.write_json(
            filename_json=filename_run,
            document=Document(id=id_run, dict_doc=dict_run),
        )

        for directory_testgroup in self.directory_run.iterdir():
            self.transform_group(
                directory_testgroup=directory_testgroup,
                run_json=dict_run,
                id_run=id_run,
            )

    def transform_group(
        self,
        directory_testgroup: Path,
        run_json: dict[str, Any],
        id_run: str,
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

        # id_group = f"{run_json[PREFIX_RUN + 'time_start']}_{dict_group[PREFIX_GROUP + 'testid']}"
        id_group = id_run + ID_DELIMITER + dict_group[PREFIX_GROUP + "testid"]
        dict_group["id_group"] = id_group
        # "join_run_group": {
        #     "name": "group",
        #     "parent": "run_001"
        # }
        dict_group["join_multiple"] = {"name": "group", "parent": id_run}
        self.write_json(
            filename_group, document=Document(id=id_group, dict_doc=dict_group)
        )

        for index, dict_outcome in enumerate(outcomes, start=1):
            test_name = dict_outcome["name"].replace("/", "-")
            id_test = id_group + ID_DELIMITER + test_name
            dict_outcome = self.prefix(doc_json=dict_outcome, label=PREFIX_TEST)
            # dict_outcome.update(dict_id)
            # dict_outcome["join_run_test"] = {"name": "test", "parent": id_run}
            dict_outcome["join_multiple"] = {"name": "test", "parent": id_group}
            filename_outcome = directory_testgroup / f"testgroup_{index}.json"
            self.write_json(
                filename_outcome, document=Document(id=id_test, dict_doc=dict_outcome)
            )


class Elastic:
    def __init__(self) -> None:
        if not ES_WRITE:
            return

        self.client = Elasticsearch(
            f"http://{ES_HOST}",
            basic_auth=(ES_USER, ES_PASSWORD),
        )
        self.template_name = "octoprobe_template"

    def close(self) -> None:
        self.client.close()

    def delete_index(self) -> None:
        if not ES_WRITE:
            return

        try:
            self.client.indices.delete(index=INDEX_NAME, ignore_unavailable=True)
            print(f"Deleted index: {INDEX_NAME}")
        except Exception as exc:
            print(f"Failed to delete index: {exc}")

    def delete_index_template(self) -> None:
        if not ES_WRITE:
            return

        try:
            self.client.indices.delete_index_template(name=self.template_name)
        except Exception as exc:
            print(f"Failed to delete template: {exc}")

        try:
            self.client.indices.delete_index_template(name=INDEX_NAME)
        except Exception as exc:
            # print(f"Failed to delete template: {exc}")
            pass

    def apply_index_template(self) -> None:
        template_path = Path(__file__).parent / "create_template.json"

        try:
            with template_path.open("r", encoding="utf-8") as f:
                template_body = json.load(f)
            self.client.indices.put_index_template(
                name=self.template_name,
                body=template_body,
            )
            print(f"Applied index template: {self.template_name}")
        except Exception as exc:
            print(f"Failed to apply template: {exc}")
            raise

    def write_documents_one_by_one(self, documents: list[Document]) -> None:
        if not ES_WRITE:
            return

        success = 0
        failed = 0
        for document in documents:
            assert isinstance(document, Document)
            try:
                # Extract routing from join field if it's a child document
                routing = None
                if "join_multiple" in document.dict_doc:
                    join_field = document.dict_doc["join_multiple"]
                    if isinstance(join_field, dict) and "parent" in join_field:
                        routing = join_field["parent"]
                
                self.client.index(
                    index=INDEX_NAME,
                    document=document.dict_doc,
                    id=document.id,
                    routing=routing,
                )
                success += 1
            except Exception as exc:
                failed += 1
                print(f"Failed to write document:\n{document}\n\n{exc}")

        print(f"Elastic upload {success}/{len(documents)}")
        if failed:
            print(f"Elastic upload failures: {failed}")

    def write_documents(self, documents: list[dict]) -> None:
        if not ES_WRITE:
            return

        for document in documents:
            assert isinstance(document, Document)

        actions = (
            {"_index": INDEX_NAME, "_source": document} for document in documents
        )

        try:
            success, errors = bulk(
                client=self.client,
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

    el = Elastic()
    el.delete_index()
    el.delete_index_template()
    el.apply_index_template()

    for directory_run in directory_reports.iterdir():
        if not directory_run.is_dir():
            continue
        testrun = Testgroup(
            directory_elastic=directory_elastic,
            directory_reports=directory_reports,
            directory_run=directory_run,
        )
        testrun.transform_run()
        el.write_documents_one_by_one(testrun.documents)

    el.close()


if __name__ == "__main__":
    main()
