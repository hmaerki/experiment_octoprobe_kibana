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
import typing

from elasticsearch import Elasticsearch, helpers

PREFIX_RUN = "r_"
PREFIX_GROUP = "g_"
PREFIX_TEST = "t_"

ES_HOST = "localhost:9200"
ES_USER = "elastic"
ES_PASSWORD = "91AwngFy"
INDEX_NAME = "octoprobe_a"

ES_WRITE = True
INHERIT_PARENT_PROPERTIES = True

ID_DELIMITER = " | "
JOIN_MULTIPLE = "id_join_multiple"


@dataclasses.dataclass(frozen=True)
class Document:
    prefix: str
    id_name: str
    id: str
    parent: typing.Self | None
    dict_doc: dict[str, str | int | dict]

    def __post_init__(self) -> None:
        assert isinstance(self.prefix, str), self.prefix
        assert isinstance(self.id_name, str), self.id_name
        assert isinstance(self.id, str), self.id
        assert self.id.find("/") == -1, f"id='{self.id}' should not contain a '/'!"
        assert isinstance(self.parent, Document | None), self.id
        assert isinstance(self.dict_doc, dict), self.dict_doc
        if self.parent is not None:
            assert self.id != self.parent.id, (
                f"Expected: {self.id=} != {self.parent.id=}"
            )

        self.apply_prefix()

        assert self.id_name not in self.dict_doc
        self.dict_doc[self.id_name] = self.id

        assert JOIN_MULTIPLE not in self.dict_doc
        dict_join = {"name": self.id_name}
        if self.parent is not None:
            dict_join["parent"] = self.parent.id
        self.dict_doc[JOIN_MULTIPLE] = dict_join

        if not INHERIT_PARENT_PROPERTIES:
            return

        parent = self.parent
        while parent is not None:
            for k, v in parent.dict_doc.items():
                if k.startswith(parent.prefix):
                    self.dict_doc[k] = v
            self.dict_doc.update(parent.dict_doc)
            parent = parent.parent

    def apply_prefix(self) -> None:
        keys = list(self.dict_doc)
        for k in keys:
            self.dict_doc[self.prefix + k] = self.dict_doc[k]
        for k in keys:
            del self.dict_doc[k]


@dataclasses.dataclass(frozen=True)
class Testgroup:
    directory_elastic: Path
    directory_reports: Path
    directory_run: Path
    documents: list[Document] = dataclasses.field(default_factory=list)

    def read_json(self, path: Path) -> dict[str, typing.Any]:
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
        id_run = dict_run["time_start"]
        run_doc = Document(
            prefix=PREFIX_RUN,
            id_name="id_run",
            id=id_run,
            parent=None,
            dict_doc=dict_run,
        )
        self.write_json(
            filename_json=filename_run,
            document=run_doc,
        )

        for directory_testgroup in self.directory_run.iterdir():
            self.transform_group(
                directory_testgroup=directory_testgroup,
                run_doc=run_doc,
                id_run=id_run,
            )

    def transform_group(
        self,
        directory_testgroup: Path,
        run_doc: Document,
        id_run: str,
    ) -> None:
        if not directory_testgroup.is_dir():
            return
        filename_group = directory_testgroup / "context_testgroup.json"
        if not filename_group.is_file():
            return
        dict_group = self.read_json(filename_group)
        outcomes = dict_group["outcomes"]
        del dict_group["outcomes"]
        id_group = id_run + ID_DELIMITER + dict_group["testid"]

        group_doc = Document(
            prefix=PREFIX_GROUP,
            id_name="id_group",
            id=id_group,
            parent=run_doc,
            dict_doc=dict_group,
        )
        self.write_json(filename_group, document=group_doc)

        for index, dict_outcome in enumerate(outcomes, start=1):
            test_name = dict_outcome["name"].replace("/", "-")
            id_test = id_group + ID_DELIMITER + test_name
            test_doc = Document(
                prefix=PREFIX_TEST,
                id_name="id_test",
                id=id_test,
                parent=group_doc,
                dict_doc=dict_outcome,
            )
            self.write_json(
                filename_json=directory_testgroup / f"testgroup_{index}.json",
                document=test_doc,
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
                routing = None
                if document.parent is not None:
                    routing = document.parent.id
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

    def write_documents_bulk(self, documents: list[Document]) -> None:
        if not ES_WRITE:
            return

        actions = (
            {"_index": INDEX_NAME, "_source": document.dict_doc, "_id": document.id}
            for document in documents
        )

        try:
            success, errors = helpers.bulk(
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
        el.write_documents_bulk(testrun.documents)
        # el.write_documents_one_by_one(testrun.documents)

    el.close()


if __name__ == "__main__":
    main()
