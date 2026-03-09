from __future__ import annotations

import os
import dataclasses
import json
import shutil
import pathlib
import typing
import urllib.request
import base64

from elasticsearch import Elasticsearch, helpers

PREFIX_RUN = "r_"
PREFIX_GROUP = "g_"
PREFIX_TEST = "t_"

ENV_PREFIX = "REMOTE_"
# ENV_PREFIX = "LOCAL_"
ES_HOST = os.environ[ENV_PREFIX + "ES_HOST"]
ES_USER = os.environ[ENV_PREFIX + "ES_USER"]
ES_PASSWORD = os.environ[ENV_PREFIX + "ES_PASSWORD"]
KIBANA_HOST = os.environ[ENV_PREFIX + "KIBANA_HOST"]

INDEX_NAME = "op_mp_index"

ES_WRITE = True
WRITE_JSON_FILES = True
INHERIT_PARENT_PROPERTIES = True
WRITE_JSON_TEST_GROUP = False
WRITE_BULK = True


ID_DELIMITER = " | "
JOIN_MULTIPLE = "id_join_multiple"
DO_JOIN_MULTIPLE = False
"""
2026-02-27_sample_data/A/create_template.json
    "id_join_multiple": {
        "type": "join",
        "relations": {
            "id_run": "id_group",
            "id_group": "id_test"
        }
    },
"""

DATAVIEW_TITLE = "op_mp_*"
DATAVIEW_BODY_DICT = {
    "data_view": {
        "title": DATAVIEW_TITLE,
        "name": "op_mp_*",
        "timeFieldName": "@timestamp",
    }
}


@dataclasses.dataclass(frozen=True)
class Document:
    prefix: str
    id_name: str
    id: str
    timestamp: str
    parent: typing.Self | None
    dict_doc: dict[str, str | int | dict]

    def __post_init__(self) -> None:
        assert isinstance(self.prefix, str), self.prefix
        assert isinstance(self.id_name, str), self.id_name
        assert isinstance(self.id, str), self.id
        assert self.id.find("/") == -1, f"id='{self.id}' should not contain a '/'!"
        assert isinstance(self.timestamp, str), self.timestamp
        assert isinstance(self.parent, Document | None), self.id
        assert isinstance(self.dict_doc, dict), self.dict_doc
        if self.parent is not None:
            assert self.id != self.parent.id, (
                f"Expected: {self.id=} != {self.parent.id=}"
            )

        self.apply_prefix()

        self.dict_doc["@timestamp"] = self.timestamp

        assert self.id_name not in self.dict_doc
        self.dict_doc[self.id_name] = self.id

        assert JOIN_MULTIPLE not in self.dict_doc
        if DO_JOIN_MULTIPLE:
            dict_join = {"name": self.id_name}
            if WRITE_JSON_TEST_GROUP:
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
            # self.dict_doc.update(parent.dict_doc)
            parent = parent.parent

    def apply_prefix(self) -> None:
        keys = list(self.dict_doc)
        for k in keys:
            self.dict_doc[self.prefix + k] = self.dict_doc[k]
        for k in keys:
            del self.dict_doc[k]


@dataclasses.dataclass(frozen=True)
class Testgroup:
    directory_elastic: pathlib.Path
    directory_reports: pathlib.Path
    directory_run: pathlib.Path
    documents: list[Document] = dataclasses.field(default_factory=list)

    def read_json(self, path: pathlib.Path) -> dict[str, typing.Any]:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def write_json(
        self,
        filename_json: pathlib.Path,
        document: Document,
    ) -> None:
        assert isinstance(document, Document)

        self.documents.append(document)

        if not WRITE_JSON_FILES:
            return

        filename = self.directory_elastic / filename_json.relative_to(
            self.directory_reports
        )
        filename.parent.mkdir(parents=True, exist_ok=True)
        with filename.open("w", encoding="utf-8") as handle:
            json.dump(document.dict_doc, handle, indent=4, ensure_ascii=False)

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
            timestamp=dict_run["time_start"],
            parent=None,
            dict_doc=dict_run,
        )
        if WRITE_JSON_TEST_GROUP:
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
        directory_testgroup: pathlib.Path,
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
        dict_group["msg_error_count_extension"] = 1 if "msg_error" != "" else 0
        dict_group["msg_skipped_count_extension"] = 1 if "msg_skipped" != "" else 0
        tentacle_variant = dict_group["tentacle_variant"]
        # tentacle_variant: "2d2d-LOLIN_D1_MINI-FLASH_512K"
        tentacle_list = tentacle_variant.split("-")
        dict_group["tentacle_serial_extension"] = tentacle_list[0]
        dict_group["tentacle_spec_extension"] = tentacle_list[1]
        dict_group["tentacle_firmware_variant_extension"] = (
            "" if len(tentacle_list) < 3 else tentacle_list[2]
        )
        id_group = id_run + ID_DELIMITER + dict_group["testid"]

        group_doc = Document(
            prefix=PREFIX_GROUP,
            id_name="id_group",
            id=id_group,
            timestamp=dict_group["time_start"],
            parent=run_doc,
            dict_doc=dict_group,
        )
        if WRITE_JSON_TEST_GROUP:
            self.write_json(filename_group, document=group_doc)

        for index, dict_outcome in enumerate(outcomes, start=1):
            test_name = dict_outcome["name"].replace("/", "-")
            id_test = id_group + ID_DELIMITER + test_name
            test_doc = Document(
                prefix=PREFIX_TEST,
                id_name="id_test",
                id=id_test,
                timestamp=group_doc.timestamp,
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
        self.template_name = "op_mp_template"

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

    def create_index(self) -> None:
        if not ES_WRITE:
            return

        try:
            self.client.indices.create(index=INDEX_NAME)
            print(f"Created index: {INDEX_NAME}")
        except Exception as exc:
            print(f"Failed to create index: {exc}")

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

    def create_index_template(self) -> None:
        template_path = pathlib.Path(__file__).parent / "create_template.json"

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

    def _kibana_request(
        self, path: str, *, method: str = "GET", data: bytes | None = None
    ) -> urllib.request.Request:
        req = urllib.request.Request(
            f"http://{KIBANA_HOST}{path}",
            data=data,
            headers={
                "Content-Type": "application/json",
                "kbn-xsrf": "true",
            },
            method=method,
        )
        credentials = base64.b64encode(f"{ES_USER}:{ES_PASSWORD}".encode()).decode()
        req.add_header("Authorization", f"Basic {credentials}")
        return req

    def delete_dataview(self) -> None:
        req = self._kibana_request("/api/data_views")
        with urllib.request.urlopen(req) as response:
            body = json.loads(response.read())
        for dv in body.get("data_view", []):
            if dv.get("title") == DATAVIEW_TITLE:
                dv_id = dv["id"]
                del_req = self._kibana_request(
                    f"/api/data_views/data_view/{dv_id}", method="DELETE"
                )
                with urllib.request.urlopen(del_req) as del_response:
                    print(
                        f"Deleted data view '{DATAVIEW_TITLE}' (id={dv_id}): {del_response.status}"
                    )

    def create_dataview(self) -> None:
        try:
            data = json.dumps(DATAVIEW_BODY_DICT).encode("utf-8")
            req = self._kibana_request(
                "/api/data_views/data_view", method="POST", data=data
            )
            with urllib.request.urlopen(req) as response:
                print(f"Created data view: {response.status}")
        except Exception as exc:
            print(f"Failed to create data view: {exc}")
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
                if DO_JOIN_MULTIPLE:
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
    directory_reports = pathlib.Path.cwd() / "reports"
    directory_elastic = pathlib.Path.cwd() / "elastic"
    assert directory_reports.is_dir(), directory_reports
    if directory_elastic.exists():
        shutil.rmtree(directory_elastic)

    el = Elastic()

    # Delete
    el.delete_dataview()
    el.delete_index_template()
    el.delete_index()

    # Create from scratch
    el.create_index_template()
    el.create_index()
    el.create_dataview()

    for directory_run in sorted(directory_reports.iterdir(), reverse=True):
        print(f"*** {directory_run}")
        if not directory_run.is_dir():
            continue
        testrun = Testgroup(
            directory_elastic=directory_elastic,
            directory_reports=directory_reports,
            directory_run=directory_run,
        )
        testrun.transform_run()
        if WRITE_BULK:
            el.write_documents_bulk(testrun.documents)
        else:
            el.write_documents_one_by_one(testrun.documents)

    el.close()


if __name__ == "__main__":
    main()
