""" """

from __future__ import annotations
import dataclasses

from elasticsearch.client import EsqlClient
import run_transform


def query_outcomes(id_group: str, limit: int) -> str:
    return f"""
FROM op_testoutcomes
| LOOKUP JOIN op_testgroups ON id_group
| WHERE id_group == "{id_group}"
| KEEP t_name, t_outcome
| SORT t_name
| LIMIT {limit}
"""


QUERY_TESTRUNS = """
FROM op_testruns
| KEEP id_run
| SORT id_run DESC
| LIMIT 1000
"""


def query_testgroups(id_run: str) -> str:
    return f"""
FROM op_testgroups
| WHERE id_run == "{id_run}"
| KEEP id_group
| SORT id_group DESC
| LIMIT 1000
"""


def query_summary(id_run: str) -> str:
    return f"""
FROM op_testoutcomes
| WHERE id_run == "{id_run}"
| LOOKUP JOIN op_testgroups ON id_group
| EVAL is_passed = CASE(t_outcome == "passed", 1, 0),
        is_failed = CASE(t_outcome == "failed", 1, 0)
| STATS passed = SUM(is_passed), failed = SUM(is_failed) BY g_testgroup
| SORT g_testgroup
| LIMIT 1000
"""


def esql_rows(response_body: dict) -> list[dict]:
    """Convert ES|QL response to a list of dicts."""
    assert isinstance(response_body, dict)
    columns = [col["name"] for col in response_body["columns"]]
    return [dict(zip(columns, row)) for row in response_body["values"]]


@dataclasses.dataclass
class QueryOutcomes2:
    id_run: str
    limit: int = 100

    def query(self) -> str:
        return f"""
FROM op_testoutcomes
| WHERE id_run == "{self.id_run}"
| LOOKUP JOIN op_testgroups ON id_group
| STATS count = COUNT(*) BY id_group, g_testgroup, t_outcome_enum
| SORT id_group, t_outcome_enum
| LIMIT {self.limit}
"""

    def print(self, esql: EsqlClient) -> None:
        with run_transform.print_duration("QueryOutcomes2"):
            response = esql.query(query=self.query(), format="json")
        rows = esql_rows(response_body=response.body)
        last_id_group = ""
        for row in rows:
            if row["id_group"] != last_id_group:
                print()
                last_id_group = row["id_group"]
                print(f"{row['g_testgroup']}: ", end="")
            print(f"{row['t_outcome_enum']}={row['count']}, ", end="")

        print("\n---")

def main() -> None:
    client = run_transform.Elastic()

    def table(query: str):

        with run_transform.print_duration("query"):
            response = client.client.esql.query(query=query, format="txt")
        print(response)

    response = client.client.esql.query(query=QUERY_TESTRUNS, format="json")
    for row in response["values"]:
        id_run = row[0]
        print(f"*** query_summary({id_run=})")
        table(query_summary(id_run=id_run))

        print(f"*** query_outcomes2({id_run=})")
        q = QueryOutcomes2(id_run=id_run)
        q.print(client.client.esql)

        response2 = client.client.esql.query(
            query=query_testgroups(id_run=id_run), format="json"
        )
        for row2 in response2["values"]:
            id_group = row2[0]
            print(f"*** {id_run=} {id_group=}")
            table(query_outcomes(id_group=id_group, limit=5))
            break

        break

    client.close()


if __name__ == "__main__":
    main()
