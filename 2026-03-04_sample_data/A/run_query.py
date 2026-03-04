""" """

from __future__ import annotations

import run_transform


def query_outcomes(id_group: str, limit: int) -> str:
    return f"""
FROM op_testoutcomes
| LOOKUP JOIN op_testgroups ON id_group
| WHERE id_group == "{id_group}"
| KEEP name, outcome
| SORT name
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
| EVAL is_passed = CASE(outcome == "passed", 1, 0),
        is_failed = CASE(outcome == "failed", 1, 0)
| STATS passed = SUM(is_passed), failed = SUM(is_failed) BY testgroup
| SORT testgroup
| LIMIT 1000
"""


def main() -> None:
    client = run_transform.Elastic()

    def table(query: str):

        with run_transform.print_duration("query"):
            response = client.client.esql.query(query=query, format="txt")
        print(response)

    response = client.client.esql.query(query=QUERY_TESTRUNS, format="json")
    for row in response["values"]:
        id_run = row[0]
        print(f"*** {id_run=}")
        table(query_summary(id_run=id_run))

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
