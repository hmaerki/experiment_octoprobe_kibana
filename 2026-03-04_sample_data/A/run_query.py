""" """

from __future__ import annotations

import run_transform

QUERY_OUTCOMES = """
FROM test-outcomes
| LOOKUP JOIN testgroups ON testgroup_id
| WHERE testgroup == "RUN-PERFBENCH"
| KEEP test_name, tentacle, outcome
| SORT test_name, tentacle
| LIMIT 1000
"""

QUERY_TESTRUNS = """
FROM op_testruns
| KEEP id_run
| SORT id_run DESC
| LIMIT 1000
"""

def query_summary(id_run: str) -> str:
        return f"""
FROM op_testoutcomes
| LOOKUP JOIN op_testgroups ON id_group
| LOOKUP JOIN op_testruns ON id_run
| WHERE id_run == "{id_run}"
| EVAL is_passed = CASE(outcome == "passed", 1, 0),
        is_failed = CASE(outcome == "failed", 1, 0)
| STATS passed = SUM(is_passed), failed = SUM(is_failed) BY testgroup
| SORT testgroup
| LIMIT 1000
"""

def main() -> None:
    client = run_transform.Elastic()

    def table(query: str):

        response = client.client.esql.query(query=query, format="txt")
        print(response)

    response = client.client.esql.query(query=QUERY_TESTRUNS, format="json")
    for row in response["values"]:
        id_run = row[0]
        print(f"*** {id_run=}")
        table(query_summary(id_run=id_run))
    table(QUERY_OUTCOMES)

    client.close()


if __name__ == "__main__":
    main()
