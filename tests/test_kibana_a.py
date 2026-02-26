# from pytest_elasticsearch import factories

# # # elasticsearch_my = factories.elasticsearch('elasticsearch_my_proc')
# elasticsearch_my = factories.elasticsearch('elasticsearch')


# def test_can_connect(elasticsearch_my):
#     assert elasticsearch_my.info()

import datetime

import pytest
import os
from elasticsearch import Elasticsearch


@pytest.fixture(scope="session")
def es_client() -> Elasticsearch:
    host = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
    password = os.getenv("ES_LOCAL_PASSWORD", "91AwngFy")
    client = Elasticsearch(host, basic_auth=("elastic", password))

    # Optional: Check connection before running tests
    if not client.ping():
        pytest.exit(f"Could not connect to Elasticsearch at {host}")

    return client


def test_es_connection(es_client: Elasticsearch):
    info = es_client.info()
    assert "cluster_name" in info

    document = {
        "test_name": "blurr",
        "status": "passed",  # 'passed', 'failed', or 'skipped'
        "duration": "42.2",
        "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        "node": "Salaminode",
    }

    es_client.index(index="pytest-results", id="4711", document=document)

def test_complex_logic(es_client: Elasticsearch):
    # ... your test logic ...
    result_data = {"accuracy": 0.98, "model": "v1"}
    
    # Write to ES
    response = es_client.index(
        index="manual-test-logs",
        document={
            "test_name": "test_complex_logic",
            "data": result_data,
            "timestamp": "2026-02-24T20:41:00Z"
        }
    )
    assert response['result'] == 'created'

def test_delete(es_client: Elasticsearch):
    es_client.indices.delete(index="pytest-results")
    es_client.indices.delete(index="manual-test-logs")
