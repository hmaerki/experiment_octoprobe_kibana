import datetime
import pytest


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # Execute the actual test
    outcome = yield
    report = outcome.get_result()

    # We only want to log the result after the 'call' phase (the actual test execution)
    if report.when == "call":
        # Get the es_client fixture from the test session
        es = item.funcargs.get("es_client")

        if es:
            document = {
                "test_name": item.nodeid,
                "status": report.outcome,  # 'passed', 'failed', or 'skipped'
                "duration": report.duration,
                "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
                "node": item.config.getoption("host", "local-dev"),
            }

            try:
                es.index(index="pytest-results", document=document)
            except Exception as e:
                print(f"\nFailed to write to ES: {e}")


import warnings

from pytest_elasticsearch import factories

warnings.simplefilter("error", category=DeprecationWarning)


elasticsearch_proc2 = factories.elasticsearch_proc(port=9393)
elasticsearch_nooproc2 = factories.elasticsearch_noproc(port=9393)

elasticsearch2 = factories.elasticsearch("elasticsearch_proc2")
elasticsearch2_noop = factories.elasticsearch("elasticsearch_nooproc2")

# elasticsearch_octoprobe = factories.elasticsearch_noproc(host="localhost", port=9200)
# elasticsearch_octoprobe = factories.elasticsearch_proc(host="localhost", port=9200)
# elasticsearch = factories.elasticsearch("elasticsearch_nooproc")
# elasticsearch = factories.elasticsearch("elasticsearch_octoprobe")

# elasticsearch = factories.elasticsearch("elasticsearch_nooproc")

# elasticsearch_nooproc2 = elasticsearch_nooproc2(port=9200)
# elasticsearch = factories.elasticsearch("elasticsearch_nooproc2")

elasticsearch_auth = factories.elasticsearch_noproc(
    # host="localhost",
    # port=9200,
    # api_key="eVh3ZGtKd0JNc2JWczlKdGx3c0E6QWdJMXVsYzl4bDZOZkc2RmJTV3YxZw==",
    # basic_auth=("elastic", "91AwngFy"),
    # request_timeout=30.0,
    # verify_certs=False,
)
elasticsearch = factories.elasticsearch("elasticsearch_auth")

elasticsearch_proc_auth2 = factories.elasticsearch_proc()
elasticsearch_proc_auth = factories.elasticsearch("elasticsearch_proc_auth_2")

