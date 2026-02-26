import datetime
import pytest

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # Execute the actual test
    outcome = yield
    report = outcome.get_result()

    # We only want to log the result after the 'call' phase (the actual test execution)
    if report.when == 'call':
        # Get the es_client fixture from the test session
        es = item.funcargs.get('es_client')
        
        if es:
            document = {
                "test_name": item.nodeid,
                "status": report.outcome,  # 'passed', 'failed', or 'skipped'
                "duration": report.duration,
                "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
                "node": item.config.getoption("host", "local-dev")
            }
            
            try:
                es.index(index="pytest-results", document=document)
            except Exception as e:
                print(f"\nFailed to write to ES: {e}")
