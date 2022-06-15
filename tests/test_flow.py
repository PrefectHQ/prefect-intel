from prefect import flow
from prefect.testing.utilities import prefect_test_harness

import pytest

@flow
def my_favorite_flow():
    return 42

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield

def test_my_favorite_flow():
    assert my_favorite_flow().result() == 42
