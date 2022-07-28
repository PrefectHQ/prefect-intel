from prefect import flow
import pytest
from prefect.testing.utilities import prefect_test_harness

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield

@flow
def my_favorite_flow():
    return 42

def test_my_favorite_flow():
    assert my_favorite_flow() == 42