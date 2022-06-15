import pytest
from prefect.testing.utilities import prefect_test_harness

from my_flows import my_favorite_flow

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield

def test_my_favorite_flow():
    assert my_favorite_flow().result() == 42
