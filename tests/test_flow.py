from prefect import flow
from prefect.testing.utilities import prefect_test_harness
import pytest


@flow
def my_favorite_flow():
    return 42

@pytest.fixture(autouse=True, scope="session")
def test_my_favorite_flow():
  with prefect_test_harness():
      # run the flow against a temporary testing database
      assert my_favorite_flow() == 42 


