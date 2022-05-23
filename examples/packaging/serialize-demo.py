from prefect_intel.packaging.abc import PythonCallableDocument
from prefect_intel.packaging.execution import run_in_environment
from prefect_intel.packaging.environments import detect_environment
from prefect_intel.packaging.serializers import (
    SourceSerializer,
    ImportSerializer,
    PickleSerializer,
    FileSerializer,
)


def add(x, y):
    return x + y


if __name__ == "__main__":

    # Package `add` then run it via the packaged code

    env = detect_environment()
    print(f"detected environment: {env!r}")

    for serializer in [
        SourceSerializer(),
        PickleSerializer(),
        ImportSerializer(),
        FileSerializer(serializer=PickleSerializer()),
    ]:
        print()

        calldoc = PythonCallableDocument.from_callable(add, serializer=serializer)
        print(f"{type(serializer).__name__} calldoc: {calldoc!r}")

        result = run_in_environment(env, calldoc, 1, 2)
        print(f"Run result: {result!r}")
