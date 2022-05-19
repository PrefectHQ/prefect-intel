from prefect_intel.packaging import *
from pathlib import Path


def add(x, y):
    return x + y


if __name__ == "__main__":

    # Package `add` then run it via the packaged code

    for type_ in SerializerType.__members__:
        packaged = package(add, serializer_type=type_)
        print(f"{type_} package: {packaged!r}")

        result, exc = run(packaged, 1, 2)
        if result:
            print(f"Run result: {result!r}")
        if exc:
            print(f"Run exception: {exc!r}")

        print()

    # Create a new virtual environment

    print("Creating virtual environment...")
    virtual = create_venv_environment(requirements=["prefect"])

    print("Creating conda environment...")
    venv = create_conda_environment(
        requirements=["prefect"], conda_requirements=["sqlite"], python_version="3.10"
    )

    # Construct a document manually

    document = PyObjectDocument(
        content=b"prefect.hello_world.hello_flow",
        serializer="reference",
        environment=venv,
    )

    # Run the document in the environment

    # result, exc = run(document)
    # print(f"Run result: {result!r}")
    # print(f"Run exception: {exc!r}")
