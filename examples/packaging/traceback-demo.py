from prefect_intel.packaging.abc import PythonCallableDocument
from prefect_intel.packaging.environments import create_venv_environment
from prefect_intel.packaging.execution import run_in_environment
from prefect_intel.packaging.serializers import ImportSerializer

if __name__ == "__main__":
    # Create a new virtual environment

    print("Creating virtual environment...")
    virtual = create_venv_environment(requirements=["requests"])

    # Construct a document manually with a non-existant attribute

    get_document = PythonCallableDocument(
        content=b"requests.does_not_exist", serializer=ImportSerializer()
    )

    # Run the document in the environment

    result = run_in_environment(virtual, get_document, "http://google.com")
    print(f"Run result: {result!r}")
