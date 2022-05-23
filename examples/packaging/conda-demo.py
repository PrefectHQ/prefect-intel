from prefect_intel.packaging.abc import DataDocument
from prefect_intel.packaging.environments import create_conda_environment
from prefect_intel.packaging import run_in_environment
from prefect_intel.packaging.serializers import ImportSerializer

if __name__ == "__main__":
    # Create a new conda environment

    print("Creating conda environment...")
    conda = create_conda_environment(
        requirements=["requests"], conda_requirements=["sqlite"], python_version="3.10"
    )

    # Construct a document manually

    get_document = DataDocument(content=b"requests.get", serializer=ImportSerializer())

    # Run the document in the environment

    result = run_in_environment(conda, get_document, "http://google.com")
    print(f"Run result: {result!r}")
