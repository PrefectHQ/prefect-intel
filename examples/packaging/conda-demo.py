from prefect_intel.packaging import *


if __name__ == "__main__":
    # Create a new conda environment

    print("Creating conda environment...")
    conda = create_conda_environment(
        requirements=["requests"], conda_requirements=["sqlite"], python_version="3.10"
    )

    # Construct a document manually

    get_document = PyObjectDocument(
        content=b"requests.get",
        serializer="reference",
        environment=conda,
    )

    # Run the document in the environment

    result = run(get_document, "http://google.com")
    print(f"Run result: {result!r}")
