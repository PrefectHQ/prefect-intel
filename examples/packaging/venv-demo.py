from prefect_intel.packaging import *


if __name__ == "__main__":
    # Create a new virtual environment

    print("Creating virtual environment...")
    virtual = create_venv_environment(requirements=["requests"])

    # Construct a document manually

    get_document = PyObjectDocument(
        content=b"requests.get",
        serializer="reference",
        environment=virtual,
    )

    # Run the document in the environment

    result = run(get_document, "http://google.com")
    print(f"Run result: {result!r}")
