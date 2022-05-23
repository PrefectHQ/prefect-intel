from typing import Any
from prefect_intel.packaging.abc import PythonCallableDocument, PythonEnvironment


def run_in_environment(
    __environment: PythonEnvironment,
    __callabledoc: PythonCallableDocument,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    Run a callable object in the environment it requires.
    """
    from prefect_intel.packaging.worker import run_in_new_worker

    if __environment.is_active():
        # Run the code here since the environment is already active
        fn = __callabledoc.to_callable()
        return fn(*args, **kwargs)

    elif __environment.is_available():
        # Run the code in the environment
        return run_in_new_worker(
            __environment.python_command(),
            (__callabledoc.json(), args, kwargs),
            env=__environment.python_variables(),
        )

    elif __environment.manager_available():
        # Build the environment and run the code in it
        raise RuntimeError("Functions cannot yet be run in new environments.")
    else:
        raise RuntimeError(
            "The required environment does not exist and the tooling to create it is "
            "not available."
        )
