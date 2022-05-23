from typing import Any, Callable
from prefect_intel.packaging.abc import DataDocument, PythonEnvironment


def run_in_environment(
    __environment: PythonEnvironment,
    __callabledoc: DataDocument[Callable],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    Run a callable object in the environment it requires.
    """

    if __environment.is_active():
        # Run the code here since the environment is already active
        fn = __callabledoc.decode()
        return fn(*args, **kwargs)

    elif __environment.is_available():
        from prefect_intel.packaging.worker import run_in_new_worker

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
