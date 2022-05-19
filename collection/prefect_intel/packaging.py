import inspect
import shutil
import sys
import warnings
import json
import subprocess
import os
from functools import partial
from pathlib import Path
from types import FunctionType
from typing import Any, List, Literal, Optional, Tuple, Dict

import cloudpickle
from packaging.version import Version
from pydantic import BaseModel

from prefect.utilities.collections import AutoEnum
from prefect.flow_runners import python_version_minor


class PickleProtocol(AutoEnum):
    cloudpickle = AutoEnum.auto()


PICKLE_PROTOCOL_IMPLEMENTATIONS = {PickleProtocol.cloudpickle: cloudpickle}


class PyEnvironment(BaseModel):
    """
    Description of a Python runtime environment.
    """
    # TODO: Consider making this class abstract and moving bare implementation to a
    #       separate class

    typename: Literal["bare"] = "bare"
    python_version: str
    requirements: List[str]

    def is_active(self) -> bool:
        """
        Returns a boolean indicating if the currently active Python is running in the
        described environment.
        """
        # TODO: Check for requirements as well
        return python_version_minor() == self.python_version

    def is_available_on_machine(self) -> bool:
        """
        Returns a boolean indicating if the described environment is available on the
        current machine.
        """
        if self.is_active():
            return True
        # TODO: Check for requirements as well
        return shutil.which(f"python{self.python_version}") is not None

    def python_command(self) -> List[str]:
        """
        Return a command that can be used to run the environment's `python` executable.
        """
        return [f"python{self.python_version}"]

    def python_variables() -> Dict[str, str]:
        """
        Return environment variables needed to run the environment's `python` executable
        in a new process.
        """
        return os.environ.copy()


class CondaEnvironment(PyEnvironment):
    typename: Literal["conda"] = "conda"
    name: str = None
    path: Path = None
    conda_requirements: List[str]

    def is_active(self) -> bool:
        # TODO: Consider a more robust implementation that uses a subprocess call to
        #       `conda info --json`. The current implementation is much faster, though.
        if self.name:
            return sys.executable.endswith("{self.name}/bin/python")
        elif self.path:
            return sys.executable == str(self._resolved_path() / "bin" / "python")
        else:
            raise ValueError("Either `name` or `path` must be set.")

    def is_available_on_machine(self) -> bool:
        try:
            output = subprocess.check_output(["conda", "env", "list", "--json"])
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                "Failed to check for conda environments on machine."
            ) from exc

        env_paths = json.loads(output)["envs"]

        if self.name:
            env_names = {path.split(os.pathsep)[-1] for path in env_paths}
            return self.name in env_names
        elif self.path:
            return str(self._resolved_path()) in env_paths
        else:
            raise ValueError("Either `name` or `path` must be set.")

    def python_command(self) -> List[str]:
        command = ["conda", "run"]
        if self.path:
            command += ["--prefix", str(self.condaenv.expanduser().resolve())]
        elif self.name:
            command += ["--name", self.condaenv]
        else:
            raise ValueError("Either `name` or `path` must be set.")

        command += ["python"]

    def python_variables(self) -> Dict[str, str]:
        return os.environ.copy()

    def _resolved_path(self) -> Path:
        return self.path.expanduser().resolve()


class VenvEnvironment(PyEnvironment):
    typename: Literal["venv"] = "venv"
    path: Path

    def is_active(self) -> bool:
        return sys.executable == self._executable_path()

    def is_available_on_machine(self) -> bool:
        return os.path.exists(self._executable_path())

    def python_command(self) -> List[str]:
        return [str(self._executable_path())]

    def python_variables(self) -> Dict[str, str]:
        # This reproduces the relevant behavior of virtualenv's activation script
        # https://github.com/pypa/virtualenv/blob/main/src/virtualenv/activation/bash/activate.sh
        env = os.environ.copy()

        # Update the path to include the bin
        env["PATH"] = str(self._resolved_path() / "bin") + os.pathsep + env["PATH"]

        env.pop("PYTHONHOME", None)
        env["VIRTUAL_ENV"] = str(self._resolved_path())

        return env

    def _resolved_path(self) -> Path:
        return self.path.expanduser().resolve()

    def _executable_path(self) -> Path:
        return self._resolved_path() / "bin" / "python"


class PyFunctionDocument(BaseModel):
    """
    A serialized Python function and a description of the runtime environment it
    requires.

    The function may be serialized as source code or pickled.
    """

    content: bytes
    function_name: str

    environment: PyEnvironment

    pickle_protocol: Optional[PickleProtocol] = None
    pickle_version: str = None

    # TODO: Consider changing the name of `pickle_protocol` as there are Python internal
    #       pickle protocols with a different meaning


def run(
    __fn_document: PyFunctionDocument, *args: Any, **kwargs: Any
) -> Tuple[Any, BaseException]:
    if __fn_document.environment.is_active():
        # Run the code here since the environment is already active
        fn = unpackage_function(__fn_document)

        ret_val = ret_exc = None
        try:
            ret_val = fn(*args, **kwargs)
        except Exception as exc:
            ret_exc = exc

        return (ret_val, ret_exc)

    elif __fn_document.environment.is_available_on_machine():
        # Run the code in the environment
        raise RuntimeError("Functions cannot yet be run in inactive environments.")
    else:
        # Build the environment and run the code in it
        raise RuntimeError("Functions cannot yet be run in new environments.")


def detect_conda_environment() -> Optional[CondaEnvironment]:
    try:
        output = subprocess.check_output(["conda", "info", "--json"])
    except subprocess.CalledProcessError:
        # TODO: Consider parsing the exit code and output to display information about
        #       why detection failed
        return None

    parsed_output = json.loads(output)
    active_path = parsed_output["active_prefix"]
    active_name = parsed_output["active_prefix_name"]

    if sys.executable != str(Path(active_path) / "bin" / "python"):
        # this is the current conda environment, but it is not being used in this
        # python session
        return None

    # `name` takes precedence over `path` since it across machines better
    # TODO: Detect requirements
    if active_name:
        return CondaEnvironment(
            python_version=python_version_minor(),
            name=active_name,
            conda_requirements=[],
            requirements=[],
        )
    else:
        return CondaEnvironment(
            python_version=python_version_minor(),
            path=active_path,
            conda_requirements=[],
            requirements=[],
        )


def detect_venv_environment() -> Optional[VenvEnvironment]:
    # TODO: Implement venv detection
    return None

def detect_bare_environment() -> PyEnvironment:
    return PyEnvironment(
        python_version=python_version_minor(),
        requirements=[],
    )

def detect_environment() -> PyEnvironment:
    """
    Detect the current environment.
    """
    return (
        detect_conda_environment()
        or detect_venv_environment()
        or detect_bare_environment()
    )


def package_function(
    fn: FunctionType, pickle_protocol: PickleProtocol = None
) -> PyFunctionDocument:

    # TODO: Detect environments other than "bare"

    document = partial(
        PyFunctionDocument,
        function_name=fn.__name__,
        environment=detect_environment(),
    )

    if pickle_protocol:
        pickler = PICKLE_PROTOCOL_IMPLEMENTATIONS[pickle_protocol]

        content = pickler.dumps(fn)

        return document(
            content=content,
            pickle_protocol=pickle_protocol,
            pickle_version=getattr(pickler, "__version__"),
        )

    else:
        # Use `inspect` to retrieve the source code
        return document(content=inspect.getsource(fn).encode())


def unpackage_function(fn_document: PyFunctionDocument) -> FunctionType:
    if fn_document.pickle_protocol:
        # The content is the pickled function

        pickler = PICKLE_PROTOCOL_IMPLEMENTATIONS[fn_document.pickle_protocol]

        if (
            hasattr(pickler, "__version__")
            and pickler.__version__ != fn_document.pickle_version
        ):
            # TODO: Improve warning
            warnings.warn("Incompatible pickle version.")

        function = pickler.loads(fn_document.content)
        assert function.__name__ == fn_document.function_name

        return function

    else:
        # The content is source code and needs to be executed to extract the function

        exec_globals, exec_locals = {}, {}
        exec(fn_document.content, exec_globals, exec_locals)
        symbols = {**exec_globals, **exec_locals}

        return symbols[fn_document.function_name]
