import inspect
import shutil
import sys
import importlib
import json
import subprocess
import base64
import os
from pathlib import Path
from typing import Any, List, Literal, Optional, Tuple, Dict

import cloudpickle
from pydantic import BaseModel

from prefect.utilities.collections import AutoEnum
from prefect.flow_runners import python_version_minor


class SerializerType(AutoEnum):
    pickle = AutoEnum.auto()
    source = AutoEnum.auto()
    reference = AutoEnum.auto()


class PickleSerializer:
    """
    Serializes objects using the pickle protocol.

    Wraps `cloudpickle` to encode bytes in base64 for safe transmission.
    """

    # TODO: Include the cloudpickle version for incompatibility debugging purposes

    @staticmethod
    def dumps(obj: Any) -> bytes:
        blob = cloudpickle.dumps(obj)

        return base64.encodebytes(blob)

    @staticmethod
    def loads(blob: bytes) -> Any:
        return cloudpickle.loads(base64.decodebytes(blob))


class SourceSerializer:
    """
    Serializes objects by retrieving their source code.

    Creates a JSON blob with keys:
        source: The source code
        symbol_name: The name of the object to extract from the source code

    Deserialization requires the code to run with `exec`.
    """

    @staticmethod
    def dumps(obj: Any) -> bytes:
        return json.dumps(
            {
                "source": inspect.getsource(obj),
                "symbol_name": obj.__name__,
            }
        )

    @staticmethod
    def loads(blob: bytes) -> Any:
        document = json.loads(blob)
        if not isinstance(document, dict) or set(document.keys()) != {
            "source",
            "symbol_name",
        }:
            raise ValueError(
                "Invalid serialized data. "
                "Expected dictionary with keys 'source' and 'symbol_name'. "
                f"Got: {document}"
            )

        exec_globals, exec_locals = {}, {}
        exec(document["source"], exec_globals, exec_locals)
        symbols = {**exec_globals, **exec_locals}

        return symbols[document["symbol_name"]]


class ReferenceSerializer:
    """
    Serializes objects by storing their importable path.
    """

    @staticmethod
    def dumps(obj: Any) -> bytes:
        return (obj.__module__ + "." + obj.__qualname__).encode()

    @staticmethod
    def loads(blob: bytes) -> Any:
        name = blob.decode()

        # Try importing it first so we support "module" or "module.sub_module"
        try:
            module = importlib.import_module(name)
            return module
        except ImportError:
            # If no subitem was included raise the import error
            if "." not in name:
                raise

        # Otherwise, we'll try to load it as an attribute of a module
        mod_name, attr_name = name.rsplit(".", 1)
        module = importlib.import_module(mod_name)
        return getattr(module, attr_name)


SERIALIZER_IMPLEMENTATIONS = {
    SerializerType.source: SourceSerializer,
    SerializerType.pickle: PickleSerializer,
    SerializerType.reference: ReferenceSerializer,
}


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


class PyObjectDocument(BaseModel):
    """
    A serialized Python function and a description of the runtime environment it
    requires.

    The function may be serialized as source code or pickled.
    """

    content: bytes
    serializer: SerializerType
    environment: PyEnvironment


def run(
    __obj_document: PyObjectDocument, *args: Any, **kwargs: Any
) -> Tuple[Any, BaseException]:
    if __obj_document.environment.is_active():
        # Run the code here since the environment is already active
        fn = unpackage(__obj_document)

        ret_val = ret_exc = None
        try:
            ret_val = fn(*args, **kwargs)
        except Exception as exc:
            ret_exc = exc

        return (ret_val, ret_exc)

    elif __obj_document.environment.is_available_on_machine():
        # Run the code in the environment
        raise RuntimeError("Functions cannot yet be run in inactive environments.")
    else:
        # Build the environment and run the code in it
        raise RuntimeError("Functions cannot yet be run in new environments.")


def detect_conda_environment() -> Optional[CondaEnvironment]:
    try:
        output = subprocess.check_output(["conda", "env", "export", "--json"])
    except subprocess.CalledProcessError:
        # TODO: Consider parsing the exit code and output to display information about
        #       why detection failed
        return None

    parsed_output = json.loads(output)

    if "error" in parsed_output:
        raise RuntimeError(
            f"Failed to export the current conda environment: {parsed_output['error']}"
        )

    active_path = parsed_output["prefix"]
    active_name = parsed_output["name"]

    if sys.executable != str(Path(active_path) / "bin" / "python"):
        # this is the current conda environment, but it is not being used in this
        # python session
        return None

    # Parse the conda environment dependencies
    all_dependencies = parsed_output["dependencies"]
    conda_dependencies = []
    pip_dependencies = []
    for dependency in all_dependencies:
        if isinstance(dependency, dict):
            if "pip" in dependency:
                pip_dependencies.extend(dependency["pip"])
        else:
            conda_dependencies.append(dependency)

    # `name` takes precedence over `path` since it transports across machines better
    if active_name:
        return CondaEnvironment(
            python_version=python_version_minor(),
            name=active_name,
            conda_requirements=conda_dependencies,
            requirements=pip_dependencies,
        )
    else:
        return CondaEnvironment(
            python_version=python_version_minor(),
            path=active_path,
            conda_requirements=conda_dependencies,
            requirements=pip_dependencies,
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


def package(obj: Any, serializer_type: SerializerType) -> PyObjectDocument:
    serializer = SERIALIZER_IMPLEMENTATIONS[serializer_type]

    return PyObjectDocument(
        content=serializer.dumps(obj),
        serializer=serializer_type,
        environment=detect_environment(),
    )


def unpackage(obj_document: PyObjectDocument) -> Any:
    serializer = SERIALIZER_IMPLEMENTATIONS[obj_document.serializer]
    return serializer.loads(obj_document.content)


# TODO: Determine the best pattern for healthchecks of packaged objects
# def healthcheck(original: Any, packaged: PyObjectDocument) -> None:
#     try:
#         unpackaged = unpackage(packaged)
#     except Exception as exc:
#         ...
#     return {
#         "equality": original == unpackaged,
#     }
