import base64
import importlib
import inspect
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import cloudpickle
from prefect.flow_runners import python_version_minor
from prefect.utilities.collections import AutoEnum
from prefect.utilities.hashing import hash_objects
from pydantic import BaseModel


def get_default_env_directory() -> Path:
    return Path(".") / "prefect-env"


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

    def is_available(self) -> bool:
        """
        Returns a boolean indicating if the described environment is available on the
        current machine.
        """
        if self.is_active():
            return True
        # TODO: Check for requirements as well
        return shutil.which(f"python{self.python_version}") is not None

    def manager_available(self) -> bool:
        """
        Returns a boolean indicating if the tool required for managing this environment
        is available on the current machine.
        """
        # We will not install Python versions on the machine
        return False

    def python_command(self) -> List[str]:
        """
        Return a command that can be used to run the environment's `python` executable.
        """
        return [f"python{self.python_version}"]

    def python_variables(self) -> Dict[str, str]:
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
    conda_executable: str = "conda"

    def is_active(self) -> bool:
        # TODO: Consider a more robust implementation that uses a subprocess call to
        #       `conda info --json`. The current implementation is much faster, though.
        if self.name:
            return sys.executable.endswith(f"{self.name}{os.sep}bin{os.sep}python")
        elif self.path:
            return sys.executable == str(self._resolved_path() / "bin" / "python")
        else:
            raise ValueError("Either `name` or `path` must be set.")

    def is_available(self) -> bool:
        try:
            output = subprocess.check_output(
                [self.conda_executable, "env", "list", "--json"]
            )
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                "Failed to check for conda environments on machine."
            ) from exc

        env_paths = json.loads(output)["envs"]

        if self.name:
            env_names = {path.split(os.sep)[-1] for path in env_paths}
            return self.name in env_names
        elif self.path:
            return str(self._resolved_path()) in env_paths
        else:
            raise ValueError("Either `name` or `path` must be set.")

    def manager_available(self) -> bool:
        """
        Returns a boolean indicating if the tool required for managing this environment
        is available on the current machine.
        """
        return shutil.which(self.conda_executable) is not None

    def python_command(self) -> List[str]:
        command = [self.conda_executable, "run"]
        if self.path:
            command += ["--prefix", str(self._resolved_path())]
        elif self.name:
            command += ["--name", self.name]
        else:
            raise ValueError("Either `name` or `path` must be set.")

        command += ["python"]
        return command

    def python_variables(self) -> Dict[str, str]:
        return os.environ.copy()

    def _resolved_path(self) -> Path:
        return self.path.expanduser().resolve()


class VenvEnvironment(PyEnvironment):
    typename: Literal["venv"] = "venv"
    path: Path

    def is_active(self) -> bool:
        return sys.executable == self._executable_path()

    def is_available(self) -> bool:
        return os.path.exists(self._executable_path())

    def manager_available(self) -> bool:
        return True

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


def parse_conda_dependencies(env_export: Any) -> Tuple[List[str], List[str]]:
    all_dependencies = env_export["dependencies"]
    conda_dependencies = []
    pip_dependencies = []
    for dependency in all_dependencies:
        if isinstance(dependency, dict):
            if "pip" in dependency:
                pip_dependencies.extend(dependency["pip"])
        else:
            conda_dependencies.append(dependency)
    return conda_dependencies, pip_dependencies


def create_venv_environment(
    *,
    requirements: List[str],
    python_executable: str = sys.executable,
    path: Path = None,
) -> VenvEnvironment:
    """
    Creates a venv environment with dependencies installed
    """
    if not shutil.which(python_executable):
        raise ValueError(f"Executable {python_executable} is not available.")

    environment_path = path or (
        get_default_env_directory() / hash_objects(requirements, python_executable)
    )
    create_command = [python_executable, "-m", "venv", str(environment_path)]
    subprocess.check_call(create_command)

    # Install packages
    subprocess.check_call(
        [
            str(environment_path / "bin" / "python"),
            "-m",
            "pip",
            "install",
        ]
        + requirements
    )

    # Retrieve the Python version
    python_version = python_version_from_executable(python_executable)

    return VenvEnvironment(
        path=environment_path, requirements=requirements, python_version=python_version
    )


def python_version_from_executable(executable: Path) -> str:
    """
    Returns the version of Python for the given Python executable.

    Creates a subprocess to check the version.

    Returns the version up to the minor.
    """
    # TODO: Consider refactoring this with a cleaner API for major/minor/micro levels

    output = subprocess.check_output([executable, "--version"])
    match = re.match(
        r"Python (?P<major>[0-9]+)\.(?P<minor>[0-9]+)\.(?P<micro>[0-9]+)",
        output.decode(),
    )
    if not match:
        raise ValueError(f"Failed to parse python version output: {output}")
    versions = match.groupdict()
    return f"{versions['major']}.{versions['minor']}"


def create_conda_environment(
    *,
    conda_requirements: List[str],
    requirements: List[str],
    python_version: str = None,
    base_path: Path = None,
    name: str = None,
    conda_executable: str = "conda",
) -> CondaEnvironment:
    """
    Creates a conda environment with dependencies installed.

    If `name` is not given, it will not be usable by `--name`, only `--prefix`.
    """
    if not shutil.which(conda_executable):
        raise RuntimeError(
            "Executable {conda_executable!r} is not available. Is conda installed?"
        )

    create_env_command = [
        conda_executable,
        "create",
        "--json",
        "--yes",
    ]

    if not name:
        base_path = base_path or get_default_env_directory()
        environment_path = base_path / hash_objects(
            requirements, python_version, conda_requirements, conda_executable
        )
        create_env_command.extend(["--prefix", str(environment_path)])
    else:
        if base_path:
            raise ValueError("You cannot specify both 'name' and 'base_path'.")
        create_env_command.extend(["--name", name])
        en

    if not python_version:
        # Specify a matching python version up to `minor`
        # We cannot match up to `micro` because it is not always available in conda
        v = sys.version_info
        python_version = f"{v.major}.{v.minor}"

    create_env_command.append(f"python={python_version}")
    create_env_command.extend(conda_requirements)

    print(f"Creating conda environment at {environment_path}")
    subprocess.check_call(create_env_command)

    subprocess.check_call(
        [
            conda_executable,
            "run",
            *(["--prefix", str(environment_path)] if not name else ["--name", name]),
            "pip",
            "install",
        ]
        + requirements
    )

    return CondaEnvironment(
        python_version=python_version,
        requirements=requirements,
        name=name,
        path=environment_path,
        conda_requirements=conda_requirements,
    )


def detect_conda_environment(
    conda_executable: str = "conda",
) -> Optional[CondaEnvironment]:
    try:
        output = subprocess.check_output([conda_executable, "env", "export", "--json"])
    except subprocess.CalledProcessError:
        # TODO: Consider parsing the exit code and output to display information about
        #       why detection failed
        return None

    parsed_output = json.loads(output)

    if "error" in parsed_output:
        # TODO: Consider including a warning here. 'error' should probably never be
        #       here without a bad exit code
        print(
            f"Failed to export the current conda environment: {parsed_output['error']}"
        )
        return None

    active_path = parsed_output["prefix"]
    active_name = parsed_output["name"]

    if sys.executable != str(Path(active_path) / "bin" / "python"):
        # This is the current conda environment, but it is not being used in this
        # python session
        return None

    conda_requirements, pip_requirements = parse_conda_dependencies(parsed_output)

    # `name` takes precedence over `path` since it transports across machines better
    if active_name:
        return CondaEnvironment(
            python_version=python_version_minor(),
            name=active_name,
            conda_requirements=conda_requirements,
            requirements=pip_requirements,
            conda_executable=conda_executable,
        )
    else:
        return CondaEnvironment(
            python_version=python_version_minor(),
            path=active_path,
            conda_requirements=conda_requirements,
            requirements=pip_requirements,
            conda_executable=conda_executable,
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

    elif __obj_document.environment.is_available():
        # Run the code in the environment
        raise RuntimeError("Functions cannot yet be run in inactive environments.")
    elif __obj_document.environment.manager_available():
        # Build the environment and run the code in it
        raise RuntimeError("Functions cannot yet be run in new environments.")
    else:
        raise RuntimeError(
            "The required environment does not exist and the tooling to create it is "
            "not available."
        )


# TODO: Determine the best pattern for healthchecks of packaged objects
# def healthcheck(original: Any, packaged: PyObjectDocument) -> None:
#     try:
#         unpackaged = unpackage(packaged)
#     except Exception as exc:
#         ...
#     return {
#         "equality": original == unpackaged,
#     }
