import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import prefect_intel
from prefect.flow_runners import python_version_minor
from prefect.utilities.hashing import hash_objects
from prefect_intel.packaging.abc import PythonEnvironment


class BareEnvironment(PythonEnvironment):
    """
    Description of a Python runtime environment without any detected isolation.
    """

    def is_active(self) -> bool:
        # TODO: Check for requirements as well
        return python_version_minor() == self.python_version

    def is_available(self) -> bool:
        if self.is_active():
            return True
        # TODO: Check for requirements as well
        return shutil.which(f"python{self.python_version}") is not None

    def manager_available(self) -> bool:
        # We will not install Python versions on the machine
        return False

    def python_command(self) -> List[str]:
        return [f"python{self.python_version}"]

    def python_variables(self) -> Dict[str, str]:
        return os.environ.copy()


class CondaEnvironment(PythonEnvironment):
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


class VenvEnvironment(PythonEnvironment):
    path: Path

    def is_active(self) -> bool:
        return sys.executable == str(self._executable_path())

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


# Creation -----------------------------------------------------------------------------


def get_default_env_directory() -> Path:
    return Path(".") / "prefect-env"


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
        # TODO: This will only work for editable installs
        + [str(prefect_intel.__module_path__.parent)]
    )

    # Retrieve the Python version
    python_version = python_version_from_executable(python_executable)

    return VenvEnvironment(
        path=environment_path, requirements=requirements, python_version=python_version
    )


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

    if not python_version:
        # Specify a matching python version up to `minor`
        # We cannot match up to `micro` because it is not always available in conda
        v = sys.version_info
        python_version = f"{v.major}.{v.minor}"

    create_env_command.append(f"python={python_version}")
    create_env_command.extend(conda_requirements)

    print(f"Creating conda environment at {environment_path}")
    create_result_raw = subprocess.check_output(create_env_command)
    create_result = json.loads(create_result_raw)

    # TODO: Use a custom error type on environment creation failure, check the exit
    #       code manually instead of raising the `CalledProcessError` or `AssertionError`
    assert create_result["success"]
    if base_path:
        assert create_result["prefix"] == str(environment_path.resolve())

    subprocess.check_call(
        [
            conda_executable,
            "run",
            *(["--prefix", str(environment_path)] if not name else ["--name", name]),
            "python",
            "-m",
            "pip",
            "install",
        ]
        + requirements
        # TODO: This will only work for editable installs
        + [str(prefect_intel.__module_path__.parent)]
    )

    return CondaEnvironment(
        python_version=python_version,
        requirements=requirements,
        name=name,
        path=environment_path,
        conda_requirements=conda_requirements,
    )


# Detection ----------------------------------------------------------------------------


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


def detect_bare_environment() -> BareEnvironment:
    return BareEnvironment(
        python_version=python_version_minor(),
        requirements=[],
    )


def detect_environment() -> PythonEnvironment:
    """
    Detect the current environment.
    """
    return (
        detect_conda_environment()
        or detect_venv_environment()
        or detect_bare_environment()
    )
