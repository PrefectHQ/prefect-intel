import inspect
import shutil
import sys
import warnings
from functools import partial
from pathlib import Path
from types import FunctionType
from typing import Any, List, Literal, Optional, Tuple

import cloudpickle
from packaging.version import Version
from pydantic import BaseModel

from prefect.utilities.collections import AutoEnum
from prefect.flow_runners import python_version_micro


class PickleProtocol(AutoEnum):
    cloudpickle = AutoEnum.auto()


PICKLE_PROTOCOL_IMPLEMENTATIONS = {PickleProtocol.cloudpickle: cloudpickle}


class PyEnvironment(BaseModel):
    """
    Description of a Python runtime environment.
    """

    typename: Literal["bare"] = "bare"
    python_version: str
    requirements: List[str]

    def is_active(self) -> bool:
        """
        Returns a boolean indicating if the currently active Python is running in the
        described environment.
        """
        # TODO: Check for requirements as well?
        return python_version_micro() == self.python_version

    def is_available_on_machine(self) -> bool:
        """
        Returns a boolean indicating if the described environment is available on the
        current machine.
        """
        if self.is_active():
            return True
        # TODO: Check for requirements as well?
        return shutil.which(f"python{self.python_version}") is not None


class CondaEnvironment(PyEnvironment):
    typename: Literal["conda"] = "conda"
    name: str
    conda_requirements: List[str]

    # TODO: Implement active/available checks


class VenvEnvironment(PyEnvironment):
    typename: Literal["venv"] = "venv"
    name: str
    path: Path

    # TODO: Implement active/available checks


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


def package_function(
    fn: FunctionType, pickle_protocol: PickleProtocol = None
) -> PyFunctionDocument:

    # TODO: Detect environments other than "bare"

    document = partial(
        PyFunctionDocument,
        function_name=fn.__name__,
        environment=PyEnvironment(
            python_version=python_version_micro(),
            requirements=[],
        ),
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
