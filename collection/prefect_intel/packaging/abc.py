import abc
from typing import Any, Callable, Dict, Generic, List, Type, TypeVar, Union

import pydantic
from prefect_intel.packaging.utilities import from_qualified_name, to_qualified_name
from typing_extensions import Self

D = TypeVar("D", bound=Any)


class Serializer(pydantic.BaseModel, Generic[D], abc.ABC):
    """
    A serializer that can encode objects of type 'D' into bytes.
    """

    # TODO: Consider allowing additional data to be sent with the serializer, making
    #       the following methods non-static

    @abc.abstractstaticmethod
    def dumps(obj: D) -> bytes:
        pass

    @abc.abstractstaticmethod
    def loads(blob: bytes) -> D:
        pass

    # Deserialization dispatch ---------------------------------------------------------

    class_import_path: str

    def __init__(__pydantic_self__, **data: Any) -> None:
        data.setdefault(
            "class_import_path", to_qualified_name(__pydantic_self__.__class__)
        )
        super().__init__(**data)

    def __new__(cls: Type[Self], **kwargs) -> Self:
        if "class_import_path" in kwargs:
            subcls = from_qualified_name(kwargs["class_import_path"])
            return super().__new__(subcls)
        else:
            return super().__new__(cls)


class PythonEnvironment(pydantic.BaseModel, abc.ABC):
    """
    Description of a Python runtime environment.
    """

    python_version: str
    requirements: List[str]

    @abc.abstractmethod
    def is_active(self) -> bool:
        """
        Returns a boolean indicating if the currently active Python is running in the
        described environment.
        """

    @abc.abstractmethod
    def is_available(self) -> bool:
        """
        Returns a boolean indicating if the described environment is available on the
        current machine.
        """

    @abc.abstractmethod
    def manager_available(self) -> bool:
        """
        Returns a boolean indicating if the tool required for managing this environment
        is available on the current machine.
        """

    @abc.abstractmethod
    def python_command(self) -> List[str]:
        """
        Return a command that can be used to run the environment's `python` executable.
        """

    @abc.abstractmethod
    def python_variables(self) -> Dict[str, str]:
        """
        Return environment variables needed to run the environment's `python` executable
        in a new process.
        """

    # Deserialization dispatch ---------------------------------------------------------

    class_import_path: str

    def __init__(__pydantic_self__, **data: Any) -> None:
        data.setdefault(
            "class_import_path", to_qualified_name(__pydantic_self__.__class__)
        )
        super().__init__(**data)

    def __new__(cls: Type[Self], **kwargs) -> Self:
        if "class_import_path" in kwargs:
            subcls = from_qualified_name(kwargs["class_import_path"])
            return super().__new__(subcls)
        else:
            return super().__new__(cls)


class PythonCallableDocument(pydantic.BaseModel):
    """
    A serialized Python object that can be called.
    """

    content: bytes
    serializer: Serializer

    def to_callable(self) -> Callable:
        return self.serializer.loads(self.content)

    @classmethod
    def from_callable(
        cls, function: Callable, serializer: Serializer
    ) -> "PythonCallableDocument":
        return cls(
            content=serializer.dumps(function),
            serializer=serializer,
        )
