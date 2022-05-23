import abc
from typing import Any, Callable, Dict, Generic, List, Type, TypeVar, Union

import pydantic
from prefect_intel.packaging.utilities import from_qualified_name, to_qualified_name
from typing_extensions import Self

D = TypeVar("D", bound=Any)
M = TypeVar("M", bound=Type[pydantic.BaseModel])


def add_deserialization_dispatch(model_cls: M) -> M:
    """
    Extend a Pydantic model to add a field that includes the import path for the model
    on serialization and dynamically imports the model on deserialization. This allows
    automatic resolution to subtypes of the decorated model.
    """
    model_cls.__fields__["_dispatch_import_path"] = pydantic.fields.ModelField(
        name="_dispatch_import_path",
        type_=str,
        required=True,
        class_validators=None,
        model_config=model_cls.Config,
    )

    cls_init = model_cls.__init__
    cls_new = model_cls.__new__

    def __init__(__pydantic_self__, **data: Any) -> None:
        data.setdefault(
            "_dispatch_import_path", to_qualified_name(__pydantic_self__.__class__)
        )
        cls_init(__pydantic_self__, **data)

    def __new__(cls: Type[Self], **kwargs) -> Self:
        if "_dispatch_import_path" in kwargs:
            subcls = from_qualified_name(kwargs["_dispatch_import_path"])
            return cls_new(subcls)
        else:
            return cls_new(cls)

    model_cls.__init__ = __init__
    model_cls.__new__ = __new__

    return model_cls


@add_deserialization_dispatch
class Serializer(pydantic.BaseModel, Generic[D], abc.ABC):
    """
    A serializer that can encode objects of type 'D' into bytes.
    """

    def dumps(self, obj: D) -> bytes:
        pass

    def loads(self, blob: bytes) -> D:
        pass


@add_deserialization_dispatch
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
