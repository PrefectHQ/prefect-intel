import base64
import inspect
import json
from pathlib import Path
from typing import Any

import pydantic
from prefect.utilities.hashing import stable_hash
from prefect_intel.packaging.abc import Serializer
from prefect_intel.packaging.utilities import from_qualified_name, to_qualified_name


class PickleSerializer(Serializer):
    """
    Serializes objects using the pickle protocol.

    Wraps pickles in base64 for safe transmission.
    """

    picklelib: str = "cloudpickle"
    picklelib_version: str = None

    @pydantic.validator("picklelib")
    def check_picklelib(cls, value):
        """
        Check that the given pickle library is importable and has dumps/loads methods.
        """
        try:
            pickler = from_qualified_name(value)
        except (ImportError, AttributeError) as exc:
            raise ValueError(
                f"Failed to import requested pickle library: {value!r}."
            ) from exc

        if not hasattr(pickler, "dumps"):
            raise ValueError(
                f"Pickle library at {value!r} does not have a 'dumps' method."
            )

        if not hasattr(pickler, "loads"):
            raise ValueError(
                f"Pickle library at {value!r} does not have a 'loads' method."
            )

    @pydantic.root_validator
    def check_picklelib_version(cls, values):
        """
        Infers a default value for `picklelib_version` if null or ensures it matches
        the version retrieved from the `pickelib`.
        """
        picklelib = values["picklelib"]
        picklelib_version = values.get("picklelib_version")

        pickler = from_qualified_name(picklelib)
        pickler_version = getattr(pickler, "__version__")

        if not picklelib_version:
            values["picklelib_version"] = pickler_version
        elif picklelib_version != pickler_version:
            raise ValueError(
                f"Mismatched {picklelib!r} versions. Found {pickler_version} in the "
                f"environment but {picklelib_version} was requested."
            )

        return values

    def dumps(self, obj: Any) -> bytes:
        pickler = from_qualified_name(self.picklelib)
        blob = pickler.dumps(obj)

        return base64.encodebytes(blob)

    def loads(self, blob: bytes) -> Any:
        pickler = from_qualified_name(self.picklelib)
        return pickler.loads(base64.decodebytes(blob))


class SourceSerializer(Serializer):
    """
    Serializes objects by retrieving their source code.

    Creates a JSON blob with keys:
        source: The source code
        symbol_name: The name of the object to extract from the source code

    Deserialization requires the code to run with `exec`.
    """

    def dumps(self, obj: Any) -> bytes:
        return json.dumps(
            {
                "source": inspect.getsource(obj),
                "symbol_name": obj.__name__,
            }
        )

    def loads(self, blob: bytes) -> Any:
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


class ImportSerializer(Serializer):
    """
    Serializes objects by storing their importable path.
    """

    def dumps(self, obj: Any) -> bytes:
        return to_qualified_name(obj).encode()

    def loads(self, blob: bytes) -> Any:
        return from_qualified_name(blob.decode())


class FileSerializer(Serializer):
    """
    Serializes objects as bytes to a unique file path.
    """

    # TODO: This could use a block with read-write support for persistence
    # TODO: This implementation is a draft of composing serializers and may not be
    #       a final pattern

    basepath: Path = pydantic.Field(default_factory=Path("."))
    filesystemlib: str = "builtins"
    serializer: Serializer = PickleSerializer()

    @pydantic.validator("filesystemlib")
    def check_filesystemlib(cls, value):
        """
        Check that the given filesystem library is importable and has an 'open' method.
        """
        try:
            fs = from_qualified_name(value)
        except (ImportError, AttributeError) as exc:
            raise ValueError(
                f"Failed to import requested filesystem library: {value!r}."
            ) from exc

        if not hasattr(fs, "open"):
            raise ValueError(
                f"Filesystem library at {value!r} does not have a 'open' method."
            )

    def dumps(self, obj: Any) -> bytes:
        fs = from_qualified_name(self.filesystemlib)
        content = self.serializer.dumps(obj)
        key = stable_hash(content)
        path = self.basepath / key

        with fs.open(path, mode="wb") as file:
            file.write(content)

        return str(path).encode()

    def loads(self, blob: bytes) -> Any:
        fs = from_qualified_name(self.filesystemlib)
        path = blob.decode()

        with fs.open(path, mode="rb") as file:
            content = file.read()

        return self.serializer.loads(content)
