import base64
import inspect
import json
from typing import Any

import cloudpickle
from prefect_intel.packaging.abc import Serializer
from prefect_intel.packaging.utilities import to_qualified_name, from_qualified_name


class PickleSerializer(Serializer):
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


class SourceSerializer(Serializer):
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


class ImportSerializer(Serializer):
    """
    Serializes objects by storing their importable path.
    """

    @staticmethod
    def dumps(obj: Any) -> bytes:
        return to_qualified_name(obj).encode()

    @staticmethod
    def loads(blob: bytes) -> Any:
        return from_qualified_name(blob.decode())
