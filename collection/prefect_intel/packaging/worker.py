import base64
import os
import subprocess
import sys
import traceback
from typing import Any, List, Tuple

import cloudpickle
import prefect_intel
from prefect_intel.packaging.abc import DataDocument
from prefect_intel.packaging.serializers import PickleSerializer


# TODO: This pattern is not relevant if we store qualified paths on pydantic models
# Dynamically register any user provided objects by importing
REGISTER_PATHS = os.environ.get("PREFECT_REGISTER_PATHS")
if REGISTER_PATHS:
    for path in REGISTER_PATHS:
        prefect_intel.packaging.serializers.ImportSerializer.loads(path.encode())


def pickle_exception(exc: BaseException) -> bytes:
    """
    Pickles an exception _and_ its traceback into a tuple because Python will drop
    tracebacks on pickle.
    """
    return cloudpickle.dumps((exc, traceback.TracebackException.from_exception(exc)))


class PickleError(Exception):
    """
    Describes an error that occured during pickling of an object.
    """

    def __init__(
        self, message: str = None, exception: BaseException = None, obj: Any = None
    ) -> None:
        # Workaround for deserialization just passing the 'message'
        # TODO: Consider improving this interface
        if obj and exception:
            generated_message = (
                f"Pickle of type {type(obj).__name__!r} failed with exception: {exception}.\n"
                f"Object: {obj!r}\n"
            )
        else:
            generated_message = "Pickle failed. No information was included."

        super().__init__(message or generated_message)


def run_in_new_worker(python_command: List[str], call: Tuple, **kwargs: Any) -> str:
    """
    Run a call in a new worker subprocess.
    """
    # TODO: The `call` argument should not be a bare tuple.
    # TODO: We may be able to reuse documents for transport here instead of using
    #       cloudpickle directly. We're duplicating the base64 implementation. We do
    #       not need all the environment information though.
    request = DataDocument.encode(call, serializer=PickleSerializer()).json()
    command = python_command + ["-m", __name__, request]
    return handle_worker_response(subprocess.check_output(command, **kwargs))


def handle_worker_response(response: str) -> Any:
    """
    Parse the response from a worker which is composed of a status and encoded result.

    Exceptions in workers will be re-raised.
    """
    try:
        status, encoded_result = response.strip().split(b"\n", maxsplit=1)
    except Exception as exc:
        raise RuntimeError(f"Malformed worker response: {response}") from exc

    try:
        result = cloudpickle.loads(base64.decodebytes(encoded_result))
    except Exception as exc:
        raise RuntimeError(f"Malformed result payload.") from exc

    if status == b"EXCEPTION":
        result: Tuple[Exception, traceback.TracebackException]
        exc, tb = result

        # TODO: Determine how to attach the traceback so it is only printed when the
        #       exception is uncaught by the caller
        for line in tb.format():
            print(line, end="")

        raise exc

    elif status == b"RETURN":
        return result
    else:
        raise RuntimeError(f"Unknown worker status {status!r} in response: {response}")


def handle_worker_request(request: bytes):
    """
    Execute the given request and return its status and encoded result.
    """
    stdout = sys.stdout
    sys.stdout = open(os.devnull, "w")

    retval = exception = None
    try:
        callabledoc_json, args, kwargs = DataDocument.parse_raw(request).decode()
        func = DataDocument.parse_raw(callabledoc_json).decode()
    except BaseException as exc:
        exception = exc
    else:
        try:
            retval = func(*args, **kwargs)
        except BaseException as exc:
            exception = exc

    try:
        if exception is not None:
            status = b"EXCEPTION"
            result = pickle_exception(exception)
        else:
            status = b"RETURN"
            result = cloudpickle.dumps(retval)
    except BaseException as exc:
        # Handle data that cannot be pickled
        exception = PickleError(exception=exc, obj=exception or retval)
        status = b"EXCEPTION"
        result = pickle_exception(exception)

    # TODO: We sent a data document to the worker but we always return a cloudpickle
    #       that is composed piecewise. This is required for better pickling of
    #       exceptions, but we should consider making this interaction symmetric.

    # Return the response
    stdout.buffer.write(status)
    stdout.buffer.write(b"\n")
    stdout.buffer.write(base64.encodebytes(result))
    stdout.buffer.flush()


if __name__ == "__main__":
    """Entrypoint for worker calls"""
    # Pass a bad input if not present so the exception will be pickled and sent back
    # to the caller of the worker
    handle_worker_request(sys.argv[1].encode() if len(sys.argv) > 1 else None)
