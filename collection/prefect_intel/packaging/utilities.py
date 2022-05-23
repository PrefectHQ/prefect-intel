import importlib
from typing import Any


def to_qualified_name(obj: Any) -> str:
    return obj.__module__ + "." + obj.__qualname__


def from_qualified_name(name: str) -> Any:
    # Try importing the path directly to support "module" or "module.sub_module"
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
