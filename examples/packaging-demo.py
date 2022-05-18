from prefect_intel.packaging import package_function, unpackage_function
from pprint import pprint


def add(x, y):
    return x + y


if __name__ == "__main__":
    packaged = package_function(add, pickle_protocol="cloudpickle")
    pprint(packaged)
    unpackaged = unpackage_function(packaged)
    assert unpackaged(1, 2) == 3

    packaged = package_function(add)
    assert not packaged.pickle_protocol
    unpackaged = unpackage_function(packaged)
    assert unpackaged(1, 2) == 3
