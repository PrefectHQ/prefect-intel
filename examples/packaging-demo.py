from prefect_intel.packaging import package_function, unpackage_function


def add(x, y):
    return x + y


if __name__ == "__main__":
    packaged = package_function(add, pickle_protocol="cloudpickle")
    print(f"Pickle package: {packaged!r}")
    unpackaged = unpackage_function(packaged)
    assert unpackaged(1, 2) == 3

    print()

    packaged = package_function(add)
    print(f"Script package: {packaged!r}")
    assert not packaged.pickle_protocol
    unpackaged = unpackage_function(packaged)
    assert unpackaged(1, 2) == 3
