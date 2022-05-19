from prefect_intel.packaging import package, unpackage


def add(x, y):
    return x + y


if __name__ == "__main__":
    packaged = package(add, serializer_type="pickle")
    print(f"Pickle package: {packaged!r}")
    unpackaged = unpackage(packaged)
    assert unpackaged(1, 2) == 3

    print()

    packaged = package(add, serializer_type="source")
    print(f"Script package: {packaged!r}")
    unpackaged = unpackage(packaged)
    assert unpackaged(1, 2) == 3

    print()

    packaged = package(add, serializer_type="reference")
    print(f"Reference package: {packaged!r}")
    unpackaged = unpackage(packaged)
    assert unpackaged(1, 2) == 3
