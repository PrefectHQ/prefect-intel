from prefect_intel.packaging import *
from pathlib import Path


def add(x, y):
    return x + y


if __name__ == "__main__":

    # Package `add` then run it via the packaged code

    for serializer in SerializerType.__members__:
        packaged = package(serializer, add)
        print(f"{serializer} package: {packaged!r}")

        result = run(packaged, 1, 2)
        print(f"Run result: {result!r}")

        print()
