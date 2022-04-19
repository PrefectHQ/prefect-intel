# prefect-intel


## Installation

Clone this repository and initialize the submodule:

```bash
git clone --recurse-submodules https://github.com/PrefectHQ/prefect-intel.git
```

Then, you can create the `prefect-intel` environment with `conda`:

```bash
conda env create --file conda-environment.yaml
```

Or install the `prefect` package into an existing environment with `pip`:

```bash
pip install -e ./prefect
```

## Upgrading

### Updating the prefect submodule

When the `prefect` upstream changes, you need to update the submodule to have the latest code:

```bash
git submodule update
```

Updating the submodule will automatically update the `prefect` Python module unless you did not use an editable install.

## Troubleshooting

### The `prefect` folder is empty

The repository was cloned without initializing the submodule, initalize it then follow the package installation instructions:

```bash
git submodule update --init
```

## Development

### Overview

This repository includes a `main` and a `prefect` branch. 

The `prefect` branch is used to track the private `PrefectHQ/orion` repository and is included as a submodule on the `main` branch. This allows updates to be pushed directly to this repository independently of releases to our public `PrefectHQ/prefect` repository.

The `main` branch includes examples, instructions, and Intel specific code extensions.

### Installation

Development dependencies will need to be installed:

```bash
pip install -e "prefect[dev]"
```

### Pull requests

Pull requests that modify the `prefect` library or submodule should be opened with the `prefect` branch as a base. Pull requests to the `prefect` branch will run the full Prefect test suite. To begin working on a change for the `prefect` library, you can create a branch based on the `prefect` branch. For example:

```bash
git checkout -b my-change prefect
```

Pull requests that update code in the `main` branch should be created with the `main` branch as a base. If the changes in the `main` branch requires unmerged changes to the `prefect` branch, you may checkout required branch while in the `prefect` directory then push a commit. If you look at the diff, you should see the commit the submodule points to has changed:

```
diff --git a/prefect b/prefect
index 64ce6a9..0ff5203 160000
--- a/prefect
+++ b/prefect
@@ -1 +1 @@
-Subproject commit 64ce6a95a3506738feaac28322fe367a291b14ac
+Subproject commit 0ff5203651880243b032577de0f4c2bf199d860a
```

If you see this in a diff on `main` while not making changes to the `prefect` submodule, you should ensure that the `prefect` submodule is on the `prefect` branch and on the latest commit.