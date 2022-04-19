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

```
git submodule update
```

Updating the submodule will automatically update the `prefect` Python module unless you did not use an editable install.

## Troubleshooting

### The `prefect` folder is empty

The repository was cloned without initializing the submodule, run `git submodule update --init`.