# py-template
Template repository for Python projects at Aarhus Stadsarkiv.

## Instructions
This repository includes default configuration files. A few changes needs to be made in some of them, specifically regarding **module name** and **author information**.

#### pyproject.toml
In `pyproject.toml`, `name` and `authors` should be changed:
```toml
[tool.poetry]
name = "YOUR_MODULE_NAME"
version = "0.1.0"
description = ""
authors = ["YOUR NAME <YOUR@EMAIL>"]
maintainers = ["Aarhus Stadsarkiv <stadsarkiv@aarhus.dk>"]
license = "GPL-3.0"
readme = "README.md"
homepage = "https://www.aarhusstadsarkiv.dk/"
```
`name` refers to your module name. Remember to use underscores instead of dashes when naming your module!

#### mypy.ini
In `mypy.ini`, the per module options should be changed to reflect your module name:
```ini
# Per module options
[mypy-YOUR_MODULE_NAME.*]
disallow_untyped_defs = True

```

#### GitHub Actions
This repository includes a simple GitHub Actions workflow that sets up `poetry` with caching and checks linting & types using `flake8`, `black`, and `mypy`.
It also includes the initial setup for testing and upload to [codecov](https://codecov.io/) as a commented block. This part of the workflow requires adding a [codecov token](https://docs.codecov.io/docs#section-getting-started) as a [GitHub secret](https://help.github.com/en/actions/configuring-and-managing-workflows/creating-and-storing-encrypted-secrets). In addition, the call to `pytest` must be updated with the relevant project name.

#### And finally...
Remember to change this `README` to suit your repository!
