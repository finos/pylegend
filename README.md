[![FINOS - Incubating](https://cdn.jsdelivr.net/gh/finos/contrib-toolbox@master/images/badge-incubating.svg)](https://community.finos.org/docs/governance/Software-Projects/stages/incubating)
[![pypi](https://img.shields.io/pypi/v/pylegend.svg)](https://pypi.org/project/pylegend/)
[![python](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org/downloads)
![CI Testing](https://img.shields.io/badge/CI%20Testing-Linux%20%7C%20%20Windows%20-orange)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![Build](https://github.com/finos/pylegend/workflows/Build%20CI/badge.svg)
![CVE Scan](https://github.com/finos/pylegend/workflows/CVE%20Scan/badge.svg)
![License Scan](https://github.com/finos/pylegend/workflows/License%20Scan/badge.svg)
[![Codecov](https://codecov.io/gh/finos/pylegend/branch/main/graph/badge.svg)](https://app.codecov.io/gh/finos/pylegend)

# PyLegend

<b> -- Library is under active development -- </b>

PyLegend is part of [Legend](https://github.com/finos/legend) data management platform suite. It is a python client library which enables easy and streamlined integration of python clients with Legend ecosystem, providing interactive query building and push-down execution capabilities.

## Build from source

PyLegend requires Python 3.9 or higher. We use [Poetry](https://python-poetry.org/) tool for dependency management and packaging. To install poetry, follow instructions [here](https://python-poetry.org/docs/#installation).

Run `poetry install` to install dependencies. If you intend to contribute, install dev dependencies using the command `poetry install --with dev`.

## Contributing
For any questions, bugs or feature requests please open an [issue](https://github.com/finos/pylegend/issues).

To submit a contribution:
1. Fork it (<https://github.com/finos/pylegend/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Read our [contribution guidelines](./CONTRIBUTING.md) and [Community Code of Conduct](https://www.finos.org/code-of-conduct)
4. Commit your changes (`git commit -am 'Add some fooBar'`)
5. Push to the branch (`git push origin feature/fooBar`)
6. Create a new Pull Request

_NOTE:_ Commits and pull requests to FINOS repositories will only be accepted from those contributors with an active, executed Individual Contributor License Agreement (ICLA) with FINOS OR who are covered under an existing and active Corporate Contribution License Agreement (CCLA) executed with FINOS. Commits from individuals not covered under an ICLA or CCLA will be flagged and blocked by the FINOS Clabot tool (or [EasyCLA](https://community.finos.org/docs/governance/Software-Projects/easycla)). Please note that some CCLAs require individuals/employees to be explicitly named on the CCLA.

*Need an ICLA? Unsure if you are covered under an existing CCLA? Email [help@finos.org](mailto:help@finos.org)*

## License

Copyright 2023 Goldman Sachs

Distributed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

SPDX-License-Identifier: [Apache-2.0](https://spdx.org/licenses/Apache-2.0)
