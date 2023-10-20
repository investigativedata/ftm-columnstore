[![ftm-columnstore on pypi](https://img.shields.io/pypi/v/ftm-columnstore)](https://pypi.org/project/ftm-columnstore/) [![Python test and package](https://github.com/investigativedata/ftm-columnstore/actions/workflows/python.yml/badge.svg)](https://github.com/investigativedata/ftm-columnstore/actions/workflows/python.yml) [![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit) [![Coverage Status](https://coveralls.io/repos/github/investigativedata/ftm-columnstore/badge.svg?branch=main)](https://coveralls.io/github/investigativedata/ftm-columnstore?branch=main) [![GPL-3.0 License](https://img.shields.io/pypi/l/ftm-columnstore)](./LICENSE)


# ftm-columnstore

Column-store (based on [Clickhouse](https://clickhouse.com)) implementation for `nomenklatura` statement-based store for `ftm` entities.

**Minimum Python version: 3.11**

It is compatible as a store for [`ftmq`](https://github.com/investigativedata/ftmq)

## Usage

Set up a running clickhouse instance (pointed to via `DATABASE_URI` env var,
default: `localhost`), for developing purposes this could work:

    make clickhouse

Then initialize the required table schema:

    ftmcs init

Or drop existing data and recreate:

    ftmcs init --recreate

When using the `make clickhouse` command, you can play around with SQL queries
in your browser: http://127.0.0.1:8123/play

### Command-line usage

```bash
# Insert a bunch of FtM entities into a store:
cat ftm-entities.ijson | ftmcs write -d my_dataset
# Re-create the entities in aggregated form:
ftmcs iterate -d my_dataset | alephclient write-entities -f my_dataset
```
