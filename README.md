# ftm-columnstore

This library provides methods to store, fetch and list entities formatted as
[`followthemoney`](https://github.com/alephdata/followthemoney) data as
datasets stored in a column store backend using
[clickhouse](https://clickhouse.com/)

This roughly follows the functionality and features from
[followthemoney-store](https://github.com/alephdata/followthemoney-store) but
with a huge performance benefit on writing and querying data.

`FtM` data is stored in one table in [statements](#statements) format.

## Usage

Set up a running clickhouse instance (pointed to via `DATABASE_URI` env var,
default: `localhost`), for developing purposes this could work:

    make clickhouse

Then initialize the required table schema:

    ftm cstore init

Or drop existing data and recreate:

    ftm cstore init --recreate

To test if it's working, run a raw query:

    ftm cstore query "SHOW TABLES"

When using the `make clickhouse` command, you can play around with SQL queries
in your browser: http://127.0.0.1:8123/play

### Command-line usage

(`ftm store` becomes `ftm cstore`)

```bash
# Insert a bunch of FtM entities into a store:
cat ftm-entities.ijson | ftm cstore write -d my_dataset
# Re-create the entities in aggregated form:
ftm cstore iterate -d my_dataset | alephclient write-entities -f my_dataset
```

```
Usage: ftm cstore [OPTIONS] COMMAND [ARGS]...

  Store FollowTheMoney object data in a column store (Clickhouse)

Options:
  --log-level TEXT  Set logging level  [default: info]
  --uri TEXT        Database connection URI  [default: localhost]
  --table TEXT      Database table  [default: ftm]
  --help            Show this message and exit.

Commands:
  delete        Delete dataset or complete store
  fingerprints  Generate fingerprint statements as csv from json entities...
  flatten       Turn json entities from `infile` into statements in csv...
  init          Initialize database and table.
  iterate       Iterate entities
  list          List datasets in a store
  query         Execute raw query and print result (csv format) to outfile
  statements    Dump all statements as csv
  write         Write json entities from `infile` to store.
```

#### statements

Under the hood, `FtM` entities are converted to statements, which is possible
via the command line too:

    cat entities.ijson | ftm cstore flatten > statements.csv

#### fingerprints

Additionally write fingerprints for entity props with type
[`name`](https://followthemoney.readthedocs.io/en/latest/types.html#name) to a
dedicated fingerprint index, usable for very fast lookups:

    cat entities.ijson | ftm cstore write -d my_dataset --fingerprints


### Python Library

```python
from ftm_columnstore import Dataset

dataset = Dataset("US-OFAC")
dataset.put(entity)

entity = dataset.get("entity-id")
```

Bulk writer behaves the same like in [`followthemoney-store`](https://github.com/alephdata/followthemoney):

```python
from ftm_columnstore import Dataset

dataset = Dataset("US-OFAC")
bulk = dataset.bulk(with_fingerprints=True)

for entity in many_entities:
  bulk.put(entity)
bulk.flush()
```

#### Querying entities

There is some weird and unintuitive stuff going on building these queries as
turning the statements back into `FtM` entities is a bit hacky here, but from
top-level, it feels quite nice:

```python
from ftm_columnstore.query import EntityQuery

q = EntityQuery().where(schema="Person")
# queries are always streaming result iterator
entities = [e for e in q]

# querying for properties:
q = EntityQuery().where(schema="Payment", amount__gte=1000)
```


## benchmarks

[OpenSanctions](https://opensanctions.org) dataset

**tl;dr** For a small dataset, the original `followthemoney-store`
implementation is faster, as the columnstore implementation has the statements
overhead. Benefits come from the explicit querying features.

```bash
curl -s https://data.opensanctions.org/datasets/latest/all/entities.ftm.json\?`date '+%s'` > opensanctions.ftm.ijson
```

All tests run on the same Dell XPS Laptop, 16 GB, 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz

### write

`ftm-columnstore`: 99s

```bash
date +"%H:%M:%S" ; ftm cstore write -d opensanctions -i opensanctions.ftm.ijson ; date +"%H:%M:%S"
15:44:51
ftm_columnstore.dataset [INFO] [opensanctions] Write: 100000 entities with 532100 statements.
# ...
ftm_columnstore.dataset [INFO] [opensanctions] Write: 536131 entities with 5565069 statements.
15:46:10
```

`followthemoney-store` (postgres): 50s

```bash
date +"%H:%M:%S" ; ftm store write -d opensanctions -i opensanctions.ftm.ijson ; date +"%H:%M:%S"
15:46:55
ftmstore [INFO] Write [opensanctions]: 10000 entities
# ...
ftmstore [INFO] Write [opensanctions]: 530000 entities
15:47:45
```

### iterate

`followthemoney-store` (postgres): 46s

```bash
date +"%H:%M:%S" ; ftm store iterate -d opensanctions > /dev/null ; date +"%H:%M:%S"
15:48:34
15:49:20
```

`ftm-columnstore`: 87s

```bash
date +"%H:%M:%S" ; ftm cstore iterate -d opensanctions > /dev/null ; date +"%H:%M:%S"
15:49:27
15:50:54
```
