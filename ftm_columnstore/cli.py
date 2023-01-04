import csv
import logging
import sys
from pathlib import Path
from typing import Iterable, Optional, Union

import click
import orjson
from followthemoney.cli.cli import cli as main
from followthemoney.cli.util import MAX_LINE, write_object

from . import exceptions, settings, statements, xref
from .dataset import DS, get_dataset
from .driver import get_driver
from .nk import apply_nk
from .util import clean_int

log = logging.getLogger(__name__)

ResPath = click.Path(dir_okay=False, writable=True, path_type=Path)


def readlines(stream):
    while True:
        line = stream.readline(MAX_LINE)
        if not line:
            return
        yield line.strip()


def _get_driver(obj):
    return get_driver(uri=obj["uri"], table=obj["table"])


def _get_dataset(
    obj,
    name: str,
    origin: Optional[str] = None,
    ignore_errors: Optional[bool] = False,
) -> DS:
    driver = _get_driver(obj)
    return get_dataset(name, origin=origin, driver=driver, ignore_errors=ignore_errors)


@click.group(help="Store FollowTheMoney object data in a column store (Clickhouse)")
@click.option(
    "--log-level",
    default=settings.LOG_LEVEL,
    help="Set logging level",
    show_default=True,
)
@click.option(
    "--uri",
    help="Database connection URI",
    default=settings.DATABASE_URI,
    show_default=True,
)
@click.option(
    "--table",
    help="Database table",
    default=settings.DATABASE_TABLE,
    show_default=True,
)
@click.pass_context
def cli(ctx, log_level, uri, table):
    """
    FtM Columnstore implementation (Clickhouse)
    """
    logging.basicConfig(level=log_level)
    ctx.obj = {"uri": uri, "table": table}
    log.info(f"Using database driver: `{uri}` (table: `{table}`)")


@cli.command("init", help="Initialize database and table.")
@click.option(
    "--recreate/--no-recreate",
    help="Recreate database if it already exists.",
    default=False,
    show_default=True,
)
@click.pass_obj
def cli_init(obj, recreate):
    driver = _get_driver(obj)
    driver.init(recreate=recreate)


@cli.command("write")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-d", "--dataset", help="dataset identifier", required=True)
@click.option("-o", "--origin", default="bulk")
@click.option(
    "--ignore-errors/--no-ignore-errors",
    type=bool,
    default=False,
    help="Don't fail on errors, only log them.",
    show_default=True,
)
@click.option(
    "--optimize/--dont-optimize",
    type=bool,
    default=True,
    help="Optimize after import (should be disabled when writing parallel)",
    show_default=True,
)
@click.pass_obj
def cli_write(obj, infile, dataset, origin, ignore_errors, optimize):
    """
    Write json entities from `infile` to store.
    """
    dataset = _get_dataset(
        obj,
        dataset,
        origin=origin,
        ignore_errors=ignore_errors,
    )

    if dataset.writable:
        bulk = dataset.store.bulk()
        for line in readlines(infile):
            entity = orjson.loads(line)
            bulk.put(entity)
        bulk.flush()
        if optimize:
            dataset.store.driver.optimize()
    else:
        raise click.ClickException(f"Dataset `{dataset}` not writable")


@cli.command("iterate", help="Iterate entities")
@click.option("-d", "--dataset", help="Dataset")
@click.option("--origin")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option("-s", "--schema", multiple=True, help="Schema(s)")
@click.option("-l", "--limit", type=int, help="Stop after reaching this limit")
@click.pass_obj
def cli_iterate(obj, dataset, origin, outfile, schema, limit):
    dataset = _get_dataset(obj, dataset, origin)
    for entity in dataset.store.iterate(origin=origin, schema=schema, limit=limit):
        write_object(outfile, entity)


@cli.command("canonize", help="Add canonical ids for entities")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-d", "--dataset", help="dataset identifier", required=True)
@click.pass_obj
def cli_canonize(obj, infile, dataset):
    """infile: csv format canonical_id,entity_id pairs per row"""
    ds = _get_dataset(obj, dataset)
    reader = csv.reader(infile)
    ix = 0
    for canonical_id, entity_id in reader:
        try:
            ds.store.canonize(entity_id, canonical_id)
            ix += 1
        except exceptions.EntityNotFound:
            pass
        if ix and ix % 1_000 == 0:
            log.info(f"[{dataset}] Canonize entity {ix} ...")
    ds.store.sync()
    log.info(f"[{dataset}] Canonized {ix} entities.")


@cli.command("fingerprints")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option("-d", "--dataset", help="Dataset", required=True)
@click.option(
    "--header/--no-header",
    type=bool,
    default=True,
    help="Print header row or not",
    show_default=True,
)
def cli_fingerprints(infile, outfile, dataset, header):
    """Generate fingerprint statements as csv from json entities from `infile`"""
    writer = csv.DictWriter(outfile, fieldnames=statements.COLUMNS_FPX)
    if header:
        writer.writeheader()
    for data in readlines(infile):
        data = orjson.loads(data)
        for stmt in statements.fingerprints_from_entity(data, dataset):
            writer.writerow(stmt)


@cli.command("dump-statements", help="Dump all statements as csv")
@click.option("-d", "--dataset", help="Dataset")
@click.option("--origin")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.pass_obj
def cli_dump_statements(obj, dataset, origin, outfile):
    dataset = _get_dataset(obj, dataset, origin)
    writer = csv.writer(outfile)
    writer.writerow(statements.COLUMNS)
    for row in dataset.statements(origin=origin):
        writer.writerow(row)


@cli.command("delete", help="Delete dataset or complete store")
@click.option("-d", "--dataset", help="Dataset")
@click.option("-o", "--origin")
@click.pass_obj
def cli_delete(obj, dataset, origin):
    dataset = _get_dataset(obj, dataset, origin)
    dataset.store.delete(origin=origin)


@cli.command("list", help="List datasets in a store")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option(
    "--verbose",
    help="Include entity count numbers",
    is_flag=True,
    default=False,
    show_default=True,
)
@click.pass_obj
def cli_list_datasets(obj, outfile, verbose):
    driver = _get_driver(obj)
    q = f"SELECT distinct dataset FROM {driver.view_stats}"
    if verbose:
        q = f"""SELECT dataset,
        countMerge(entities) AS entities,
        countMerge(statements) AS statements
        FROM {driver.view_stats}
        GROUP BY dataset
        """
    df = driver.query_dataframe(q)
    df = df.set_index("dataset").sort_index()
    if verbose and len(df) > 1:
        df.loc["__total__", :] = df.sum(numeric_only=True)
    df.applymap(clean_int).to_csv(outfile)


@cli.command("get", help="Get a composite entity by id")
@click.argument("id")
@click.option("-d", "--datasets", help="Dataset(s)", multiple=True)
@click.option("-o", "--origin")
@click.pass_obj
def cli_get(
    obj, id: str, datasets: Iterable[str] | None = None, origin: str | None = None
):
    datasets = datasets or "*"
    ds = _get_dataset(obj, datasets)
    proxy = ds.store.get(id)
    sys.stdout.write(orjson.dumps(proxy.to_dict()).decode())


@cli.command("query", help="Execute raw query and print result (csv format) to outfile")
@click.argument("query")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.pass_obj
def cli_query(obj, query, outfile):
    driver = _get_driver(obj)
    df = driver.query_dataframe(query)
    df.to_csv(outfile, index=False)


@cli.command("xref", help="Generate dedupe candidates")
@click.argument("dataset", required=False, default=None)
@click.option("-d", "--datasets", help="Dataset(s)", multiple=True)
@click.option("--origin")
@click.option("-a", "--auto-threshold", type=click.FLOAT, default=None)
@click.option("-s", "--schema", help="Limit to specific ftm Schema", default=None)
@click.option("-l", "--limit", type=click.INT, default=100_000)
@click.option(
    "--algorithm",
    help="Metaphone algorithm to create candidate chunks",
    default="metaphone1",
)
@click.option("--scored/--unscored", is_flag=True, type=click.BOOL, default=True)
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option("--format-csv/--format-nk", is_flag=True, type=click.BOOL, default=True)
@click.option(
    "--entities",
    is_flag=True,
    type=click.BOOL,
    default=False,
    help="Get candidate entities instead of xref score result",
)
@click.pass_obj
def cli_xref(
    obj,
    dataset: Union[str | None],
    datasets: Iterable[str],
    origin: Union[str | None],
    outfile: click.File,
    auto_threshold: Optional[float] = None,
    schema: Optional[str] = None,
    limit: int = 100_000,
    algorithm: str = "metaphone1",
    scored: bool = True,
    format_csv: bool = True,
    entities: bool = False,
):
    """
    Perform xref in 3 possible ways:

    a dataset against itself:
        use only argument [DATASET]

    a dataset against 1 or more other datasets:
        use argument [DATASET] and 1 or more `-d <dataset>` options

    datasets against each other:
        omit argument [DATASET] and use 2 or more `-d <dataset>` options
    """
    if dataset is None and not datasets:
        raise click.ClickException("Please specify at least 1 dataset")

    xkwargs = {
        "auto_threshold": auto_threshold,
        "schema": schema,
        "limit": limit,
        "algorithm": algorithm,
        "scored": scored,
    }
    format_kwargs = {
        "auto_threshold": auto_threshold,
        "left_dataset": None,
        "min_datasets": 1,
    }
    datasets = [_get_dataset(obj, d, origin) for d in datasets]

    if dataset is not None:
        dataset = _get_dataset(obj, dataset, origin)
        # we have a base dataset
        if not len(datasets):
            # perform dataset against itself
            result = xref.xref_dataset(dataset, **xkwargs)
        else:
            # perform dataset against others
            datasets.append(dataset)
            format_kwargs["left_dataset"] = str(dataset)
            format_kwargs["min_datasets"] = 2
            result = xref.xref_datasets(datasets, dataset, **xkwargs)
    else:
        if not len(datasets) > 1:
            raise click.ClickException(
                "Specify at least 2 or more datasets via repeated `-d` arguments"
            )
        # perform full xref between datasets
        format_kwargs["min_datasets"] = 2
        result = xref.xref_datasets(datasets, **xkwargs)

    if entities:
        for entity in xref.get_candidates(result, as_entities=True, **format_kwargs):
            outfile.write(
                orjson.dumps(
                    entity.to_dict(), option=orjson.OPT_APPEND_NEWLINE
                ).decode()
            )
        return

    if format_csv:
        writer = csv.DictWriter(outfile, fieldnames=xref.MATCH_COLUMNS)
        writer.writeheader()
        for row in xref.get_candidates(result, **format_kwargs):
            writer.writerow(row)
        return

    # normal nk rslv format
    for loader in result:
        resolver = loader.resolver
        for edge in resolver.edges.values():
            outfile.write(edge.to_line())


@cli.command("apply-nk")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option("-a", "--auto-threshold", type=click.FLOAT, default=None)
def cli_apply_nk(infile, outfile, auto_threshold):
    """apply re-deduplication on nk resolver file via connected components"""

    def _get_pairs():
        for line in readlines(infile):
            canonical_id, entity_id, judgement, threshold, *rest = orjson.loads(line)
            if auto_threshold is not None:
                if (threshold or 0) > auto_threshold or judgement == "positive":
                    yield canonical_id, entity_id
            elif judgement == "positive":
                yield canonical_id, entity_id

    writer = csv.writer(outfile)
    for row in apply_nk(_get_pairs()):
        writer.writerow(row)


@cli.command("optimize", help="run table optimization")
@click.option("--full", type=click.BOOL, default=False, is_flag=True)
@click.pass_obj
def cli_optimize(obj, full):
    driver = _get_driver(obj)
    driver.optimize(full)


@cli.group("predict")
def cli_predict():
    pass


@cli_predict.command("create-training-data")
@click.argument("output-dir", type=click.Path(file_okay=False, writable=True))
@click.option(
    "-l",
    "--limit",
    default=1_000_000,
    type=click.INT,
    help="Number of sample entities per schema",
    show_default=True,
)
@click.option("-d", "--datasets", help="Dataset(s)", multiple=True)
def predict_create_training_data(
    output_dir, limit, datasets: Optional[Iterable[str]] = None
):
    try:
        from . import predict
    except ModuleNotFoundError:
        raise click.ClickException("Please install `followthemoney-typepredict`")

    with predict.get_sampler(output_dir) as sampler:
        for proxy in predict.get_sample_entities(limit, datasets):
            sampler.add_entity(proxy)


# Register with main FtM command-line tool.
main.add_command(cli, name="cstore")
