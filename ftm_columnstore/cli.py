import csv
import json
import logging
from pathlib import Path
from typing import Iterable, Optional, Union

import click
from followthemoney.cli.cli import cli as main
from followthemoney.cli.util import MAX_LINE, write_object
from nomenklatura import Resolver

from . import settings, statements
from .dataset import Dataset
from .driver import get_driver
from .nk import apply_nk
from .xref import MATCH_COLUMNS, format_candidates, run_xref

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
):
    driver = _get_driver(obj)
    return Dataset(name, origin=origin, driver=driver, ignore_errors=ignore_errors)


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


@cli.command("statements")
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
def cli_statements(infile, outfile, dataset, header):
    """Turn json entities from `infile` into statements in csv format"""
    writer = csv.DictWriter(outfile, fieldnames=statements.COLUMNS)
    if header:
        writer.writeheader()
    for data in readlines(infile):
        data = json.loads(data)
        for stmt in statements.statements_from_entity(data, dataset):
            writer.writerow(stmt)


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
        data = json.loads(data)
        for stmt in statements.fingerprints_from_entity(data, dataset):
            writer.writerow(stmt)


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
    "--fingerprints/--no-fingerprints",
    help="Populate fingerprints ix table",
    default=True,
    show_default=True,
    type=bool,
)
@click.option(
    "--ignore-errors/--no-ignore-errors",
    type=bool,
    default=False,
    help="Don't fail on errors, only log them.",
    show_default=True,
)
@click.pass_obj
def cli_write(obj, infile, dataset, origin, fingerprints, ignore_errors):
    """
    Write json entities from `infile` to store.
    """
    dataset = _get_dataset(
        obj,
        dataset,
        origin=origin,
        ignore_errors=ignore_errors,
    )

    bulk = dataset.bulk(with_fingerprints=fingerprints)
    for line in readlines(infile):
        entity = json.loads(line)
        bulk.put(entity)
    bulk.flush()


@cli.command("iterate", help="Iterate entities")
@click.option("-d", "--dataset", help="Dataset")
@click.option("--origin")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.pass_obj
def cli_iterate(obj, dataset, origin, outfile):
    dataset = _get_dataset(obj, dataset, origin)
    for entity in dataset.iterate():
        write_object(outfile, entity)


@cli.command("canonize", help="Add canonical ids for entities")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-d", "--dataset", help="dataset identifier", required=True)
@click.pass_obj
def cli_canonize(obj, infile, dataset):
    """infile: csv format canonical_id,entity_id pairs per row"""
    dataset = _get_dataset(obj, dataset)
    reader = csv.reader(infile)
    for canonical_id, entity_id in reader:
        dataset.canonize(entity_id, canonical_id)


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
    dataset.delete(origin=origin)


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
    q = f"SELECT distinct dataset FROM {driver.table}"
    if verbose:
        q = f"""SELECT dataset,
        count(distinct entity_id) as entities,
        count(*) as statements
        FROM {driver.table} GROUP BY dataset"""
    df = driver.query_dataframe(q)
    df.to_csv(outfile, index=False)


@cli.command("query", help="Execute raw query and print result (csv format) to outfile")
@click.argument("query")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.pass_obj
def cli_query(obj, query, outfile):
    driver = _get_driver(obj)
    df = driver.query_dataframe(query)
    df.to_csv(outfile, index=False)


@cli.command("xref", help="Generate dedupe candidates")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option("-d", "--dataset", help="Dataset(s)", required=True, multiple=True)
@click.option("--origin")
@click.option("-r", "--resolver", type=ResPath)
@click.option("-a", "--auto-threshold", type=click.FLOAT, default=None)
@click.option("-s", "--schema", help="ftm Schema", default=None)
@click.option("-l", "--limit", type=click.INT, default=100_000)
@click.option(
    "--algorithm",
    help="Metaphone algorithm to create candidate chunks",
    default="metaphone1",
)
@click.option("--scored/--unscored", is_flag=True, type=click.BOOL, default=True)
@click.option("--format-csv/--format-nk", is_flag=True, type=click.BOOL, default=True)
@click.pass_obj
def cli_xref(
    obj,
    outfile: click.File,
    dataset: Iterable[str],
    origin: Union[str | None],
    resolver: Optional[Path] = None,
    auto_threshold: Optional[float] = None,
    schema: Optional[str] = None,
    limit: int = 100_000,
    algorithm: str = "metaphone1",
    scored: bool = True,
    format_csv: bool = True,
):
    datasets = [_get_dataset(obj, d, origin) for d in dataset]
    left_dataset = dataset[0]
    resolver_ = Resolver(resolver)
    result = run_xref(
        datasets,
        resolver_,
        auto_threshold=auto_threshold,
        scored=scored,
        schema=schema,
        limit=limit,
        algorithm=algorithm,
    )

    if format_csv:
        writer = csv.DictWriter(outfile, fieldnames=MATCH_COLUMNS)
        writer.writeheader()
        for row in format_candidates(
            result,
            auto_threshold=auto_threshold,
            min_datasets=2,
            left_dataset=left_dataset,
        ):
            writer.writerow(row)
        return
    # normal nk rslv format
    for _, res in result:
        for edge in res.edges.values():
            outfile.write(edge.to_line())


@cli.command("apply-nk")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option("-a", "--auto-threshold", type=click.FLOAT, default=None)
def cli_apply_nk(infile, outfile, auto_threshold):
    """apply re-deduplication on nk resolver file via connected components"""

    def _get_pairs():
        for line in readlines(infile):
            canonical_id, entity_id, judgement, threshold, *rest = json.loads(line)
            if auto_threshold is not None:
                if (threshold or 0) > auto_threshold or judgement == "positive":
                    yield canonical_id, entity_id
            elif judgement == "positive":
                yield canonical_id, entity_id

    writer = csv.writer(outfile)
    for row in apply_nk(_get_pairs()):
        writer.writerow(row)


@cli.command("optimize", help="run table optimization")
@click.pass_obj
def cli_optimize(obj):
    driver = _get_driver(obj)
    driver.execute(f"OPTIMIZE TABLE {driver.table} FINAL DEDUPLICATE")


# Register with main FtM command-line tool.
main.add_command(cli, name="cstore")
