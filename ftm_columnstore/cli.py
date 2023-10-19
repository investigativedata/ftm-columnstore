import logging
from typing import Annotated, Optional

import typer
from ftmq.io import smart_read_proxies
from rich import print

from ftm_columnstore import get_engine, get_store, settings

log = logging.getLogger(__name__)

cli = typer.Typer(no_args_is_help=True)


@cli.callback(invoke_without_command=True)
def cli_version(
    version: Annotated[Optional[bool], typer.Option(..., help="Show version")] = False
):
    if version:
        print(settings.VERSION)
        raise typer.Exit()


@cli.command("init", help="Initialize database and table.")
def cli_init(
    recreate: Annotated[
        Optional[bool],
        typer.Option(
            ..., help="Recreate tables if existing (requires DROP TABLE privileges)"
        ),
    ] = False
):
    engine = get_engine()
    engine.ensure(recreate=recreate, exists_ok=True)


@cli.command("write", help="Write entity fragments to the store.")
def cli_write(
    in_uri: Annotated[
        str, typer.Option("-i", help="Entities uri (as read by `ftmq`)")
    ] = "-",
    dataset: Annotated[
        Optional[str], typer.Option("-d", help="Dataset to write to")
    ] = None,
    optimize: Annotated[
        Optional[bool],
        typer.Option(
            ..., help="Optimize after import (don't do that when writing parallel)"
        ),
    ] = False,
):
    """
    Write json entities from `infile` to store.
    """
    store = get_store(dataset=dataset)
    with store.writer() as bulk:
        for ix, proxy in enumerate(smart_read_proxies(in_uri)):
            bulk.add_entity(proxy)
            if ix and ix % 10_000 == 0:
                print("Write entity `%d` ..." % ix)

    if optimize:
        engine = get_engine()
        engine.optimize()


@cli.command("optimize")
def cli_optimize(
    full: Annotated[
        Optional[bool],
        typer.Option(..., help="dedupe full"),
    ] = False,
):
    """
    Perform clickhouse table optimizations
    """
    engine = get_engine()
    engine.optimize(full)
