import logging
from typing import Annotated, Optional

import typer
from rich import print

from ftm_columnstore import get_engine, settings

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
