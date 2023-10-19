from pathlib import Path

import orjson
from nomenklatura.entity import CompositeEntity
from typer.testing import CliRunner

from ftm_columnstore.cli import cli

runner = CliRunner()


def _get_lines(output: str) -> list[str]:
    lines = output.strip().split("\n")
    return [li.strip() for li in lines if li.strip()]


def test_cli(fixtures_path: Path):
    result = runner.invoke(cli, "--help")
    assert result.exit_code == 0

    in_uri = str(fixtures_path / "eu_authorities.ftm.json")
    result = runner.invoke(cli, ["write", "-i", in_uri, "-d", "eu_authorities"])
    assert result.exit_code == 0
