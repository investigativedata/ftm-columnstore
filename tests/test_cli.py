from pathlib import Path

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

    in_uri = str(fixtures_path / "donations.ijson")
    result = runner.invoke(cli, ["write", "-i", in_uri, "-d", "donations"])
    assert result.exit_code == 0

    res = runner.invoke(cli, ["iterate"])
    assert result.exit_code == 0
    lines = _get_lines(res.stdout)
    assert len(lines) == 625
    res = runner.invoke(cli, ["iterate", "-d", "donations"])
    assert result.exit_code == 0
    lines = _get_lines(res.stdout)
    assert len(lines) == 474
