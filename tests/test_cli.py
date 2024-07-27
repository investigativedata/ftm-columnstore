import time
from pathlib import Path

from click.testing import CliRunner as CCliRunner
from ftmq.cli import cli as ftmq
from typer.testing import CliRunner

from ftm_columnstore.cli import cli
from ftm_columnstore.settings import DATABASE_URI

runner = CliRunner()
q_runner = CCliRunner()


def _get_lines(output: str) -> list[str]:
    lines = output.strip().split("\n")
    return [li.strip() for li in lines if li.strip()]


def test_cli(fixtures_path: Path):
    res = runner.invoke(cli, "--help")
    assert res.exit_code == 0

    in_uri = str(fixtures_path / "eu_authorities.ftm.json")
    res = q_runner.invoke(ftmq, ["-i", in_uri, "-o", DATABASE_URI])
    assert res.exit_code == 0

    in_uri = str(fixtures_path / "donations.ijson")
    res = q_runner.invoke(ftmq, ["-i", in_uri, "-o", DATABASE_URI])
    assert res.exit_code == 0

    # sync after write
    res = runner.invoke(cli, ["optimize", "--full"])
    assert res.exit_code == 0
    time.sleep(5)

    res = q_runner.invoke(ftmq, ["-i", DATABASE_URI])
    assert res.exit_code == 0
    lines = _get_lines(res.stdout)
    assert len(lines) == 625
    res = q_runner.invoke(ftmq, ["-i", DATABASE_URI, "-d", "donations"])
    assert res.exit_code == 0
    lines = _get_lines(res.stdout)
    assert len(lines) == 474
