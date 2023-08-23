from pathlib import Path

import pytest
from ftmq.io import smart_read_proxies

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()


@pytest.fixture(scope="module")
def fixtures_path():
    return FIXTURES_PATH


@pytest.fixture(scope="module")
def luanda_leaks():
    return smart_read_proxies(FIXTURES_PATH / "icij_luanda_leaks.jsonl")


# @pytest.fixture(scope="module")
# def opensanctions():
#     return smart_read_proxies(FIXTURES_PATH / "opensanctions.ftm.ijson")


# @pytest.fixture(scope="module")
# def wd_peps():
#     return smart_read_proxies(FIXTURES_PATH / "wd_peps.ftm.json")


@pytest.fixture(scope="module")
def ec_meetings():
    return smart_read_proxies(FIXTURES_PATH / "ec_meetings.ftm.json")


@pytest.fixture(scope="module")
def eu_authorities():
    return smart_read_proxies(FIXTURES_PATH / "eu_authorities.ftm.json")


@pytest.fixture(scope="module")
def donations():
    return smart_read_proxies(FIXTURES_PATH / "donations.ijson")
