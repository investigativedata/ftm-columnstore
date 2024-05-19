from pathlib import Path

import pytest
from ftmq.io import smart_read_proxies

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()
AUTHORITIES = "eu_authorities.ftm.json"
DONATIONS = "donations.ijson"


@pytest.fixture(scope="module")
def fixtures_path():
    return FIXTURES_PATH


@pytest.fixture(scope="module")
def proxies():
    proxies = []
    proxies.extend(smart_read_proxies(FIXTURES_PATH / AUTHORITIES))
    proxies.extend(smart_read_proxies(FIXTURES_PATH / DONATIONS))
    return proxies


@pytest.fixture(scope="module")
def eu_authorities():
    return [x for x in smart_read_proxies(FIXTURES_PATH / AUTHORITIES)]


@pytest.fixture(scope="module")
def donations():
    return [x for x in smart_read_proxies(FIXTURES_PATH / DONATIONS)]
