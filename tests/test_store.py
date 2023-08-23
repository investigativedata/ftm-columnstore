from ftmq.model import Catalog, Dataset
from ftmq.query import Query
from nomenklatura.db import get_metadata
from nomenklatura.entity import CompositeEntity

from ftm_columnstore.statements import FingerprintStatement
from ftm_columnstore.store import get_store


def test_store_base(donations):
    get_metadata.cache_clear()
    store = get_store()
    len_proxies = 0
    with store.writer() as bulk:
        for proxy in donations:
            bulk.add_entity(proxy)
            len_proxies += 1

    view = store.default_view()
    proxies = [e for e in view.entities()]
    assert len(proxies) == len_proxies

    entity = view.get_entity("4e0bd810e1fcb49990a2b31709b6140c4c9139c5")
    assert entity.caption == "Tchibo Holding AG"

    tested = False
    for prop, value in entity.itervalues():
        if prop.type.name == "entity":
            for iprop, ientity in view.get_inverted(value):
                assert iprop.reverse == prop
                assert ientity == entity
                tested = True
                break
    assert tested

    adjacent = list(view.get_adjacent(entity))
    assert len(adjacent) == 2

    # FIXME delete GRANT
    # writer = store.writer()
    # stmts = writer.pop(entity.id)
    # assert len(stmts) == len(list(entity.statements))
    # assert view.get_entity(entity.id) is None

    # fingerprint statements
    with store.engine.connect() as conn:
        cursor = conn.execute(
            f"SELECT * FROM {store.engine.table_fpx} WHERE entity_id = '4e0bd810e1fcb49990a2b31709b6140c4c9139c5'"
        )
        row = cursor.fetchone()
        stmt = FingerprintStatement.from_row(*row)
        assert stmt["value"] == "ag holding tchibo"

    # upsert
    with store.writer() as bulk:
        for proxy in donations:
            bulk.add_entity(proxy)

    proxies = [e for e in view.entities()]
    assert len(proxies) == len_proxies
    entity = view.get_entity(entity.id)
    assert entity.caption == "Tchibo Holding AG"


def test_store_queries(ec_meetings, eu_authorities):
    get_metadata.cache_clear()
    catalog = Catalog(
        datasets=[Dataset(name="eu_authorities"), Dataset(name="ec_meetings")]
    )
    store = get_store(catalog=catalog)
    with store.writer() as bulk:
        for proxy in eu_authorities:
            bulk.add_entity(proxy)
        for proxy in ec_meetings:
            bulk.add_entity(proxy)
    view = store.default_view()
    properties = view.get_entity("eu-authorities-satcen").to_dict()["properties"]
    assert properties == {
        "legalForm": ["security_agency"],
        "keywords": ["security_agency"],
        "website": ["https://www.satcen.europa.eu/"],
        "description": [
            "The European Union Satellite Centre (SatCen) supports EU decision-making and\naction in the context of Europe’s Common Foreign and Security Policy. This\nmeans providing products and services based on exploiting space assets and\ncollateral data, including satellite imagery and aerial imagery, and related\nservices."  # noqa
        ],
        "name": ["European Union Satellite Centre"],
        "weakAlias": ["SatCen"],
        "jurisdiction": ["eu"],
        "sourceUrl": ["https://www.asktheeu.org/en/body/satcen"],
    }
    assert store.dataset.leaf_names == {"ec_meetings", "eu_authorities"}
    tested = False
    for proxy in store.iterate():
        assert isinstance(proxy, CompositeEntity)
        tested = True
        break
    assert tested

    view = store.default_view()
    ds = Dataset(name="eu_authorities").to_nk()
    view = store.view(ds)
    assert len([e for e in view.entities()]) == 151

    view = store.query()
    q = Query().where(dataset="eu_authorities")
    res = [e for e in view.entities(q)]
    assert len(res) == 151
    assert "eu_authorities" in res[0].datasets
    q = Query().where(schema="Event", prop="date", value=2023, operator="gte")
    res = [e for e in view.entities(q)]
    assert res[0].schema.name == "Event"
    assert len(res) == 76

    # coverage
    q = Query().where(dataset="eu_authorities")
    coverage = view.coverage(q)
    assert coverage.countries == [{"code": "eu", "label": "eu", "count": 151}]
    assert coverage.entities == 151
    assert coverage.schemata == [
        {
            "name": "PublicBody",
            "label": "Public body",
            "plural": "Public bodies",
            "count": 151,
        }
    ]

    # ordering
    q = Query().where(schema="Event", prop="date", value=2023, operator="gte")
    q = q.order_by("location")
    res = [e for e in view.entities(q)]
    assert len(res) == 76
    assert res[0].get("location") == ["Abu Dhabi, UAE"]
    q = q.order_by("location", ascending=False)
    res = [e for e in view.entities(q)]
    assert len(res) == 76
    assert res[0].get("location") == ["virtual"]

    # slice
    q = Query().where(schema="Event", prop="date", value=2023, operator="gte")
    q = q.order_by("location")
    q = q[:10]
    res = [e for e in view.entities(q)]
    assert len(res) == 10
    assert res[0].get("location") == ["Abu Dhabi, UAE"]

    # aggregation
    q = Query().aggregate("max", "date").aggregate("min", "date")
    res = view.aggregations(q)
    assert res == {"max": {"date": "2023-01-20"}, "min": {"date": "2014-11-12"}}

    return True
