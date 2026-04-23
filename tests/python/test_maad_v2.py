import importlib
import json
import sqlite3


def load_module():
    maad_v2 = importlib.import_module('maad_v2')
    return importlib.reload(maad_v2)


def sample_payload(total_addrs: int = 42) -> str:
    return json.dumps(
        {
            'schemaVersion': 1,
            'metadata': {
                'input': '-',
                'minPrefixLength': 7,
                'maxPrefixLength': 23,
                'totalAddrs': total_addrs,
            },
            'structure': [{'q': -0.5, 'tauTilde': -0.98, 'sd': 0.01}],
            'spectrum': [{'alpha': 0.75, 'f': 0.60}],
            'dimensions': [{'q': 1, 'dim': 0.48}],
        }
    )


def test_parse_maad_json_accepts_demo_contract() -> None:
    maad_v2 = load_module()

    result = maad_v2.parse_maad_json(sample_payload())

    assert result.schema_version == 1
    assert result.metadata == {
        'input': '-',
        'minPrefixLength': 7,
        'maxPrefixLength': 23,
        'totalAddrs': 42,
    }
    assert result.structure == [{'q': -0.5, 'tauTilde': -0.98, 'sd': 0.01}]
    assert result.spectrum == [{'alpha': 0.75, 'f': 0.60}]
    assert result.dimensions == [{'q': 1, 'dim': 0.48}]


def test_build_maad_v2_rows_stores_source_and_destination_json() -> None:
    maad_v2 = load_module()
    source = maad_v2.parse_maad_json(sample_payload(10))
    destination = maad_v2.parse_maad_json(sample_payload(20))

    rows = maad_v2.build_maad_v2_rows(
        source_id='oh_ir1_gw',
        granularity='5m',
        bucket_start=1744733100,
        bucket_end=1744733400,
        ip_version=4,
        source_result=source,
        destination_result=destination,
    )

    assert rows['structure']['source_id'] == 'oh_ir1_gw'
    assert rows['structure']['granularity'] == '5m'
    assert rows['structure']['bucket_start'] == 1744733100
    assert rows['structure']['ip_version'] == 4
    assert json.loads(rows['structure']['structure_json_sa']) == source.structure
    assert json.loads(rows['structure']['structure_json_da']) == destination.structure
    assert json.loads(rows['structure']['metadata_json_sa'])['totalAddrs'] == 10
    assert json.loads(rows['structure']['metadata_json_da'])['totalAddrs'] == 20
    assert json.loads(rows['spectrum']['spectrum_json_sa']) == source.spectrum
    assert json.loads(rows['dimensions']['dimensions_json_da']) == destination.dimensions


def test_insert_maad_v2_rows_persists_all_three_tables() -> None:
    maad_v2 = load_module()
    conn = sqlite3.connect(':memory:')
    maad_v2.init_maad_v2_tables(conn)

    rows = maad_v2.build_maad_v2_rows(
        source_id='oh_ir1_gw',
        granularity='30m',
        bucket_start=1744733100,
        bucket_end=1744733400,
        ip_version=4,
        source_result=maad_v2.parse_maad_json(sample_payload(10)),
        destination_result=maad_v2.parse_maad_json(sample_payload(20)),
    )
    maad_v2.insert_maad_v2_rows(conn, rows)

    structure = conn.execute(
        'SELECT source_id, granularity, bucket_start, ip_version, structure_json_sa, metadata_json_da FROM structure_stats_v2'
    ).fetchone()
    spectrum = conn.execute(
        'SELECT source_id, granularity, bucket_start, ip_version, spectrum_json_da FROM spectrum_stats_v2'
    ).fetchone()
    dimensions = conn.execute(
        'SELECT source_id, granularity, bucket_start, ip_version, dimensions_json_sa FROM dimension_stats_v2'
    ).fetchone()

    assert structure[:4] == ('oh_ir1_gw', '30m', 1744733100, 4)
    assert json.loads(structure[4]) == [{'q': -0.5, 'tauTilde': -0.98, 'sd': 0.01}]
    assert json.loads(structure[5])['totalAddrs'] == 20
    assert json.loads(spectrum[4]) == [{'alpha': 0.75, 'f': 0.60}]
    assert json.loads(dimensions[4]) == [{'q': 1, 'dim': 0.48}]
