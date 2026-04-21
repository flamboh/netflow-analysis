import importlib
import json
import sqlite3
from pathlib import Path


def load_modules():
    pipeline_v2 = importlib.import_module('pipeline_v2')
    normalized_rows_v2 = importlib.import_module('normalized_rows_v2')
    return importlib.reload(pipeline_v2), importlib.reload(normalized_rows_v2)


def test_process_input_specs_populates_v2_tables_for_csv(tmp_path: Path) -> None:
    pipeline_v2, _ = load_modules()
    mapping_path = tmp_path / 'mapping.json'
    mapping_path.write_text(
        json.dumps(
            {
                'timestamp_format': 'unix',
                'columns': {
                    'time_received': 'received_at',
                    'src_ip': 'src',
                    'dst_ip': 'dst',
                    'protocol': 'pr',
                    'packets': 'pkt',
                    'bytes': 'byt',
                },
                'source_id': {'value': 'uo-feed'},
            }
        ),
        encoding='utf-8',
    )
    csv_path = tmp_path / 'flows.csv'
    csv_path.write_text(
        '\n'.join(
            [
                'received_at,src,dst,pr,pkt,byt',
                '1744733279,192.0.2.1,198.51.100.9,6,10,1000',
                '1744733001,2001:db8::1,2001:db8::2,58,3,300',
            ]
        )
        + '\n',
        encoding='utf-8',
    )
    conn = sqlite3.connect(':memory:')

    pipeline_v2.process_input_specs(
        conn,
        [
            {
                'input_kind': 'csv',
                'path': str(csv_path),
                'mapping_path': str(mapping_path),
            }
        ],
    )

    processed_inputs = conn.execute(
        'SELECT input_kind, input_locator, source_id, bucket_start FROM processed_inputs_v2 ORDER BY bucket_start'
    ).fetchall()
    netflow = conn.execute(
        'SELECT source_id, bucket_start, ip_version, flows, packets, bytes FROM netflow_stats_v2 ORDER BY bucket_start, ip_version'
    ).fetchall()
    ip_stats = conn.execute(
        'SELECT source_id, bucket_start, sa_ipv4_count, sa_ipv6_count FROM ip_stats_v2 ORDER BY bucket_start'
    ).fetchall()
    protocol_stats = conn.execute(
        'SELECT source_id, bucket_start, protocols_list_ipv4, protocols_list_ipv6 FROM protocol_stats_v2 ORDER BY bucket_start'
    ).fetchall()

    assert processed_inputs == [
        ('csv', str(csv_path), 'uo-feed', 1744732800),
        ('csv', str(csv_path), 'uo-feed', 1744733100),
    ]
    assert netflow == [
        ('uo-feed', 1744732800, 6, 1, 3, 300),
        ('uo-feed', 1744733100, 4, 1, 10, 1000),
    ]
    assert ip_stats == [
        ('uo-feed', 1744732800, 0, 1),
        ('uo-feed', 1744733100, 1, 0),
    ]
    assert protocol_stats == [
        ('uo-feed', 1744732800, '', '58'),
        ('uo-feed', 1744733100, '6', ''),
    ]


def test_process_input_specs_uses_nfdump_adapter_for_nfcapd(monkeypatch) -> None:
    pipeline_v2, normalized_rows_v2 = load_modules()
    conn = sqlite3.connect(':memory:')

    def fake_iter_nfdump_rows(path: str, source_id: str):
        assert path == '/captures/oh_ir1_gw/2025/04/15/nfcapd.202504150005'
        assert source_id == 'oh_ir1_gw'
        return iter(
            [
                normalized_rows_v2.NormalizedRow(
                    source_id='oh_ir1_gw',
                    bucket_start=1744733100,
                    bucket_end=1744733400,
                    time_received=1744733279,
                    time_end=1744733000,
                    time_start=1744732700,
                    src_ip='192.0.2.1',
                    dst_ip='198.51.100.9',
                    ip_version=4,
                    src_port=443,
                    dst_port=55000,
                    protocol=6,
                    packets=10,
                    bytes=1000,
                    src_tos=2,
                    dst_tos=0,
                ),
                normalized_rows_v2.NormalizedRow(
                    source_id='oh_ir1_gw',
                    bucket_start=1744733100,
                    bucket_end=1744733400,
                    time_received=1744733279,
                    time_end=1744733000,
                    time_start=1744732700,
                    src_ip='2001:db8::1',
                    dst_ip='2001:db8::2',
                    ip_version=6,
                    src_port=0,
                    dst_port=0,
                    protocol=58,
                    packets=3,
                    bytes=300,
                    src_tos=1,
                    dst_tos=0,
                ),
            ]
        )

    monkeypatch.setattr(pipeline_v2, 'iter_nfdump_rows', fake_iter_nfdump_rows)

    pipeline_v2.process_input_specs(
        conn,
        [
            {
                'input_kind': 'nfcapd',
                'path': '/captures/oh_ir1_gw/2025/04/15/nfcapd.202504150005',
                'source_id': 'oh_ir1_gw',
            }
        ],
    )

    netflow = conn.execute(
        'SELECT source_id, bucket_start, ip_version, flows FROM netflow_stats_v2 ORDER BY ip_version'
    ).fetchall()
    processed_inputs = conn.execute(
        'SELECT input_kind, input_locator, source_id, bucket_start, netflow_stats_v2_status, ip_stats_v2_status, protocol_stats_v2_status FROM processed_inputs_v2'
    ).fetchall()

    assert netflow == [
        ('oh_ir1_gw', 1744733100, 4, 1),
        ('oh_ir1_gw', 1744733100, 6, 1),
    ]
    assert processed_inputs == [
        (
            'nfcapd',
            '/captures/oh_ir1_gw/2025/04/15/nfcapd.202504150005',
            'oh_ir1_gw',
            1744733100,
            1,
            1,
            1,
        )
    ]


def test_process_input_specs_always_runs_maad(monkeypatch) -> None:
    pipeline_v2, normalized_rows_v2 = load_modules()
    conn = sqlite3.connect(':memory:')

    def fake_iter_nfdump_rows(path: str, source_id: str):
        return iter(
            [
                normalized_rows_v2.NormalizedRow(
                    source_id='oh_ir1_gw',
                    bucket_start=1744733100,
                    bucket_end=1744733400,
                    time_received=1744733279,
                    time_end=1744733000,
                    time_start=1744732700,
                    src_ip='192.0.2.1',
                    dst_ip='198.51.100.9',
                    ip_version=4,
                    src_port=443,
                    dst_port=55000,
                    protocol=6,
                    packets=10,
                    bytes=1000,
                    src_tos=2,
                    dst_tos=0,
                ),
                normalized_rows_v2.NormalizedRow(
                    source_id='oh_ir1_gw',
                    bucket_start=1744733100,
                    bucket_end=1744733400,
                    time_received=1744733279,
                    time_end=1744733000,
                    time_start=1744732700,
                    src_ip='192.0.2.2',
                    dst_ip='198.51.100.10',
                    ip_version=4,
                    src_port=443,
                    dst_port=55001,
                    protocol=6,
                    packets=11,
                    bytes=1100,
                    src_tos=2,
                    dst_tos=0,
                ),
            ]
        )

    maad_calls = []

    def fake_run_maad_json(maad_bin, addresses):
        maad_calls.append((str(maad_bin), sorted(addresses)))
        return pipeline_v2.MaadJsonResult(
            schema_version=1,
            metadata={
                'input': '-',
                'minPrefixLength': 7,
                'maxPrefixLength': 23,
                'totalAddrs': len(addresses),
            },
            structure=[{'q': -0.5, 'tauTilde': -0.98, 'sd': 0.01}],
            spectrum=[{'alpha': 0.75, 'f': 0.60}],
            dimensions=[{'q': 1, 'dim': 0.48}],
        )

    monkeypatch.setattr(pipeline_v2, 'iter_nfdump_rows', fake_iter_nfdump_rows)
    monkeypatch.setattr(pipeline_v2, 'run_maad_json', fake_run_maad_json)

    pipeline_v2.process_input_specs(
        conn,
        [
            {
                'input_kind': 'nfcapd',
                'path': '/captures/oh_ir1_gw/2025/04/15/nfcapd.202504150005',
                'source_id': 'oh_ir1_gw',
            }
        ],
        maad_bin='/tmp/MAAD',
    )

    structure = conn.execute(
        'SELECT source_id, bucket_start, ip_version, structure_json_sa, structure_json_da FROM structure_stats_v2'
    ).fetchone()
    processed_inputs = conn.execute(
        'SELECT structure_stats_v2_status, spectrum_stats_v2_status, dimension_stats_v2_status FROM processed_inputs_v2'
    ).fetchone()

    assert maad_calls == [
        ('/tmp/MAAD', ['192.0.2.1', '192.0.2.2']),
        ('/tmp/MAAD', ['198.51.100.10', '198.51.100.9']),
    ]
    assert structure[:3] == ('oh_ir1_gw', 1744733100, 4)
    assert processed_inputs == (1, 1, 1)
