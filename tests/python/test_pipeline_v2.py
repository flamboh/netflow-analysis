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
        max_workers=1,
    )

    processed_inputs = conn.execute(
        'SELECT input_kind, input_locator, source_id, bucket_start FROM processed_inputs_v2 ORDER BY bucket_start'
    ).fetchall()
    netflow = conn.execute(
        'SELECT source_id, bucket_start, ip_version, flows, packets, bytes FROM netflow_stats_v2 ORDER BY bucket_start, ip_version'
    ).fetchall()
    ip_stats = conn.execute(
        "SELECT source_id, bucket_start, sa_ipv4_count, sa_ipv6_count FROM ip_stats_v2 WHERE granularity = '5m' ORDER BY bucket_start"
    ).fetchall()
    protocol_stats = conn.execute(
        "SELECT source_id, bucket_start, protocols_list_ipv4, protocols_list_ipv6 FROM protocol_stats_v2 WHERE granularity = '5m' ORDER BY bucket_start"
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
    pipeline_v2, _ = load_modules()
    conn = sqlite3.connect(':memory:')

    def fake_build_nfcapd_bucket_payload(path: str, source_id: str):
        assert path == '/captures/oh_ir1_gw/2025/04/15/nfcapd.202504150005'
        assert source_id == 'oh_ir1_gw'
        return {
            'processed_bucket': {
                'input_kind': 'nfcapd',
                'input_locator': path,
                'source_id': source_id,
                'bucket_start': 1744733100,
                'bucket_end': 1744733400,
            },
            'netflow_rows': [
                {
                    'source_id': 'oh_ir1_gw',
                    'bucket_start': 1744733100,
                    'bucket_end': 1744733400,
                    'ip_version': 4,
                    'flows': 1,
                    'flows_tcp': 1,
                    'flows_udp': 0,
                    'flows_icmp': 0,
                    'flows_other': 0,
                    'packets': 10,
                    'packets_tcp': 10,
                    'packets_udp': 0,
                    'packets_icmp': 0,
                    'packets_other': 0,
                    'bytes': 1000,
                    'bytes_tcp': 1000,
                    'bytes_udp': 0,
                    'bytes_icmp': 0,
                    'bytes_other': 0,
                },
                {
                    'source_id': 'oh_ir1_gw',
                    'bucket_start': 1744733100,
                    'bucket_end': 1744733400,
                    'ip_version': 6,
                    'flows': 1,
                    'flows_tcp': 0,
                    'flows_udp': 0,
                    'flows_icmp': 1,
                    'flows_other': 0,
                    'packets': 3,
                    'packets_tcp': 0,
                    'packets_udp': 0,
                    'packets_icmp': 3,
                    'packets_other': 0,
                    'bytes': 300,
                    'bytes_tcp': 0,
                    'bytes_udp': 0,
                    'bytes_icmp': 300,
                    'bytes_other': 0,
                },
            ],
            'ip_row': {
                'source_id': 'oh_ir1_gw',
                'granularity': '5m',
                'bucket_start': 1744733100,
                'bucket_end': 1744733400,
                'sa_ipv4_count': 1,
                'da_ipv4_count': 1,
                'sa_ipv6_count': 1,
                'da_ipv6_count': 1,
            },
            'protocol_row': {
                'source_id': 'oh_ir1_gw',
                'granularity': '5m',
                'bucket_start': 1744733100,
                'bucket_end': 1744733400,
                'unique_protocols_count_ipv4': 1,
                'unique_protocols_count_ipv6': 1,
                'protocols_list_ipv4': '6',
                'protocols_list_ipv6': '58',
            },
            'raw_bucket': {
                'source_id': 'oh_ir1_gw',
                'bucket_start': 1744733100,
                'source_ipv4': ['192.0.2.1'],
                'destination_ipv4': ['198.51.100.9'],
                'source_ipv6': ['2001:db8::1'],
                'destination_ipv6': ['2001:db8::2'],
                'protocols_ipv4': ['6'],
                'protocols_ipv6': ['58'],
                'maad_source_ipv4': ['192.0.2.1'],
                'maad_destination_ipv4': ['198.51.100.9'],
            },
        }

    monkeypatch.setattr(pipeline_v2, 'build_nfcapd_bucket_payload', fake_build_nfcapd_bucket_payload)

    pipeline_v2.process_input_specs(
        conn,
        [
            {
                'input_kind': 'nfcapd',
                'path': '/captures/oh_ir1_gw/2025/04/15/nfcapd.202504150005',
                'source_id': 'oh_ir1_gw',
            }
        ],
        max_workers=1,
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

    def fake_iter_csv_rows(path: str, mapping_path: str):
        assert path == '/captures/flows.csv'
        assert mapping_path == '/captures/mapping.json'
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

    monkeypatch.setattr(pipeline_v2, 'iter_csv_rows', fake_iter_csv_rows)
    monkeypatch.setattr(pipeline_v2, 'run_maad_json', fake_run_maad_json)

    pipeline_v2.process_input_specs(
        conn,
        [
                {
                    'input_kind': 'csv',
                    'path': '/captures/flows.csv',
                    'mapping_path': '/captures/mapping.json',
                }
        ],
        maad_bin='/tmp/MAAD',
        max_workers=1,
    )

    structure = conn.execute(
        "SELECT source_id, bucket_start, ip_version, structure_json_sa, structure_json_da FROM structure_stats_v2 WHERE granularity = '5m'"
    ).fetchone()
    processed_inputs = conn.execute(
        'SELECT structure_stats_v2_status, spectrum_stats_v2_status, dimension_stats_v2_status FROM processed_inputs_v2'
    ).fetchone()

    assert maad_calls[:2] == [
        ('/tmp/MAAD', ['192.0.2.1', '192.0.2.2']),
        ('/tmp/MAAD', ['198.51.100.10', '198.51.100.9']),
    ]
    assert structure[:3] == ('oh_ir1_gw', 1744733100, 4)
    assert processed_inputs == (1, 1, 1)


def test_fractional_nfdump_timestamp_rows_are_not_headers() -> None:
    pipeline_v2, _ = load_modules()

    assert not pipeline_v2.looks_like_nfdump_header(['1741075200.000'])
    assert pipeline_v2.looks_like_nfdump_header(['received'])


def test_process_input_specs_can_write_parallel_worker_payloads(monkeypatch) -> None:
    pipeline_v2, _ = load_modules()
    conn = sqlite3.connect(':memory:')
    captured_processes = []

    payloads = [
        {
            'processed_buckets': [
                {
                    'input_kind': 'csv',
                    'input_locator': '/tmp/a.csv',
                    'source_id': 'feed-a',
                    'bucket_start': 1744732800,
                    'bucket_end': 1744733100,
                }
            ],
            'netflow_rows': [],
            'ip_rows': [
                    {
                        'source_id': 'feed-a',
                        'granularity': '5m',
                        'bucket_start': 1744732800,
                    'bucket_end': 1744733100,
                    'sa_ipv4_count': 1,
                    'da_ipv4_count': 1,
                    'sa_ipv6_count': 0,
                    'da_ipv6_count': 0,
                }
            ],
            'protocol_rows': [],
            'maad_rows': [],
        },
        {
            'processed_buckets': [
                {
                    'input_kind': 'csv',
                    'input_locator': '/tmp/b.csv',
                    'source_id': 'feed-b',
                    'bucket_start': 1744733100,
                    'bucket_end': 1744733400,
                }
            ],
            'netflow_rows': [],
            'ip_rows': [
                    {
                        'source_id': 'feed-b',
                        'granularity': '5m',
                        'bucket_start': 1744733100,
                    'bucket_end': 1744733400,
                    'sa_ipv4_count': 2,
                    'da_ipv4_count': 2,
                    'sa_ipv6_count': 0,
                    'da_ipv6_count': 0,
                }
            ],
            'protocol_rows': [],
            'maad_rows': [],
        },
    ]

    class FakePool:
        def __init__(self, processes):
            captured_processes.append(processes)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def imap_unordered(self, worker, tasks, chunksize=1):
            assert chunksize == 1
            return iter(payloads)

    monkeypatch.setattr(pipeline_v2, 'Pool', FakePool)
    monkeypatch.setattr(
        pipeline_v2,
        'process_input_spec_worker',
        lambda task: (_ for _ in ()).throw(AssertionError('fake pool should supply payloads')),
        raising=False,
    )

    pipeline_v2.process_input_specs(
        conn,
        [
            {'input_kind': 'csv', 'path': '/tmp/a.csv', 'mapping_path': '/tmp/a.json'},
            {'input_kind': 'csv', 'path': '/tmp/b.csv', 'mapping_path': '/tmp/b.json'},
        ],
        max_workers=2,
    )

    assert captured_processes == [2]
    assert conn.execute(
        'SELECT source_id, bucket_start, sa_ipv4_count FROM ip_stats_v2 ORDER BY source_id'
    ).fetchall() == [
        ('feed-a', 1744732800, 1),
        ('feed-b', 1744733100, 2),
    ]


def test_process_input_specs_writes_v1_granularity_aggregates_for_csv(monkeypatch, tmp_path: Path) -> None:
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
                '1744732801,192.0.2.1,198.51.100.1,6,10,1000',
                '1744733101,192.0.2.2,198.51.100.2,17,20,2000',
            ]
        )
        + '\n',
        encoding='utf-8',
    )
    conn = sqlite3.connect(':memory:')

    def fake_run_maad_json(maad_bin, addresses):
        return pipeline_v2.MaadJsonResult(
            schema_version=1,
            metadata={
                'input': '-',
                'minPrefixLength': 7,
                'maxPrefixLength': 23,
                'totalAddrs': len(addresses),
            },
            structure=[],
            spectrum=[],
            dimensions=[],
        )

    monkeypatch.setattr(pipeline_v2, 'run_maad_json', fake_run_maad_json)

    pipeline_v2.process_input_specs(
        conn,
        [
            {
                'input_kind': 'csv',
                'path': str(csv_path),
                'mapping_path': str(mapping_path),
            }
        ],
        maad_bin='/tmp/MAAD',
        max_workers=1,
    )

    ip_stats = conn.execute(
        """
        SELECT granularity, bucket_start, sa_ipv4_count, da_ipv4_count
        FROM ip_stats_v2
        ORDER BY granularity, bucket_start
        """
    ).fetchall()
    protocol_stats = conn.execute(
        """
        SELECT granularity, bucket_start, unique_protocols_count_ipv4, protocols_list_ipv4
        FROM protocol_stats_v2
        ORDER BY granularity, bucket_start
        """
    ).fetchall()
    maad_30m = conn.execute(
        """
        SELECT json_extract(metadata_json_sa, '$.totalAddrs'),
               json_extract(metadata_json_da, '$.totalAddrs')
        FROM structure_stats_v2
        WHERE granularity = '30m'
        """
    ).fetchone()

    assert ip_stats == [
        ('1d', 1744700400, 2, 2),
        ('1h', 1744732800, 2, 2),
        ('30m', 1744732800, 2, 2),
        ('5m', 1744732800, 1, 1),
        ('5m', 1744733100, 1, 1),
    ]
    assert protocol_stats == [
        ('1d', 1744700400, 2, '17,6'),
        ('1h', 1744732800, 2, '17,6'),
        ('30m', 1744732800, 2, '17,6'),
        ('5m', 1744732800, 1, '6'),
        ('5m', 1744733100, 1, '17'),
    ]
    assert maad_30m == (2, 2)
