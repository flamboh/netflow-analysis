import importlib
import json
import sqlite3
from pathlib import Path


def load_modules():
    enrich = importlib.import_module('enrich_maad_from_csv_v2')
    maad_v2 = importlib.import_module('maad_v2')
    processed_inputs = importlib.import_module('processed_inputs_v2')
    return (
        importlib.reload(enrich),
        importlib.reload(maad_v2),
        importlib.reload(processed_inputs),
    )


def write_ugr16_mapping(path: Path) -> Path:
    path.write_text(
        json.dumps(
            {
                'timestamp_format': 'datetime',
                'datetime_format': '%Y-%m-%d %H:%M:%S',
                'timestamp_timezone': 'Europe/Madrid',
                'input_order': 'timestamp_ascending',
                'out_of_order_lag_buckets': 0,
                'has_header': False,
                'fieldnames': [
                    'time_end',
                    'duration',
                    'src_ip',
                    'dst_ip',
                    'src_port',
                    'dst_port',
                    'protocol',
                    'flags',
                    'forwarding_status',
                    'src_tos',
                    'packets',
                    'bytes',
                    'label',
                ],
                'skip_bad_column_count': True,
                'archive': {'member_contains': 'csv'},
                'discovery': {
                    'include_contains': [],
                    'include_suffixes': ['.csv'],
                    'exclude_suffixes': ['.aria2', '.txt'],
                },
                'columns': {
                    'time_end': 'time_end',
                    'src_ip': 'src_ip',
                    'dst_ip': 'dst_ip',
                    'protocol': 'protocol',
                    'packets': 'packets',
                    'bytes': 'bytes',
                },
                'source_id': {'value': 'ugr16-test'},
            }
        ),
        encoding='utf-8',
    )
    return path


def test_enrich_database_from_csv_specs_writes_maad_and_marks_processed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    enrich, maad_v2, processed_inputs = load_modules()
    mapping_path = write_ugr16_mapping(tmp_path / 'ugr16.mapping.json')
    csv_path = tmp_path / 'july.week5.csv'
    csv_path.write_text(
        '\n'.join(
            [
                '2016-07-27 13:43:30,0.000,42.219.154.107,143.72.8.137,59212,53,UDP,.A....,0,0,1,72,background',
                '2016-07-27 13:44:01,0.000,42.219.154.108,143.72.8.138,443,58676,TCP,.AP.S.,0,0,6,5298,background',
                '2016-07-27 13:44:02,0.000,2001:db8::1,2001:db8::2,443,58676,TCP,.AP.S.,0,0,6,5298,background',
            ]
        )
        + '\n',
        encoding='utf-8',
    )
    conn = sqlite3.connect(':memory:')
    processed_inputs.init_processed_inputs_v2_table(conn)
    maad_v2.init_maad_v2_tables(conn)
    with conn:
        processed_inputs.upsert_input_bucket(
            conn,
            input_kind='csv',
            input_locator=str(csv_path),
            source_id='ugr16-test',
            bucket_start=1469619600,
            bucket_end=1469619900,
        )

    def fake_process_maad_tasks(tasks, _maad_workers):
        return [
            maad_v2.build_maad_v2_rows(
                source_id=task['source_id'],
                granularity=task['granularity'],
                bucket_start=task['bucket_start'],
                bucket_end=task['bucket_end'],
                ip_version=4,
                source_result=maad_v2.MaadJsonResult(
                    schema_version=1,
                    metadata={'totalAddrs': len(task['source_addresses'])},
                    structure=[],
                    spectrum=[],
                    dimensions=[],
                ),
                destination_result=maad_v2.MaadJsonResult(
                    schema_version=1,
                    metadata={'totalAddrs': len(task['destination_addresses'])},
                    structure=[],
                    spectrum=[],
                    dimensions=[],
                ),
            )
            for task in tasks
        ]

    monkeypatch.setattr(enrich, 'process_maad_tasks', fake_process_maad_tasks)

    enrich.enrich_database_from_csv_specs(
        conn,
        [
            {
                'input_kind': 'csv',
                'path': str(csv_path),
                'mapping_path': str(mapping_path),
            }
        ],
        maad_bin='/tmp/MAAD',
        maad_backend='python',
        maad_workers=1,
    )

    maad_totals = conn.execute(
        """
        SELECT granularity,
               json_extract(metadata_json_sa, '$.totalAddrs'),
               json_extract(metadata_json_da, '$.totalAddrs')
        FROM structure_stats_v2
        ORDER BY granularity
        """
    ).fetchall()
    statuses = conn.execute('SELECT status FROM processed_inputs_v2').fetchall()

    assert maad_totals == [
        ('1d', 2, 2),
        ('1h', 2, 2),
        ('30m', 2, 2),
        ('5m', 2, 2),
    ]
    assert statuses == [('processed',)]


def test_merge_endpoint_batch_prefilters_to_pending_time_range(tmp_path: Path) -> None:
    import pyarrow as pa

    enrich, _, _ = load_modules()
    mapping_path = write_ugr16_mapping(tmp_path / 'ugr16.mapping.json')
    config = enrich.load_csv_source_config(mapping_path)
    active_buckets = {}

    max_bucket_start = enrich.merge_endpoint_batch(
        active_buckets,
        pa.table(
            {
                'time_end': [
                    '2016-07-27 13:39:59',
                    '2016-07-27 13:43:30',
                    '2016-07-27 13:44:01',
                    '2016-07-27 13:44:30',
                    '2016-07-27 13:44:40',
                    '2016-07-27 13:45:00',
                ],
                'src_ip': [
                    '192.0.2.1',
                    '42.219.154.107',
                    '42.219.154.108',
                    '999.219.154.109',
                    '42.219.154.110',
                    '198.51.100.1',
                ],
                'dst_ip': [
                    '203.0.113.1',
                    '143.72.8.137',
                    '143.72.8.138',
                    '143.72.8.139',
                    'not-ip',
                    '198.51.100.2',
                ],
            }
        ),
        config,
        'ugr16-test',
        None,
        {1469619600},
    )

    bucket = active_buckets[1469619600]
    assert max_bucket_start == 1469619600
    assert bucket.source_ipv4 == {'42.219.154.107', '42.219.154.108'}
    assert bucket.destination_ipv4 == {'143.72.8.137', '143.72.8.138'}
