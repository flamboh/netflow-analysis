import importlib
import sqlite3


def load_modules():
    common = importlib.import_module('common')
    flow_db = importlib.import_module('flow_db')
    return importlib.reload(common), importlib.reload(flow_db)


def test_parse_nfdump_output_parses_numeric_fields() -> None:
    _, flow_db = load_modules()

    parsed = flow_db.parse_nfdump_output('Flows: 12\nbytes_tcp: 99\nIdent: router-1\n')

    assert parsed == {'flows': 12, 'bytes_tcp': 99, 'ident': 'router-1'}


def test_process_file_worker_returns_zero_stats_for_gap_placeholder() -> None:
    _, flow_db = load_modules()

    result = flow_db.process_file_worker(('/tmp/missing', 'r1', 123, 0))

    assert result['success'] is True
    assert result['data']['flows'] == 0
    assert result['data']['sequence_failures'] == 0


def test_batch_insert_results_inserts_successful_rows() -> None:
    common, flow_db = load_modules()
    conn = sqlite3.connect(':memory:')
    flow_db.init_netflow_stats_table(conn)

    inserted = flow_db.batch_insert_results(
        conn,
        [
            {
                'file_path': '/tmp/a',
                'router': 'r1',
                'timestamp': 123,
                'success': True,
                'data': {'flows': 3, 'packets': 4, 'bytes': 5},
            },
            {
                'file_path': '/tmp/b',
                'router': 'r2',
                'timestamp': 456,
                'success': False,
                'data': {'flows': 9},
            },
        ],
    )

    row = conn.execute(
        'SELECT file_path, router, flows, packets, bytes, sequence_failures FROM netflow_stats'
    ).fetchone()
    assert inserted == 1
    assert row == ('/tmp/a', 'r1', 3, 4, 5, 0)
