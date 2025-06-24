import sqlite3
import subprocess

conn = sqlite3.connect("flowStats.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS flow_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    flows INTEGER,
    flows_tcp INTEGER,
    flows_udp INTEGER,
    flows_icmp INTEGER,
    flows_other INTEGER,
    packets INTEGER,
    packets_tcp INTEGER,
    packets_udp INTEGER,
    packets_icmp INTEGER,
    packets_other INTEGER,
    bytes INTEGER,
    bytes_tcp INTEGER,
    bytes_udp INTEGER,
    bytes_icmp INTEGER,
    bytes_other INTEGER,
    first INTEGER,
    last INTEGER,
    msec_first INTEGER,
    msec_last INTEGER,
    sequence_failures INTEGER
)
""")

command = ["nfdump", "-I", "-r", "/research/tango_cis/uonet-in/cc-ir1-gw/2025/05/02/nfcapd.202505020000"]

result = subprocess.run(command, capture_output=True, text=True)


if result.returncode == 0:
  print("Output:\n", result.stdout)
else:
  print("Error:\n", result.stderr)
