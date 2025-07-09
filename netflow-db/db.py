import sqlite3
import subprocess
from datetime import datetime, timedelta
import os
  
# Set to True to drop the table and start from scratch
FIRST_RUN = False

def main():
    conn = sqlite3.connect("flowStats.db")
    cursor = conn.cursor()

    global FIRST_RUN

    if FIRST_RUN:
        cursor.execute("""
        DROP TABLE IF EXISTS netflow_stats
        """)
    else:
        print("--------------------------------")
        print(f"Maintaining database at {datetime.now()}")
        print("--------------------------------")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS netflow_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_path TEXT NOT NULL UNIQUE,
        router TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        
        flows INTEGER NOT NULL,
        flows_tcp INTEGER NOT NULL,
        flows_udp INTEGER NOT NULL,
        flows_icmp INTEGER NOT NULL,
        flows_other INTEGER NOT NULL,
        
        packets INTEGER NOT NULL,
        packets_tcp INTEGER NOT NULL,
        packets_udp INTEGER NOT NULL,
        packets_icmp INTEGER NOT NULL,
        packets_other INTEGER NOT NULL,
        
        bytes INTEGER NOT NULL,
        bytes_tcp INTEGER NOT NULL,
        bytes_udp INTEGER NOT NULL,
        bytes_icmp INTEGER NOT NULL,
        bytes_other INTEGER NOT NULL,
        
        first_timestamp INTEGER NOT NULL,
        last_timestamp INTEGER NOT NULL,
        msec_first INTEGER NOT NULL,
        msec_last INTEGER NOT NULL,
        
        sequence_failures INTEGER NOT NULL,
        
        processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_router_timestamp ON netflow_stats (router, timestamp)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_file_path ON netflow_stats (file_path)
    """)
    if FIRST_RUN:
        # nfcapd starts at 2024-03-01 00:00:00
        start_time = datetime(2024, 3, 1)
    else:
        # Get the last processed file from the database
        last_file = cursor.execute("""
        SELECT file_path FROM netflow_stats ORDER BY timestamp DESC LIMIT 1
        """).fetchone()
        if last_file:
            last_file_path = last_file[0]
            timestamp_str = last_file_path.split('/')[-1].split('.')[1]
            year = timestamp_str[:4]
            month = timestamp_str[4:6]
            day = timestamp_str[6:8]
            hour = timestamp_str[8:10]
            minute = timestamp_str[10:12]
            last_file_time = datetime(int(year), int(month), int(day), int(hour), int(minute))
            start_time = last_file_time + timedelta(minutes=5)
            print(f"Start found at {start_time}")
        else:
            raise Exception("Failure to find last processed file")

    current_time = start_time

    # Process files in 5-minute intervals
    routers = ['cc-ir1-gw', 'oh-ir1-gw']    
    base_path = '/research/tango_cis/uonet-in'

    processed_count = 0
    error_count = 0

    print(f"Starting processing from {start_time}")

    while current_time < datetime.now():
        timestamp_str = current_time.strftime('%Y%m%d%H%M')
        timestamp_unix = int(current_time.timestamp())
        
        for router in routers:
            # Construct file path
            year = current_time.strftime('%Y')
            month = current_time.strftime('%m')
            day = current_time.strftime('%d')
            
            file_path = f"{base_path}/{router}/{year}/{month}/{day}/nfcapd.{timestamp_str}"
            
            # Check if file exists before processing
            if os.path.exists(file_path):
                if process_netflow_file(conn, file_path, router, timestamp_unix):
                    processed_count += 1
                else:
                    error_count += 1
            else:
                print(f"File not found: {file_path}")
        
        # Move to next 5-minute interval
        current_time += timedelta(minutes=5)
        
        # Commit every 100 files to avoid losing data
        if processed_count % 100 == 0:
            conn.commit()
            print(f"Progress: {processed_count} files processed, {error_count} errors")

    # Final commit
    conn.commit()
    print(f"Processing complete: {processed_count} files processed, {error_count} errors")

    conn.close()



def parse_nfdump_output(output):
    """Parse nfdump output and return a dictionary of values"""
    data = {}
    for line in output.split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip().lower()
            try:
                data[key] = int(value.strip())
            except ValueError:
                data[key] = value.strip()
    return data

def process_netflow_file(conn, file_path, router, timestamp_unix):
    """Process a single netflow file and insert data into database"""
    command = ["nfdump", "-I", "-r", file_path]
    
    try:
        result = subprocess.run(command, capture_output=True, text=True)
        
        if result.returncode == 0:
            data = parse_nfdump_output(result.stdout)
            
            
            # Insert data into database
            conn.execute("""
                INSERT OR REPLACE INTO netflow_stats (
                    file_path, router, timestamp,
                    flows, flows_tcp, flows_udp, flows_icmp, flows_other,
                    packets, packets_tcp, packets_udp, packets_icmp, packets_other,
                    bytes, bytes_tcp, bytes_udp, bytes_icmp, bytes_other,
                    first_timestamp, last_timestamp, msec_first, msec_last, sequence_failures
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                file_path, router, timestamp_unix,
                data.get('flows', 0),
                data.get('flows_tcp', 0),
                data.get('flows_udp', 0),
                data.get('flows_icmp', 0),
                data.get('flows_other', 0),
                data.get('packets', 0),
                data.get('packets_tcp', 0),
                data.get('packets_udp', 0),
                data.get('packets_icmp', 0),
                data.get('packets_other', 0),
                data.get('bytes', 0),
                data.get('bytes_tcp', 0),
                data.get('bytes_udp', 0),
                data.get('bytes_icmp', 0),
                data.get('bytes_other', 0),
                data.get('first', 0),
                data.get('last', 0),
                data.get('msec_first', 0),
                data.get('msec_last', 0),
                data.get('sequence_failures', 0)
            ))
            
            print(f"Processed {file_path}: {data.get('flows', 0)} flows")
            return True
        else:
            print(f"Error processing {file_path}: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Exception processing {file_path}: {e}")
        return False


if __name__ == "__main__":
    main()