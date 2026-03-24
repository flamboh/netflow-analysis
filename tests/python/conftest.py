import os
import sys
from pathlib import Path


os.environ.setdefault('NETFLOW_DB_SKIP_AUTO_INIT', '1')

TOOLS_DIR = Path(__file__).resolve().parents[2] / 'tools' / 'netflow-db'
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))
