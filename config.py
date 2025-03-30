import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ModuleNotFoundError:
    pass


local_root = Path(os.environ['local_root'])
assert local_root.exists()

sqlite3_path = os.environ['sqlite3_path']

ffmpeg = str(Path(os.environ['ffmpeg_path']).expanduser())

