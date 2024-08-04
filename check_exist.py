import sqlite3

from config import local_root, sqlite3_path
local_root = local_root / 'pixiv'
db = sqlite3.connect(sqlite3_path)

for source, local_path in db.execute('SELECT source, local FROM pixiv'):
    assert (local_root).exists(), f"{source} : {local_path} lost!"
