import sqlite3
from PIL import Image
Image.MAX_IMAGE_PIXELS *= 10
from jxlpy import JXLImagePlugin
from rich.progress import track, Progress
from rich.traceback import install
install(show_locals=True)

from config import sqlite3_path, local_root
from img_like import CLIP_hash, ORB_hash

db = sqlite3.connect(sqlite3_path)

columns = set(info[1] for info in db.execute("PRAGMA table_info('pixiv');").fetchall())

print('Original columns:', columns)

db.execute('ALTER TABLE pixiv ADD COLUMN CLIP_hash BLOB')
db.execute('ALTER TABLE pixiv ADD COLUMN ORB_hash BLOB')

total_count = db.execute("SELECT COUNT() FROM pixiv").fetchone()[0]

with Progress() as progress:
    for rowid, local_ in progress.track(db.execute('SELECT rowid, local from pixiv'), total=total_count):
        progress.console.print(rowid, local_, sep='\t')
        with Image.open(local_root / 'pixiv' / local_) as img:
            db.execute(
                'UPDATE pixiv SET CLIP_hash = ?, ORB_hash = ? WHERE rowid = ?',
                (
                    CLIP_hash(img).tobytes(),
                    ORB_hash(img).tobytes(),
                    rowid,
                )
            )

db.commit()
db.close()
