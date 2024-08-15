import sqlite3, itertools
from pathlib import Path
from enum import Enum
from io import BytesIO
from PIL import Image
import requests
s = requests.Session()
s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:130.0) Gecko/20100101 Firefox/130.0"})

from imagehash import average_hash, phash, dhash, whash
from img_like import hamming_distance, ImageHash2int

from fastapi import FastAPI, Depends
from fastapi.responses import HTMLResponse

app = FastAPI()

def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}

def get_db():
    db = sqlite3.connect("file:pixiv.db?mode=ro", check_same_thread=False)
    try:
        yield db
    finally:
        db.close()

class Order(str, Enum):
    asc = "asc"
    random = "random"
    desc = "desc"

@app.get("/")
def root():
    return HTMLResponse(content=Path('index.html').read_bytes(), status_code=200)

@app.get("/api/list")
def search(page: int = 0, q: str = '', order: Order = Order.desc, url: str | None = None, db: sqlite3.Connection = Depends(get_db)):
    sql_order = {
        Order.asc: 'rowid ASC',
        Order.desc: 'rowid DESC',
        Order.random: 'RANDOM()'
    }
    if url is None:
        sql_str = []
        sql_parameters = []
        for f in filter(None, map(str.strip, q.split(','))):
            sql_str.append('(translated_tags LIKE ? OR title LIKE ? OR caption LIKE ?)')
            sql_parameters += [f"%{f}%", f"%{f}%", f"%{f}%"]
        cursor = db.cursor()
        cursor.row_factory = dict_factory
        return cursor.execute(
            f"SELECT rowid, * FROM pixiv {'WHERE ' if sql_str else ''}{'AND'.join(sql_str)} ORDER BY {sql_order[order]} LIMIT 16 OFFSET ?",
            (*sql_parameters, 16*page,)
        ).fetchall()
    else: # PoC, not for performance yet
        r = s.get(url, timeout=15)
        r.raise_for_status()
        with Image.open(BytesIO(r.content)) as im:
            im_aHash = ImageHash2int(average_hash(im))
            im_pHash = ImageHash2int(phash(im))
            im_dHash = ImageHash2int(dhash(im))
            im_wHash = ImageHash2int(whash(im))
        found_rowids = [
            rowid for rowid, aHash, pHash, dHash, wHash in db.execute('SELECT rowid, aHash, pHash, dHash, wHash FROM pixiv')
            if hamming_distance(im_aHash, aHash, signed=True) < 11 or hamming_distance(im_pHash, pHash, signed=True) < 18 or hamming_distance(im_dHash, dHash, signed=True) < 18 or hamming_distance(im_wHash, wHash, signed=True) < 10
        ]
        if len(found_rowids) == 0:
            return []
        cursor = db.cursor()
        cursor.row_factory = dict_factory
        return cursor.execute(
            f"SELECT rowid, * FROM pixiv WHERE {' OR '.join(itertools.repeat('rowid = ?', len(found_rowids)))} ORDER BY {sql_order[order]} LIMIT 16 OFFSET ?",
            (*found_rowids, 16*page,)
        ).fetchall()


@app.get("/tags.json")
def all_tags(db: sqlite3.Connection = Depends(get_db)):
    ret = set()
    for translated_tags, in db.execute("SELECT translated_tags FROM pixiv"):
        ret.update(translated_tags.split(', '))
    return ret
