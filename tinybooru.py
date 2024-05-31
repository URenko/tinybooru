import sqlite3
from pathlib import Path
from enum import Enum

from fastapi import FastAPI, Depends
from fastapi.responses import HTMLResponse

app = FastAPI()

def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}

def get_db():
    db = sqlite3.connect("file:pixiv.db?mode=ro", check_same_thread=False)
    db.row_factory = dict_factory
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
def search(page: int = 0, q: str = '', order: Order = Order.desc, db: sqlite3.Connection = Depends(get_db)):
    sql_order = {
        Order.asc: 'rowid ASC',
        Order.desc: 'rowid DESC',
        Order.random: 'RANDOM()'
    }
    sql_str = []
    sql_parameters = []
    for f in filter(None, map(str.strip, q.split(','))):
        sql_str.append('(tags LIKE ? OR title LIKE ? OR caption LIKE ?)')
        sql_parameters += [f"%{f}%", f"%{f}%", f"%{f}%"]
    return db.execute(
        f"SELECT * FROM pixiv {'WHERE ' if sql_str else ''}{'AND'.join(sql_str)} ORDER BY {sql_order[order]} LIMIT 16 OFFSET ?",
        (*sql_parameters, 16*page,)
    ).fetchall()

@app.get("/tags.json")
def all_tags(db: sqlite3.Connection = Depends(get_db)):
    ret = set()
    for row in db.execute("SELECT tags FROM pixiv"):
        ret.update(row['tags'].split(', '))
    return ret
