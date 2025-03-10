import sqlite3, ast
from pathlib import Path
from enum import Enum
from io import BytesIO
from PIL import Image
import requests
s = requests.Session()
s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:130.0) Gecko/20100101 Firefox/130.0"})

from imagehash import average_hash, phash, dhash, whash
from img_like import hamming_distance, ImageHash2int, CLIP_hash, ORB_hash, wilson_score

from fastapi import FastAPI, Depends, UploadFile, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse

app = FastAPI()

table_name = 'pixiv'

def get_db():
    db = sqlite3.connect("file:pixiv.db?mode=ro", check_same_thread=False, uri=True)
    try:
        yield db
    finally:
        db.close()

def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}

class Order(str, Enum):
    asc = "asc"
    random = "random"
    desc = "desc"

class SearchMethod(str, Enum):
    MAGI = "MAGI"
    average_hash = "average_hash"
    phash = "phash"
    dhash = "dhash"
    whash = "whash"
    CLIP = "CLIP"
    ORB = "ORB"

faiss_index = {}
last_rowid = 0
def build_faiss_index(db:sqlite3.Connection):
    global faiss_index, last_rowid
    
    try:
        import numpy as np
        import faiss
    except ImportError as err:
        print('FAISS is not available:', err)
        return

    if (new_last_rowid := db.execute(f'SELECT rowid FROM {table_name} ORDER BY rowid DESC LIMIT 1').fetchone()[0]) == last_rowid:
        return
    print('Rebuild faiss index...')
    
    for hash_name in ('aHash', 'pHash', 'dHash', 'wHash'):
        data = np.fromiter(
            db.execute(f'SELECT rowid, aHash, pHash, dHash, wHash FROM {table_name}'),
            dtype=np.dtype([('rowid', int), ('aHash', np.int64), ('pHash', np.int64), ('dHash', np.int64), ('wHash', np.int64)])
        )
        faiss_index[hash_name] = faiss.IndexBinaryFlat(8*8)
        faiss_index[hash_name].add(np.frombuffer(data[hash_name].tobytes(order='C'), dtype=np.uint8).reshape((-1, 8)))
        faiss_index['rowid'] = data['rowid']
    
    print('Rebuild CLIP')
    data = np.fromiter(
        map(lambda row: (row[0], np.frombuffer(row[1], dtype=np.float32, count=1024)), db.execute(f'SELECT rowid, CLIP_hash FROM {table_name}')),
        dtype=np.dtype([('rowid', int), ('CLIP', (np.float32, 1024))])
    )
    faiss_index['CLIP'] = faiss.IndexIDMap(faiss.IndexFlatIP(1024))
    faiss_index['CLIP'].add_with_ids(
        np.ascontiguousarray(data['CLIP']),
        np.ascontiguousarray(data['rowid']),
    )

    print('Rebuild ORB')
    ORB_faiss_index_filename = f'ORB_faiss_index_{new_last_rowid}'
    if Path(ORB_faiss_index_filename).exists():
        index_ORB = faiss.read_index_binary(ORB_faiss_index_filename)
    else:
        index_ORB = faiss.read_index_binary('ORB_faiss_index')
        rowids = []
        ORB_hashs = []
        for rowid, ORB_hash in db.execute(f'SELECT rowid, ORB_hash FROM {table_name}'):
            descriptors = np.frombuffer(ORB_hash, dtype=np.uint8).reshape((-1, 32))
            ORB_hashs.append(descriptors)
            rowids.append(np.repeat(rowid, descriptors.shape[0]))
        index_ORB.add_with_ids(
            np.concatenate(ORB_hashs, axis=0),
            np.concatenate(rowids, axis=0)
        )
        faiss.write_index_binary(index_ORB, ORB_faiss_index_filename)
    faiss_index['ORB'] = index_ORB

    print('Finished.')
    last_rowid = new_last_rowid

_db_gen = get_db()
build_faiss_index(next(_db_gen))
_db_gen.close()

def search_image(img_content: BytesIO, method: SearchMethod, db:sqlite3.Connection) -> list[int]:
    global faiss_index
    trad_methods = {
        SearchMethod.average_hash: ('aHash', 11, average_hash),
        SearchMethod.phash: ('pHash', 18, phash),
        SearchMethod.dhash: ('dHash', 18, dhash),
        SearchMethod.whash: ('wHash', 10, whash)
    }
    if method == SearchMethod.MAGI:
        try:
            import numpy as np
            import faiss

            found_rowids = {}
            with Image.open(img_content) as im:
                for hash_name, threshold, hash_f in trad_methods.values():
                    _, D, I = faiss_index[hash_name].range_search(
                        np.frombuffer(np.int64(ImageHash2int(hash_f(im))).tobytes(order='C'), dtype=np.uint8)[np.newaxis,:],
                        threshold
                    )
                    found_rowids[hash_name] = I[np.argsort(D)]
            concatenated = np.concatenate(tuple(found_rowids.values()))
            _, idx, unique_counts = np.unique(concatenated, return_counts=True, return_index=True)
            idx = idx[unique_counts>=2]
            unique_counts = unique_counts[unique_counts>=2]
            idx_idx = np.lexsort((idx, -unique_counts))
            return faiss_index['rowid'][concatenated[idx[idx_idx]]].tolist(), unique_counts[idx_idx].tolist()
        except ImportError:
            with Image.open(img_content) as im:
                im_aHash = ImageHash2int(average_hash(im))
                im_pHash = ImageHash2int(phash(im))
                im_dHash = ImageHash2int(dhash(im))
                im_wHash = ImageHash2int(whash(im))
            found_rowids = [
                rowid for rowid, aHash, pHash, dHash, wHash in db.execute(f'SELECT rowid, aHash, pHash, dHash, wHash FROM {table_name}')
                if hamming_distance(im_aHash, aHash, signed=True) < 11 or hamming_distance(im_pHash, pHash, signed=True) < 18 or hamming_distance(im_dHash, dHash, signed=True) < 18 or hamming_distance(im_wHash, wHash, signed=True) < 10
            ]
            return found_rowids, [-1]*len(found_rowids)
    elif method in trad_methods.keys():
        import numpy as np
        import faiss

        hash_name, threshold, hash_f = trad_methods[method]

        with Image.open(img_content) as im:
            D, I = faiss_index[hash_name].search(
                np.frombuffer(np.int64(ImageHash2int(hash_f(im))).tobytes(order='C'), dtype=np.uint8)[np.newaxis,:],
                128
            )
        return faiss_index['rowid'][I[0]].tolist(), D[0].tolist()
    elif method == SearchMethod.CLIP:
        import numpy as np
        import faiss

        with Image.open(img_content) as im:
            query = CLIP_hash(im)
            D, I = faiss_index['CLIP'].search(
                query,
                128
            )
        return I[0].tolist(), [round(d, 3) for d in D[0]]
    elif method == SearchMethod.ORB:
        import heapq
        from collections import defaultdict
        import numpy as np
        import faiss

        with Image.open(img_content) as im:
            query = ORB_hash(im)
            # faiss_index['ORB'].nprobe = 1
            D, I = faiss_index['ORB'].search(
                query,
                3
            )

        kds = defaultdict(list)
        for label, distance in zip(I, D):
            t = defaultdict(lambda: 256)
            for l, d in zip(label, distance):
                t[l] = min(t[l], d)
            for l, d in t.items():
                kds[l].append(d)

        rowid_to_score = {rowid: score for rowid, v in kds.items() if (score := wilson_score(v)) > 0.2}
        
        sorted_rowid = heapq.nlargest(16, rowid_to_score, key=rowid_to_score.get)

        return sorted_rowid, [round(rowid_to_score[rowid] * 100, 2) for rowid in sorted_rowid]


@app.get("/")
def root():
    return HTMLResponse(content=Path('index.html').read_bytes(), status_code=200)

@app.get("/rebuild_faiss_index")
def rebuild_faiss_index(background_tasks: BackgroundTasks, db: sqlite3.Connection = Depends(get_db)):
    background_tasks.add_task(build_faiss_index, db)

@app.post("/")
def post_root(file: UploadFile, url: str | None = Form(default=None), method: SearchMethod = Form(), db: sqlite3.Connection = Depends(get_db)):
    if file.size > 0:
        found_rowids = search_image(file.file, method, db)
    else:
        r = s.get(url, timeout=15)
        r.raise_for_status()
        found_rowids = search_image(BytesIO(r.content), method, db)
    return RedirectResponse(f"?_ri_s={found_rowids}", status_code=303)

@app.get("/api/list")
def search(page: int = 0, q: str = '', order: Order = Order.desc, _ri_s: str | None = None, unsafe: int = 0, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.row_factory = dict_factory
    if _ri_s is None:
        sql_order = {
            Order.asc: 'rowid ASC',
            Order.desc: 'rowid DESC',
            Order.random: 'RANDOM()'
        }
        sql_str = []
        sql_parameters = []
        for f in filter(None, map(str.strip, q.split(','))):
            sql_str.append('(translated_tags LIKE ? OR title LIKE ? OR caption LIKE ?)')
            sql_parameters += [f"%{f}%", f"%{f}%", f"%{f}%"]
        if not unsafe:
            sql_str.append(r'(custom_tags LIKE "%🔞:2%" OR custom_tags LIKE "%rating:g%")')
        return cursor.execute(
            f"SELECT rowid, pixiv, twitter, yandere, danbooru, gelbooru, zerochan, unique_source, nonunique_source, `from`, source_url, local, title, caption, custom_tags, pixiv_tags, booru_tags, romanized_tags, translated_tags, ML_tags, thumbnail FROM {table_name} {'WHERE ' if sql_str else ''}{' AND '.join(sql_str)} ORDER BY {sql_order[order]} LIMIT 16 OFFSET ?",
            (*sql_parameters, 16*page,)
        ).fetchall()
    else:
        rowids, similarities = ast.literal_eval(_ri_s)
        rowids = rowids[8*page: 8*(page+1)]
        similarities = similarities[8*page: 8*(page+1)]
        ret = cursor.execute(
            f"SELECT rowid, pixiv, twitter, yandere, danbooru, gelbooru, zerochan, unique_source, nonunique_source, `from`, source_url, local, title, caption, custom_tags, pixiv_tags, booru_tags, romanized_tags, translated_tags, ML_tags, thumbnail FROM {table_name} WHERE rowid IN ({','.join(['?']*len(rowids))}) LIMIT 8",
            rowids
        ).fetchall()
        return [next(filter(lambda r: r['rowid'] == rowid, ret)) | {'similarity': similarity} for rowid, similarity in zip(rowids, similarities)]


@app.get("/tags.json")
def all_tags(db: sqlite3.Connection = Depends(get_db)):
    ret = set()
    for translated_tags, in db.execute(f"SELECT translated_tags FROM {table_name}"):
        ret.update(translated_tags.split(', '))
    return ret
