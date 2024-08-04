#!/usr/bin/env python3
from rich.traceback import install
install()
from rich import print

import sqlite3, io, http, time, tempfile, subprocess
from pathlib import Path, PurePosixPath
from urllib.parse import quote_plus
from PIL import Image, UnidentifiedImageError
import urllib3
assert urllib3.__version__ > "2"
import requests
from requests.adapters import HTTPAdapter, Retry
s = requests.Session()
_retry = Retry(backoff_factor=1, status_forcelist=[502,], allowed_methods=None)
s.mount('http://', HTTPAdapter(max_retries=_retry))
s.mount('https://', HTTPAdapter(max_retries=_retry))


from config import local_root
local_root = local_root / 'pixiv'


def update_thumb(source: str, db: sqlite3.Connection, target_size=5*2**20):
    print(source)
    local, = db.execute('SELECT local FROM pixiv WHERE source = ?', (source,)).fetchone()

    origin_path = local_root / local
    origin_name = PurePosixPath(local).name
    if origin_path.suffix.lower() == '.jxl':
        _jpg_tmp_file = tempfile.NamedTemporaryFile(suffix='.jpg')
        subprocess.run(["djxl", str(origin_path), _jpg_tmp_file.name], check=True)
        origin_path = Path(_jpg_tmp_file.name)
        origin_name = str(Path(local).with_suffix('.jpg').name)
        _jxl = True
    else:
        _jxl = False
    
    origin_size = origin_path.stat().st_size
    with Image.open(origin_path) as im:
        if origin_size < target_size:
            thumb_file = open(origin_path, 'rb')
            filename = origin_name
        else:
            reduce_factor = 0
            thumb_size = target_size # force start
            while thumb_size >= target_size:
                reduce_factor += 1
                im_tmp = im.reduce(reduce_factor) if reduce_factor != 1 else im
                thumb_file = io.BytesIO()
                im_tmp.save(thumb_file, 'WEBP', method=6)
                thumb_size = len(thumb_file.getvalue())
                print(f"{origin_size/2**10:.1f} KB reduced {reduce_factor} times to {thumb_size/2**10:.1f} KB")
            thumb_file.seek(0)
            filename = str(Path(origin_name).with_suffix('.webp'))
    
    r = s.post("https://p.sda1.dev/api_dup2/v1/upload_external_noform?filename="+quote_plus(filename), data=thumb_file)
    
    if _jxl: _jpg_tmp_file.close()

    if r.status_code != http.HTTPStatus.OK:
        print(r.text)
        raise RuntimeError
    try:
        j = r.json()
        print(j['message'])
        URL = j['data']['url']
    except KeyError:
        print(r.text)
        raise
    print(URL)

    time.sleep(1)
    while True:
        r = s.get(URL, stream=True)
        r.raw.decode_content = True
        try:
            Image.open(r.raw).verify()
            print('✅\n')
            break
        except (UnidentifiedImageError, urllib3.exceptions.ProtocolError): # This includes IncompleteRead.
            pass
        print('.', end='')
        time.sleep(6)
    
    db.execute('UPDATE pixiv SET thumbnail = ? WHERE source = ?', (URL, source))

if __name__ == "__main__":
    db = sqlite3.connect("pixiv.db")
    try:
        for source, in db.execute('SELECT source FROM pixiv WHERE thumbnail IS NULL'):
            update_thumb(source, db)
    finally:
        db.commit()
        db.close()
