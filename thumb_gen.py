#!/usr/bin/env python3
from rich.traceback import install
install()
from rich import print

import sqlite3, io, http, time, tempfile, subprocess, base64, mimetypes, os
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
s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:130.0) Gecko/20100101 Firefox/130.0"})


from config import local_root
local_root = local_root / 'pixiv'


def update_thumb(rowid: str, db: sqlite3.Connection, image_data=None, image_suffix='.png', target_size=5*2**20):
    print(rowid)
    local, = db.execute('SELECT local FROM pixiv WHERE rowid = ?', (rowid,)).fetchone()

    if image_data is None:
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
        origin_image = open(origin_path, 'rb')
    else:
        origin_name = PurePosixPath(local).with_suffix(image_suffix).name
        origin_image = io.BytesIO(image_data)
        origin_size = len(image_data)
    
    with Image.open(origin_image) as im:
        if origin_size < target_size:
            thumb_buffer = origin_image
            filename = origin_name
        else:
            if im.mode == 'RGBA':  # cannot write mode RGBA as JPEG
                im = im.convert('RGB')
            reduce_factor = 0
            thumb_size = target_size # force startj
            while thumb_size >= target_size:
                reduce_factor += 1
                im_tmp = im.reduce(reduce_factor) if reduce_factor != 1 else im
                thumb_buffer = io.BytesIO()
                im_tmp.save(thumb_buffer, 'JPEG', optimize=True)#'WEBP', method=6)
                thumb_size = len(thumb_buffer.getvalue())
                print(f"{origin_size/2**10:.1f} KB reduced {reduce_factor} times to {thumb_size/2**10:.1f} KB")
            filename = str(Path(origin_name).with_suffix('.jpg'))
    thumb_buffer.seek(0)
    
    # r = s.post("https://p.sda1.dev/api_dup2/v1/upload_external_noform?filename="+quote_plus(filename), data=thumb_file)
    r = s.post(
        "https://member.bilibili.com/x/vu/web/cover/up",
        data={
            'csrf': os.environ['csrf'],
            'cover': b'data:' + mimetypes.guess_type(filename, strict=False)[0].encode('UTF-8') + b';base64,' + base64.b64encode(thumb_buffer.getvalue()),
        },
        cookies={'SESSDATA': os.environ['SESSDATA']}
    )
    # https://github.com/SocialSisterYi/bilibili-API-collect/pull/1066/files#diff-36446fff007e20642f1fe9dd135046f63817bfa63a6589426821a9f421b558f5
    
    if image_data is None and _jxl: _jpg_tmp_file.close()

    if r.status_code != http.HTTPStatus.OK:
        print(r.text)
        raise RuntimeError
    try:
        j = r.json()
        assert j['code'] == 0, r.text
        print(j['message'])
        URL = j['data']['url']
        if URL.startswith('http://'):
            URL = 'https://' + URL[:len('http://')]
        URL += '@progressive'
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
    
    db.execute('UPDATE pixiv SET thumbnail = ? WHERE rowid = ?', (URL, rowid))

if __name__ == "__main__":
    db = sqlite3.connect("pixiv.db")
    try:
        for source, in db.execute('SELECT source FROM pixiv WHERE thumbnail IS NULL'):
            update_thumb(source, db)
    finally:
        db.commit()
        db.close()
