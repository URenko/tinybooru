#!/usr/bin/env python3
import http.cookies
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

from tinybooru_image import TinyBooruImage
from config import local_root
local_root = local_root / 'pixiv'

s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:130.0) Gecko/20100101 Firefox/130.0", 'Cookie': os.environ['bili_cookies']})

def update_thumb(rowid: str, db: sqlite3.Connection, thumb_buffer, filename):
    # r = s.post("https://p.sda1.dev/api_dup2/v1/upload_external_noform?filename="+quote_plus(filename), data=thumb_file)
    cookies = http.cookies.SimpleCookie(os.environ['bili_cookies'])
    r = s.post(
        "https://member.bilibili.com/x/vu/web/cover/up",
        data={
            'csrf': cookies["bili_jct"].value,
            'cover': b'data:' + mimetypes.guess_type(filename, strict=False)[0].encode('UTF-8') + b';base64,' + base64.b64encode(thumb_buffer.getvalue()),
        },
    )
    # https://github.com/SocialSisterYi/bilibili-API-collect/pull/1066/files#diff-36446fff007e20642f1fe9dd135046f63817bfa63a6589426821a9f421b558f5

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
