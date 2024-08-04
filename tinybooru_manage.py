#!/usr/bin/env python3

import functools
from io import BytesIO
import json, sqlite3, re, argparse, os, tempfile, subprocess, filecmp, http, shutil
from pathlib import Path, PurePath
from pprint import pprint
from urllib.parse import unquote
from html import unescape
from PIL import Image, UnidentifiedImageError

from rich.traceback import install
install()
from rich import print

from jxl import jxl
from thumb_gen import update_thumb
from utils import image_verify, get_metadata, url2source

from config import local_root, sqlite3_path
local_root = local_root / 'pixiv'
db = sqlite3.connect(sqlite3_path)

# try:
#     import httpx
#     s = httpx.Client(transport=httpx.HTTPTransport(retries=15), follow_redirects=True, http2=True)
# except ModuleNotFoundError:
import urllib3
assert urllib3.__version__ > "2"
import requests
from requests.adapters import HTTPAdapter
s = requests.Session()
s.mount('http://', HTTPAdapter(max_retries=15))
s.mount('https://', HTTPAdapter(max_retries=15))
# s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:125.0) Gecko/20100101 Firefox/125.0"})


def pull_picture(source: str, from_: str=None):
    """
    from_: 指定的实际从该 URL 网页获取，pixiv 不填写
    """
    print('pulling', source)
    # 开始时，source只能算是from
    source_site, _, source_id = source.partition(':')
    if source_site in {'pixiv', 'yandere', 'danbooru', 'gelbooru', 'zerochan'}:
        metadata = get_metadata(source_site, source_id) # 填充 source, source_url, title, caption, tags: list[str]
    else:
        raise NotImplementedError(f"{source} 不是被支持的自动pull类型，请检查或从本地添加")
    if 'source' in metadata:
        print("探测到上游source，以其作为source:", metadata['source'])
    else:
        metadata['source'] = source
    
    if source_site != 'pixiv': # pixiv 不填写 from
        assert from_ is not None
        metadata['from'] = from_
    
    metadata['tags'] = ', '.join(metadata['tags'])

    if 'i.pximg.net' in metadata['source_url']:
        metadata['local'] = source_id + Path(os.path.basename(metadata['source_url'])).suffix
        if '_p' in source_id:
            pixiv_num = source_id.partition('_p')[0]
            assert pixiv_num.isdigit()
            metadata['local'] = pixiv_num + '/' + metadata['local']
        r = s.get(metadata['source_url'].replace('i.pximg.net', 'o.acgpic.net'), headers={"Referer": "https://pixivic.com/illusts/114514?VNK=da32baf"}, timeout=300)
    else:
        metadata['local'] = unquote(os.path.basename(metadata['source_url']))
        r = s.get(metadata['source_url'], timeout=300)
    r.raise_for_status()

    store_image(r.content, metadata)

def exists(metadata: dict):
    return db.execute('SELECT source FROM pixiv WHERE source = ?', (metadata['source'],)).fetchone() is not None


def store_image(image_data: bytes, metadata: dict):
    if exists(metadata):
        raise FileExistsError(f"{metadata['source']} 已存在！")
    
    image_verify(image_data)
    fpath = local_root / metadata['local']
    fpath.parent.mkdir(exist_ok=True)
    fpath.write_bytes(image_data)
    metadata['local'] = PurePath(metadata['local']).with_name(jxl(fpath).name).as_posix()
    pprint(metadata)

    db.execute(
        "INSERT INTO pixiv VALUES(:source, :from, :source_url, :local, :title, :caption, :tags, NULL)",
        {'from': None, 'title': None, 'caption': None} | metadata
    )
    
    update_thumb(metadata['source'], db)
    db.commit()



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser_group = parser.add_mutually_exclusive_group()
    parser_group.add_argument("-p", "--pixiv", action='store_true', help="获取pixiv收藏")
    parser_group.add_argument("-a", "--add", type=str, help=r"从URL或pixiv:\d+添加图片")
    parser_group.add_argument("-t", "--twitter", type=str, help="从 twiiter 导出的 json 文件导入")
    parser_group.add_argument("-l", "--local", type=str, help="添加本地图片(http:)")
    parser_group.add_argument("-d", "--delete", type=str, help="删除Pixiv图片，例：pixiv:88888888_p0")
    args = parser.parse_args()

    if args.add != None:
        if args.add.startswith('http'):
            pull_picture(url2source(args.add), args.add)
        elif args.add.startswith('pixiv:'):
            source_site, _, source_id = args.add.partition(':')
            illust_id, _, pixiv_is = source_id.partition('_p')
            if pixiv_is == '' or ',' not in pixiv_is:
                pull_picture(args.add)
            else:
                for pixiv_i in pixiv_is.split(','):
                    pull_picture(f"pixiv:{illust_id}_p{pixiv_i}")
        else:
            raise NotImplementedError(args.add)
    elif args.local != None:
        from_ = input('来源URL(用于source, from) ')
        source = url2source(from_)
        source_site, _, source_id = source.partition(':')
        assert source_site in ('http', 'https')
        fpath = local_root / Path(args.local).name
        shutil.move(args.local, fpath)
        db.execute("INSERT INTO pixiv VALUES(:source, :from, :source_url, :local, :title, :caption, :tags)", {
            "source": source,
            "from": from_,
            "source_url": None,
            "local": jxl(fpath).name,
            "title": None,
            "caption": None,
            "tags": '',
        })
    elif args.pixiv:
        from providers.pixiv import Papi
        next_qs = {'user_id': Papi.user_id}
        while next_qs != None:
            json_result = Papi.user_bookmarks_illust(**next_qs)
            for illust in json_result['illusts']:
                source = f"pixiv:{illust['id']}"
                print('testing', source)
                if db.execute('SELECT source FROM pixiv WHERE source = ?', (source,)).fetchone() is None and db.execute('SELECT source FROM pixiv WHERE source = ?', (source+'_p0',)).fetchone() is None:
                    if bool(illust['meta_pages']): source += '_p0'
                    pull_picture(source)
                    db.commit()
            break # TODO
            next_qs = AppPixivAPI.parse_qs(json_result.next_url)
    elif args.twitter:
        from providers.twitter import twitter_generator
        for image_data, metadata in twitter_generator(Path(args.twitter), exists):
            store_image(image_data, metadata)
    elif not args.delete is None:
        local, = db.execute("SELECT local FROM pixiv WHERE source == ?", (args.delete,)).fetchone()
        source_site, _, source_id = args.delete.partition(':')
        if source_site == 'pixiv':
            illust_id, _, pixiv_i = source_id.partition('_p')
            if pixiv_i == '' or pixiv_i == '0':
                pprint(Papi.illust_bookmark_delete(illust_id))
        (local_root / local).unlink()
        db.execute("DELETE FROM pixiv WHERE source == ?", (args.delete,))

    db.close()
