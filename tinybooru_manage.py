#!/usr/bin/env python3
from typing import Callable
import time, sqlite3, re, argparse, os, tempfile, shutil
from pathlib import Path, PurePath
from urllib.parse import unquote

from rich.traceback import install
install()
from rich import print
pprint = print
from rich.prompt import Confirm, Prompt

from thumb_gen import update_thumb
from utils import image_verify, get_metadata, url2source
from tinybooru_image import TinyBooruImage
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
from requests.adapters import HTTPAdapter, Retry
s = requests.Session()
retries = Retry(backoff_factor=1)
s.mount('http://', HTTPAdapter(max_retries=retries))
s.mount('https://', HTTPAdapter(max_retries=retries))
# s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:125.0) Gecko/20100101 Firefox/125.0"})


def pull_picture(source_site: str, source_id: str, from_: str = None):
    """
    from_: 指定的实际从该 URL 网页获取，pixiv 不填写
    """
    print(f'pulling {source_site}:{source_id}')

    metadata = get_metadata(source_site, source_id) # 填充 site, source_url, title, caption, site_tags: list[str]

    metadata[source_site] = source_id
    metadata['from'] = from_
    
    if 'i.pximg.net' in metadata['source_url']:
        metadata['local'] = source_id + Path(os.path.basename(metadata['source_url'])).suffix
        if '_p' in source_id:
            pixiv_num = source_id.partition('_p')[0]
            assert pixiv_num.isdecimal()
            metadata['local'] = pixiv_num + '/' + metadata['local']
        r = s.get(metadata['source_url'].replace('i.pximg.net', 'o.acgpic.net'), headers={"Referer": "https://pixivic.com/illusts/114514?VNK=da32baf"}, timeout=(15, 300))
    else:
        metadata['local'] = unquote(os.path.basename(metadata['source_url']))
        for _ in range(3):
            try:
                r = s.get(metadata['source_url'], timeout=(15, 300))
                break
            except requests.exceptions.ChunkedEncodingError:
                time.sleep(7)
        else:
            raise
    r.raise_for_status()

    store_image(r.content, metadata)


def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}

def exists(metadata: dict):
    cursor = db.cursor()
    cursor.row_factory = dict_factory

    if (p_id := metadata.get('pixiv')) is not None:
        if p_id.isdecimal():
            q_pid = p_id + '_p0'
        elif p_id.endswith('_p0') or p_id.endswith('_p?'):
            q_pid = p_id[:-3]
        else:
            q_pid = None
        if q_pid is not None and (ret := cursor.execute('SELECT * FROM pixiv WHERE pixiv = ?', (q_pid,)).fetchone()) is not None:
            return ret
    
    q = tuple(zip(*(
        (site, metadata[site])
        for site in ('pixiv', 'twitter', 'yandere', 'danbooru', 'gelbooru', 'zerochan', 'unique_source')
        if metadata.get(site) is not None
    )))
    if q:
        q_site, q_id = q
        if (ret := cursor.execute(f"SELECT * FROM pixiv WHERE {' OR '.join(s+' = ?' for s in q_site)}", q_id).fetchone()) is not None:
            return ret
        elif (unique_source := metadata.get('unique_source')) is not None:
            if (ret := cursor.execute('SELECT * FROM pixiv WHERE nonunique_source = ?', (unique_source,)).fetchone()) is not None:
                return ret
    else: # if find the unique_source in nonunique_source, warn
        if (nonunique_source := metadata.get('nonunique_source')) is not None:
            if (ret_nus := cursor.execute('SELECT * FROM pixiv WHERE unique_source = ?', (nonunique_source,)).fetchone()) is not None:
                return ret_nus
            if (ret_nus := cursor.execute('SELECT * FROM pixiv WHERE nonunique_source = ?', (nonunique_source,)).fetchone()) is not None:
                print('[yellow]WARNING[/yellow]: find existing nonunique_source:')
                print(ret_nus)
        return None


def store_image(media: bytes | Path | Callable, metadata: dict):

    if (row := exists(metadata)) is not None:
        raise FileExistsError(f"已存在！\n{row}")
    
    if 'local' not in metadata and isinstance(media, PurePath):
        metadata['local'] = media.name
    
    tinybooru_image = TinyBooruImage(metadata)

    if callable(media):
        temp_dir = Path(tempfile.gettempdir()) / 'tmp_tinybooru'
        temp_dir.mkdir(parents=False, exist_ok=False)
        tmp_video_path = temp_dir / metadata['local']
        tmp_video_path.parent.mkdir(parents=True, exist_ok=True)
        media(str(tmp_video_path))
        tinybooru_image.fetch_media(tmp_video_path)
    else:
        tinybooru_image.fetch_media(media)
    
    tinybooru_image.calc_hash()

    with tinybooru_image.thumb(target_size=5*2**20) as (thumb_buffer, thumb_filename):
        tinybooru_image.process_tags(thumb_buffer)

        tinybooru_image.save_file()
        if callable(media):
            shutil.rmtree(temp_dir)
    
        display_metadata = dict(tinybooru_image.metadata)
        del display_metadata['CLIP_hash']
        del display_metadata['ORB_hash']
        pprint(display_metadata)

        cursor = db.cursor()
        cursor.execute(f"INSERT INTO pixiv VALUES({', '.join(':'+k for k in tinybooru_image.metadata.keys())})", tinybooru_image.metadata)
        
        update_thumb(cursor.lastrowid, db, thumb_buffer, thumb_filename)
    db.commit()



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser_group = parser.add_mutually_exclusive_group()
    parser_group.add_argument("-p", "--pixiv", action='store_true', help="获取pixiv收藏")
    parser_group.add_argument("-a", "--add", type=str, help="从 URL 或 88888888_p0,2,3 (pixiv) 添加图片")
    parser_group.add_argument("-t", "--twitter", type=str, help="从 twiiter 导出的 json 文件导入")
    parser_group.add_argument("-l", "--local", type=str, help="添加本地图片")
    parser_group.add_argument("-d", "--delete", type=str, help="删除，例：pixiv:88888888_p0")
    args = parser.parse_args()

    if args.add is not None:
        if args.add.startswith('http'):
            pull_picture(*url2source(args.add), args.add)
        elif re.fullmatch(r'\d+_p(?:\d+,)*\d+', args.add, re.ASCII) is not None:
            illust_id, _, pixiv_is = args.add.partition('_p')
            for pixiv_i in pixiv_is.split(','):
                pull_picture('pixiv', f"{illust_id}_p{pixiv_i}")
        else:
            raise NotImplementedError(args.add)
    elif args.local is not None:
        from_ = Prompt.ask('from', default=None)
        source_site, source_id = url2source(Prompt.ask('source'))
        store_image(
            Path(args.local),
            {
                source_site: source_id,
                'from': from_,
                'source_url': Prompt.ask('source_url', default=None),
                'title': Prompt.ask('title', default=None),
                'caption': Prompt.ask('caption', default=None),
            }
        )
    elif args.pixiv:
        from providers.pixiv import Papi
        next_qs = {'user_id': Papi.user_id}
        while next_qs != None:
            json_result = Papi.user_bookmarks_illust(**next_qs)
            for illust in json_result['illusts']:
                p_id = str(illust['id'])
                print('testing', p_id)
                if db.execute('SELECT pixiv FROM pixiv WHERE pixiv = ?', (p_id,)).fetchone() is None and db.execute('SELECT pixiv FROM pixiv WHERE pixiv = ?', (p_id+'_p0',)).fetchone() is None:
                    pull_picture('pixiv', p_id+'_p0' if illust['meta_pages'] else p_id)
                    db.commit()
                    final_skip = False
                else:
                    final_skip = True
            if final_skip:
                break
            else:
                next_qs = Papi.parse_qs(json_result.next_url)
    elif args.twitter:
        from providers.twitter import twitter_generator
        for image_data, metadata in twitter_generator(Path(args.twitter), exists):
            store_image(image_data, metadata)
    elif args.delete is not None:
        source_site, _, source_id = args.delete.partition(':')
        if (record := exists({source_site: source_id})) is not None:
            print(record)
            if Confirm.ask('删除上面这一记录?'):
                if source_site == 'pixiv':
                    from providers.pixiv import Papi
                    illust_id, _, pixiv_i = source_id.partition('_p')
                    if pixiv_i == '' or pixiv_i == '0':
                        pprint(Papi.illust_bookmark_delete(illust_id))
                (local_root / record['local']).unlink()
                db.execute(f"DELETE FROM pixiv WHERE {source_site} == ?", (source_id,))
                db.commit()
        else:
            print('未在数据库中找到记录')

    db.close()
