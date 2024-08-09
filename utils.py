import functools
from io import BytesIO
import json, sqlite3, re, argparse, os, tempfile, subprocess, filecmp, http, shutil
from pathlib import Path
from pprint import pprint
from urllib.parse import unquote
from html import unescape
from PIL import Image, UnidentifiedImageError
from rich.traceback import install
install()
from rich import print

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


def image_verify(img_content):
    try:
        image = Image.open(BytesIO(img_content))
    except UnidentifiedImageError:
        Path('/tmp/tmp').write_bytes(img_content)
        print('Saved to /tmp/tmp .')
        raise
    image.verify()
    return image
    # if image.format.lower() == 'jpeg':  # 有些JPG尾部有额外字节，如 https://yande.re/post/show/107805 ，libjxl会确保JPG的正确
    #     if not img_content.rstrip(b'\0\r\n').endswith(b'\xff\xd9'): raise NotImplementedError('未知的JPEG格式')

@functools.cache
def cached_pixiv_illust_detail(illust_id):
    from providers.pixiv import Papi
    return Papi.illust_detail(illust_id)

def get_pixiv_metadata(pixiv_id: str):
    illust_id, _, pixiv_i = pixiv_id.partition('_p')
    _p = cached_pixiv_illust_detail(illust_id)
    if 'error' in _p:
        pprint(_p)
        raise NotImplementedError
    illust = _p['illust']
    pixiv_i_y = bool(illust['meta_pages'])
    assert pixiv_i_y == (pixiv_i != '')
    pximg_url = illust['meta_pages'][int(pixiv_i)]['image_urls']['original'] if pixiv_i_y else illust['meta_single_page']['original_image_url']
    if not pximg_url.startswith('https://i.pximg.net'):
        pprint(_p)
        return
    return {
        'tags': [tag['name'] for tag in illust['tags']] + [f"🔞:{illust['sanity_level']}", f"©:{illust['user']['name']}${illust['user']['id']}@{illust['user']['account']}"],
        'title': illust['title'],
        'caption': illust['caption'],
        'source_url': pximg_url
    }


@functools.cache
def get_yandere_metadata(id: str):
    j = s.get(f"https://yande.re/post.json?tags=id:{id}").json()[0]
    ret =  {
        'tags': list(j['tags'].split(' ')),
        'source_url': j['file_url'],
    }
    if j['source'] != "":
        if not j["source"].startswith('http'):
            ret['caption'] = j["source"]
        else:
            ret['source'] = j['source']
    return ret

@functools.cache
def get_danbooru_metadata(id: str):
    j = s.get(f"https://danbooru.donmai.us/posts/{id}.json").json()  # , headers={"User-Agent": 'curl/7.74.0'}  https://github.com/mikf/gallery-dl/issues/3665
    ret = {
        'tags': list(j['tag_string'].split(' ')),
        'source_url': j.get('file_url') # https://danbooru.donmai.us/wiki_pages/help%3Ausers , 例: https://danbooru.donmai.us/posts/7245496
    }
    if j['source'] != "":
        if not j["source"].startswith('http'):
            ret['caption'] = j["source"]
        else:
            ret['source'] = j['source']
    return ret

@functools.cache
def get_gelbooru_metadata(id: str):
    j = s.get(f"https://gelbooru.com/index.php?page=dapi&s=post&q=index&json=1&id={id}").json()  # , headers={"User-Agent": 'curl/7.74.0'}
    assert 'post' in j, f"gelbooru:{id} 无 post 字段" # 例子: gelbooru:5412709 被删除, gelbooru:7657521 重定向到首页, https://pbs.twimg.com/media/FbjxCGwaAAA2y1C?format=jpg&name=orig 之 SauceNAO 结果
    j = j['post'][0]
    ret = {
        'tags': list(j['tags'].split(' ')),
        'source_url': j['file_url']
    }
    if j['source'] != "":
        j["source"] = unescape(j["source"])
        if not j["source"].startswith('http'):
            ret['caption'] = j["source"]
        else:
            ret['source'] = j['source']
    return ret

def get_zerochan_metadata(id: str):
    j = s.get(f"https://www.zerochan.net/{id}?json", headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:102.0) Gecko/20100101 Firefox/102.0"}).json()
    ret = {
        'tags': j['tags'],
        'source_url': j['full']
    }
    if "source" in j:
        if not j["source"].startswith('http'):
            ret['caption'] = j["source"]
        else:
            ret['source'] = j['source']
    return ret


def get_metadata(site: str, id: str, coarse=False):
    func_name = f'get_{site}_metadata'
    if func_name not in globals():
        raise NotImplementedError(func_name+' 未实现')
    metadata = globals()[func_name](id)
    if 'source' in metadata:
        metadata['source'] = url2source(metadata['source'], coarse)
    return metadata


def url2source(url: str, coarse=False):
    """
    coarse: twiiter 和 pixiv 之 index 无需准确，并静默忽略未知源（return None）
    """
    if re.match(r'https?://(?:twitter|x)\.com/', url) is not None:
        if not coarse:
            while input(f'source 为 twitter < {url} >，仅在已散佚的情况下才可通过此法添加，确认？[Y] ') != 'Y':
                pass
    if not (m := re.fullmatch(r'https?://(?:twitter|x)\.com/(\w+?)/(?:web/)?status/(\d+)/photo/(\d)', url)) is None:
        return f'twitter:{m[2]}@{m[3]}'
    elif not (m := re.fullmatch(r'https?://(?:twitter|x)\.com/(\w+?)/(?:web/)?status/(\d+)/?', url)) is None:
        num = input('告诉我这是第几张? (从1计数) ') if not coarse else 1
        return f'twitter:{m[2]}@{int(num)}'
    elif not (m := re.fullmatch(r'https?://yande\.re/post/show/(\d+)', url)) is None:
        return f'yandere:{m[1]}'
    elif not (m := re.fullmatch(r'https?://danbooru\.donmai\.us/posts/(\d+)', url)) is None:
        return f'danbooru:{m[1]}'
    elif not (m := re.fullmatch(r'https?://gelbooru\.com/index\.php\?page=post&s=view&id=(\d+)', url)) is None:
        return f'gelbooru:{m[1]}'
    elif not (m := re.fullmatch(r'https?://www\.zerochan\.net/(\d+)', url)) is None:
        return f'zerochan:{m[1]}'
    elif not (m := re.fullmatch(r'https?://static\.zerochan\.net/\W+?\.(\d)+\.\W+', url)) is None:
        return f'zerochan:{m[1]}'
    elif not (m := re.fullmatch(r'https?://www\.zerochan\.net/(\d)+', url)) is None:
        return f'zerochan:{m[1]}'
    elif not (m := re.fullmatch(r'https?://www\.pixiv\.net/artworks/(\d+)', url)) is None:
        return f'pixiv:{m[1]}'  # 假设没有多张
    elif not (m := re.fullmatch(r'https?://www\.pixiv\.net/member_illust\.php\?mode=medium&illust_id=(\d+)', url)) is None:
        return f'pixiv:{m[1]}'  # 假设没有多张
    elif not (m := re.fullmatch(r'https?://i\d?.(?:pximg|pixiv).net/img-original/img/[\d/]+?/(\w+?)\.\w+', url)) is None:
        if not coarse and m[1].endswith('_p0') and (ret := input(f"[pixiv:{m[1]}]? ")) != '':
            return ret
        return f'pixiv:{m[1]}'
    elif coarse:
        return
    else:
        if (ret := input(f"无法解析url，直接用原值作为source? [{url}] ").lower()) not in ('y', ''):
            return ret
        return url
