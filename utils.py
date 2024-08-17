import functools, mimetypes, re, urllib.parse
from collections import namedtuple
from io import BytesIO
from pathlib import Path
from pprint import pprint
from html import unescape
from PIL import Image, UnidentifiedImageError
from rich.traceback import install
install()
from rich import print
from rich.prompt import Prompt, IntPrompt, Confirm
from imagehash import ImageHash

# try:
#     import httpx
#     s = httpx.Client(transport=httpx.HTTPTransport(retries=15), follow_redirects=True, http2=True)
# except ModuleNotFoundError:
import urllib3
assert urllib3.__version__ > "2"
import requests
from requests.adapters import HTTPAdapter, Retry
s = requests.Session()
s.mount('http://', HTTPAdapter(max_retries=Retry(backoff_factor=1)))
s.mount('https://', HTTPAdapter(max_retries=Retry(backoff_factor=1)))
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
    # if illust['tags']:
    #     assert 'translated_name' in illust['tags'][0]
    return {
        'custom_tags': [f"🔞:{illust['sanity_level']}", f"©:{illust['user']['name']}${illust['user']['id']}@{illust['user']['account']}"],
        'pixiv_tags': [tag['name'] for tag in illust['tags']],
        # 'translated_tags': [tag['name'] if tag['translated_name'] is None else tag['translated_name'] for tag in illust['tags']],
        'title': illust['title'],
        'caption': illust['caption'],
        'source_url': pximg_url
    }


@functools.cache
def get_yandere_metadata(id: str):
    j = s.get(f"https://yande.re/post.json?tags=id:{id}").json()[0]
    ret =  {
        'booru_tags': list(j['tags'].split(' ')),
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
        'booru_tags': list(j['tag_string'].split(' ')),
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
        'booru_tags': list(j['tags'].split(' ')),
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
        'booru_tags': j['tags'],
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
    metadata[site] = id
    if 'source' in metadata:
        source = url2source(metadata.pop('source'), coarse)
        if not coarse:
            print('探测到上游 source:', source)
        metadata[source.site] = source.id
    return metadata

Source = namedtuple('source', ['site', 'id'])

def url2source(url: str, coarse=False):
    """
    coarse: twiiter 和 pixiv 之 index 无需准确，并静默忽略未知源（作为 nonunique_source）
    """
    if re.match(r'https?://(?:twitter|x)\.com/', url) is not None:
        if not coarse:
            assert Confirm.ask(f'source 为 twitter < {url} >，仅在已散佚的情况下才可通过此法添加，确认？')
    if not (m := re.fullmatch(r'https?://(?:twitter|x)\.com/(\w+?)/(?:web/)?status/(\d+)/photo/(\d)', url)) is None:
        return Source('twitter', f'{m[2]}#{m[3]}')
    elif not (m := re.fullmatch(r'https?://(?:twitter|x)\.com/(\w+?)/(?:web/)?status/(\d+)/?', url)) is None:
        num = IntPrompt.ask('告诉我这是第几张? (从1计数，默认未知) ', default='?') if not coarse else '?'
        return Source('twitter', f'{m[2]}#{num}')
    elif not (m := re.fullmatch(r'https?://yande\.re/post/show/(\d+)', url)) is None:
        return Source('yandere', m[1])
    elif not (m := re.fullmatch(r'https?://danbooru\.donmai\.us/posts/(\d+)', url)) is None:
        return Source('danbooru', m[1])
    elif not (m := re.fullmatch(r'https?://danbooru\.donmai\.us/post/show/(\d+)', url)) is None:
        return Source('danbooru', m[1])
    elif not (m := re.fullmatch(r'https?://gelbooru\.com/index\.php\?page=post&s=view&id=(\d+)', url)) is None:
        return Source('gelbooru', m[1])
    elif not (m := re.fullmatch(r'https?://www\.zerochan\.net/(\d+)', url)) is None:
        return Source('zerochan', m[1])
    elif not (m := re.fullmatch(r'https?://static\.zerochan\.net/\W+?\.(\d)+\.\W+', url)) is None:
        return Source('zerochan', m[1])
    elif not (m := re.fullmatch(r'https?://www\.zerochan\.net/(\d)+', url)) is None:
        return Source('zerochan', m[1])
    elif not (m := re.fullmatch(r'https?://www\.pixiv\.net/(?:en/)?artworks/(\d+)', url)) is None:
        return Source('pixiv', m[1] + ('_p?' if coarse else Prompt.ask('使用 pixiv', default='_p?')))
    elif not (m := re.fullmatch(r'https?://www\.pixiv\.net/member_illust\.php\?mode=medium&illust_id=(\d+)', url)) is None:
        return Source('pixiv', m[1] +('_p?' if coarse else Prompt.ask('使用 pixiv', default='_p?')))
    elif not (m := re.fullmatch(r'https?://i\d?.(?:pximg|pixiv).net/img-original/img/[\d/]+?/(\w+?)\.\w+', url)) is None:
        if not coarse and m[1].endswith('_p0'):
            return Source('pixiv', Prompt.ask('使用 pixiv', choices=[m[1], m[1].removesuffix('_p0')]))
        return Source('pixiv', m[1])
    elif (mime := mimetypes.guess_type(url, strict=False)[0]) is not None and mime.startswith('image/'):
        return Source('unique_source', url)
    elif coarse:
        return Source('nonunique_source', url)
    else:
        if Confirm.ask('未知类型的 URL '+url+' ，直接作为 source，它是唯一的吗?'):
            return Source('unique_source', url)
        else:
            return Source('nonunique_source', url)


quote_all = functools.partial(urllib.parse.quote, safe='')

@functools.cache
def tag_other_to_booru(other: str):
    r = s.get('https://danbooru.donmai.us/wiki_pages.json?search[other_names_match]='+quote_all(other), timeout=9)
    r.raise_for_status()
    j = r.json()
    if j:
        return j[0]['title']
    else:
        return None

@functools.cache
def booru_tag_detail(tag: str):
    r = s.get('https://danbooru.donmai.us/wiki_pages.json?search[title]='+quote_all(tag), timeout=9)
    r.raise_for_status()
    j = r.json()
    if j:
        assert j[0]['title'] == tag, tag
    return j

@functools.cache
def tag_pixiv_translation(tag_ja: str):
    r = s.get('https://www.pixiv.net/ajax/search/tags/'+quote_all(tag_ja), headers={'Accept-Language': 'zh-CN,zh'}, timeout=9)
    r.raise_for_status()
    j = r.json()
    if tag_ja in j['body']['tagTranslation'] and 'zh' in j['body']['tagTranslation'][tag_ja] and j['body']['tagTranslation'][tag_ja]['zh']:
        return j['body']['tagTranslation'][tag_ja]['zh']
    else:
        return None

def process_tags(metadata: dict[str, list[str]]):
    # please keep the order
    metadata['romanized_tags'] = metadata['custom_tags'].copy()
    metadata['translated_tags'] = metadata['custom_tags'].copy()
    for tag in metadata['pixiv_tags']:
        booru_tag = tag_other_to_booru(re.sub(r'\d+users入り$', '', tag, count=1))
        if booru_tag is not None:
            metadata['romanized_tags'].append(booru_tag)
        else:
            metadata['romanized_tags'].append(tag)
        translated_tag = tag_pixiv_translation(tag)
        if translated_tag is not None:
            metadata['translated_tags'].append(translated_tag)
        else:
            metadata['translated_tags'].append(tag)
    metadata['romanized_tags'] += metadata['booru_tags']
    for tag in metadata['booru_tags']:
        if (detail := booru_tag_detail(tag)):
            other_names = detail[0]['other_names']
            if other_names:
                if (translated_tag := tag_pixiv_translation(other_names[0])) is not None:
                    metadata['translated_tags'].append(translated_tag)
                else:
                    metadata['translated_tags'].append(other_names[0])
            else: # Ambiguous tag, e.g. miku
                metadata['translated_tags'].append(tag)
        else: # only roma
            metadata['translated_tags'].append(tag)

    metadata['custom_tags'] = ', '.join(metadata['custom_tags'])
    metadata['pixiv_tags'] = ', '.join(metadata['pixiv_tags'])
    metadata['booru_tags'] = ', '.join(metadata['booru_tags'])
    metadata['romanized_tags'] = ', '.join(metadata['romanized_tags'])
    metadata['translated_tags'] = ', '.join(metadata['translated_tags'])


def ImageHash2int(imagehash: ImageHash, signed=True):
    ret = sum([2**i for i, v in enumerate(imagehash.hash.flatten()) if v])
    if signed:
        return int.from_bytes(ret.to_bytes(length=8, byteorder='big', signed=False), byteorder='big', signed=True)
    else:
        return ret

