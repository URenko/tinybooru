import io, time, pickle, http, json, re, lzma, fcntl, tempfile, subprocess
from typing import Callable
from pathlib import Path, PurePosixPath
from io import BytesIO
from pprint import pprint
from collections import OrderedDict, namedtuple
from urllib.parse import quote_plus, unquote, urlparse, parse_qs
from PIL import Image, ExifTags
import piexif
import piexif.helper
from bs4 import BeautifulSoup
import img_like
from rich import print
from rich import print as pprint
from rich.console import Console
console = Console()

import urllib3
assert urllib3.__version__ > "2"
import requests
from requests.adapters import HTTPAdapter, Retry
s = requests.Session()
retries = Retry(backoff_factor=1, status_forcelist=[])
s.mount('http://', HTTPAdapter(max_retries=retries))
s.mount('https://', HTTPAdapter(max_retries=retries))
# s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:125.0) Gecko/20100101 Firefox/125.0"})
import cloudscraper
scraper = cloudscraper.create_scraper()

from config import ffmpeg
from utils import image_verify, get_metadata, url2source


SearchResult = namedtuple('SearchResult', ['link', 'similarity', 'site_and_ids'])

def sim_twitter_id(a: str|None, b: str|None):
    '''
    twitter ID 粗糙比较，条件：
    ① 不为 None
    ② twitter:
    ③ id 相同
    '''
    if a is None or b is None or not a.startswith("twitter:") or not b.startswith("twitter"):
        return False
    if not a.isnumeric():
        a = re.fullmatch(r"twitter:(\d+)@\d", a, re.ASCII)[1]
    if not b.isnumeric():
        b = re.fullmatch(r"twitter:(\d+)@\d", b, re.ASCII)[1]
    return a == b

def ascii2d_eq_twitter(search_result: SearchResult, source: str, threshold=img_like.threshold):
    if sim_twitter_id(search_result.site_and_ids.get('twitter'), source):
        return True
    if search_result.similarity >= threshold:
        for site in ('yandere', 'danbooru', 'gelbooru'):
            if site in search_result.site_and_ids and sim_twitter_id(get_metadata(site, search_result.site_and_ids[site], coarse=True).get('source'), source):
                return True
    return False

saucenao_last_call_time = 0

def process_search_results(
        search_results: list[SearchResult],
        search_identities: list[SearchResult],
        threshold: float,
        metadata: dict,
        display_url: str,
        checkpoint: set
    ) -> bool:  # False 指示抛弃该原图, continue
    pprint(search_results)
    pprint(search_identities)
    assert len(search_identities) + len(search_results) > 0
    max_similarity = max(map(lambda search_result: search_result.similarity, search_results))
    if max_similarity >= threshold:
        print(f"发现相似图片，相似度 {'[red]' if max_similarity >= 0.9 else ''}{max_similarity:.2%}{'[/red]' if max_similarity >= 0.9 else ''}，见 {display_url}")
        while True:
            match console.input('[green]忽略识图并接受原图(I/c)[/green] / [red]抛弃原图并手动收藏识图(R/b)[/red]: '):
                case 'I' | 'c':
                    break  # break 此处局部的 while
                case 'R' | 'b': # 因相似图片抛弃该原图
                    checkpoint.add(metadata['source'])
                    return False
    pre_tags = metadata['tags'].split(', ')
    normal_tags = []
    # 反向优先级解析以覆盖 normal_tags
    for site in ('gelbooru', 'danbooru', 'yandere'):
        if (search_result := next(filter(lambda search_result: site in search_result.site_and_ids, search_identities), None)) is not None:
            normal_tags = get_metadata(site, search_result.site_and_ids[site], coarse=True)['tags']
            pre_tags.append(f'{site}_id:{search_result.site_and_ids[site]}')
    metadata['tags'] = ', '.join(pre_tags + normal_tags)
    return True

def twitter_generator(json_path: Path, exists: Callable[[dict], bool], search: bool = True):
    global saucenao_last_call_time
    break_flag = False  # 预料之中的正常终结
    checkpoint_path = Path('twitter.checkpoint')
    if checkpoint_path.exists():
        with open(checkpoint_path, 'r+b') as f:
            fcntl.lockf(f, fcntl.LOCK_EX)
            checkpoint = pickle.load(f)
    else:
        checkpoint = set()
    
    try:
        for item in json.loads(json_path.read_bytes()):
            if break_flag: break
            source = 'https://twitter.com/i/status/' + item['id']
            print('\n--->', source)
            artist = f"{item['name']}${item['metadata']['core']['user_results']['result']['rest_id']}@{item['screen_name']}"
            for index, media in enumerate(item['media']):
                source_url = media['original']
                print(source_url)
                source_url_parsed = urlparse(source_url)
                if source_url_parsed.hostname == 'video.twimg.com':
                    video_path = Path(source_url_parsed.path)
                    assert video_path.suffix == '.mp4'
                    metadata = {
                        "source": f"twitter:{item['id']}@{index+1}",
                        'local': video_path.name,
                    }
                    if exists(metadata):
                        checkpoint.add(metadata['source'])
                        continue
                    with tempfile.NamedTemporaryFile(suffix='.mp4') as tmp:
                        tmp.write(s.get(source_url).content)
                        tmp.flush()
                        cmd = [
                            ffmpeg,
                            '-i',
                            tmp.name,
                            '-metadata',
                            'title='+item['metadata']['legacy']['extended_entities']['media'][index]['expanded_url'],
                            '-metadata',
                            'artist='+artist,
                            '-metadata',
                            "comment="+item['full_text'],
                            '-codec',
                            'copy',
                        ]
                        pprint(cmd)
                        yield (lambda output: subprocess.run(cmd + [output], check=True)), metadata
                else:
                    format = parse_qs(source_url_parsed.query)['format'][0]
                    assert format in ('jpg', 'png')
                    metadata = {
                        "source": f"twitter:{item['id']}@{index+1}",
                        "from": item['metadata']['legacy']['extended_entities']['media'][index]['expanded_url'],
                        "source_url": source_url,
                        "local": PurePosixPath(unquote(source_url_parsed.path)).name + '.' + format,
                        "title": None,
                        "caption": item['full_text'],
                        "tags": "©:"+artist,
                    }
                    if metadata['source'] in checkpoint:
                        continue
                    if exists(metadata):
                        checkpoint.add(metadata['source'])
                        continue
                    
                    r = s.get(metadata['source_url'], timeout=300)
                    r.raise_for_status()
                    image_verify(r.content)

                    if search:
                        # 识图
                        search_url = metadata['source_url']

                        ## 识图 ascii2d
                        raw_request = s.get(source_url)
                        image_verify(raw_request.content)
                        for _ in range(3):
                            _r = scraper.get(f'https://ascii2d.net/search/url/{search_url}', headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:125.0) Gecko/20100101 Firefox/125.0'})
                            try:
                                _r.raise_for_status()
                            except Exception:
                                print(_r.text)
                                raise
                            if '未対応の形式のファイルです' not in _r.text:
                                break
                            print('未対応の形式のファイルです')
                            time.sleep(100)
                        soup = BeautifulSoup(_r.text, 'html.parser')
                        item_boxes = soup.find_all("div", class_="item-box")
                        assert len(item_boxes) > 0, _r.text
                        assert item_boxes[0].div.img['loading'] == 'eager', item_boxes[0]
                        search_results = []
                        search_identities = []
                        for item_box in item_boxes[1:]:
                            assert item_box.div.img['loading'] == 'lazy', item_box
                            thumb_request = scraper.get("https://ascii2d.net"+item_box.div.img['src'])
                            if thumb_request.status_code == http.HTTPStatus.NOT_FOUND:
                                similarity = 0
                            elif len(thumb_request.content) == 0:
                                similarity = 0
                            else:
                                image_verify(thumb_request.content)
                                similarity = img_like.sim(Image.open(BytesIO(thumb_request.content)), Image.open(BytesIO(raw_request.content)))
                            if item_box.find('div', class_='detail-box').a is None:
                                continue
                            site_and_ids = {}
                            link = item_box.find('div', class_='detail-box').a['href']
                            search_result_source = url2source(link, coarse=True)
                            if search_result_source is not None:
                                site, _, id = search_result_source.partition(':')
                                site_and_ids[site] = id
                            search_result = SearchResult(
                                link=link,
                                similarity=similarity,
                                site_and_ids=site_and_ids
                            )
                            if ascii2d_eq_twitter(search_result, metadata['source']):
                                search_identities.append(search_result)
                            else:
                                search_results.append(search_result)
                        if process_search_results(search_results, search_identities, img_like.threshold, metadata, f'https://ascii2d.net/search/url/{search_url}', checkpoint) is False:
                            continue

                        ## ascii2d: 特徴検索
                        next_ascii2d_url = "https://ascii2d.net" + soup.find("a", string="特徴検索")["href"]
                        _r = scraper.get(next_ascii2d_url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:125.0) Gecko/20100101 Firefox/125.0'})
                        _r.raise_for_status()
                        soup = BeautifulSoup(_r.text, 'html.parser')
                        item_boxes = soup.find_all("div", class_="item-box")
                        assert item_boxes[0].div.img['loading'] == 'eager'
                        search_results = []
                        search_identities = []
                        for item_box in item_boxes[1:]:
                            assert item_box.div.img['loading'] == 'lazy'
                            thumb_request = scraper.get("https://ascii2d.net"+item_box.div.img['src'])
                            if thumb_request.status_code == http.HTTPStatus.NOT_FOUND:
                                similarity = 0
                            else:
                                image_verify(thumb_request.content)
                                similarity = img_like.sim(Image.open(BytesIO(thumb_request.content)), Image.open(BytesIO(raw_request.content)))
                            if item_box.find('div', class_='detail-box').a is None:
                                continue
                            site_and_ids = {}
                            link = item_box.find('div', class_='detail-box').a['href']
                            search_result_source = url2source(link, coarse=True)
                            if search_result_source is not None:
                                site, _, id = search_result_source.partition(':')
                                site_and_ids[site] = id
                            search_result = SearchResult(
                                link=link,
                                similarity=similarity,
                                site_and_ids=site_and_ids
                            )
                            if ascii2d_eq_twitter(search_result, metadata['source']):
                                search_identities.append(search_result)
                            else:
                                search_results.append(search_result)
                        if process_search_results(search_results, search_identities, img_like.threshold, metadata, next_ascii2d_url, checkpoint) is False:
                            continue

                        # 识图 SauceNAO
                        time.sleep(max(0, 10 - (time.time() - saucenao_last_call_time)))
                        saucenao_last_call_time = time.time()
                        _r = s.get(f'https://saucenao.com/search.php?output_type=2&db=999&url={quote_plus(search_url)}&api_key=63b003c17c4a1482f0a9b61a8232241c03fc836f')
                        if _r.status_code != http.HTTPStatus.OK:
                            print(_r.text)
                            break_flag = True
                            break
                        try:
                            results = json.loads(_r.text, object_pairs_hook=OrderedDict)['results']
                        except KeyError:
                            raise NotImplementedError(_r.text)
                        search_results = []
                        search_identities = []
                        for result in results:
                            site_and_ids={}
                            if 'source' in result['data']:
                                if (site_id_str := url2source(result['data']['source'], coarse=True)) is not None:
                                    site, _, id = site_id_str.partition(':')
                                    site_and_ids[site] = id
                            for ext_url in result['data'].get('ext_urls', []):
                                if (site_id_str := url2source(ext_url, coarse=True)) is not None:
                                    site, _, id = site_id_str.partition(':')
                                    site_and_ids[site] = id
                            for site in ('pixiv', 'yandere', 'danbooru', 'gelbooru'):
                                if f"{site}_id" in result['data']:
                                    site_and_ids[site] = result['data'][f"{site}_id"]
                            search_result = SearchResult(
                                link = result['data'].get('source'),
                                similarity=float(result['header']['similarity']) / 100,
                                site_and_ids=site_and_ids
                            )
                            if ascii2d_eq_twitter(search_result, metadata['source'], threshold=0.57):
                                search_identities.append(search_result)
                            else:
                                search_results.append(search_result)
                        if process_search_results(search_results, search_identities, 0.57, metadata, f"https://saucenao.com/search.php?url={quote_plus(search_url)}", checkpoint) is False:
                            continue

                    # 元数据写入 jpg 图片
                    if format == 'jpg':
                        img_data_io = io.BytesIO()
                        exif = Image.open(io.BytesIO(r.content)).getexif()
                        assert len(exif) == 0
                        exif_bytes = piexif.dump({
                            "0th": {
                                piexif.ImageIFD.Copyright: metadata['from'],
                                piexif.ImageIFD.Artist: artist.encode('UTF-8')
                            },
                            "Exif": {
                                piexif.ExifIFD.UserComment: piexif.helper.UserComment.dump(metadata['caption'], encoding='unicode')
                            },
                        })
                        piexif.insert(exif_bytes, r.content, new_file=img_data_io)
                        img_data = img_data_io.getvalue()
                    else:
                        img_data = r.content
                    pprint(metadata)
                    yield img_data, metadata
                
                checkpoint.add(metadata['source'])

        if not break_flag:
            Path('./twitter_metadata_archive/').mkdir(exist_ok=True)
            with lzma.open(Path('./twitter_metadata_archive/') / (json_path.name+'.xz'), "w") as f:
                f.write(json_path.read_bytes())
            json_path.unlink()
        break_flag = True # 正常结束，跳过下面的保存询问
    finally:
        if break_flag or input("保存 checkpoint? [y/N] ").lower() == 'y':
            with open(checkpoint_path, 'wb') as f:
                pickle.dump(checkpoint, f, protocol=5)
                fcntl.lockf(f, fcntl.LOCK_UN)
            print("😀 已保存")
