from typing import Callable
import asyncio, json, sys
import urllib.parse
from pathlib import Path, PurePosixPath
from rich import print as pprint
import urllib3
assert urllib3.__version__ > "2"
import requests
from requests.adapters import HTTPAdapter, Retry
s = requests.Session()
retries = Retry(backoff_factor=1, status_forcelist=[])
s.mount('http://', HTTPAdapter(max_retries=retries))
s.mount('https://', HTTPAdapter(max_retries=retries))
s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:130.0) Gecko/20100101 Firefox/130.0"})

sys.path.insert(0, './XHS-Downloader')

from source import XHS


async def async_xhs_generator(multiple_links):
    async with XHS(
                   user_agent="Mozilla/5.0 (Windows NT 10.0; rv:130.0) Gecko/20100101 Firefox/130.0",
                   cookie="",
                   record_data=False,
                   image_format="PNG",
                   folder_mode=True,
                   ) as xhs:  # 使用自定义参数
        return await xhs.extract(multiple_links, download=False)

def xhs_generator(link_path: str, exists: Callable[[dict], bool]):
    if link_path == '-':
        multiple_links = input('贴入以空格分隔的链接\n')
    else:
        multiple_links = Path(link_path).read_text()
    ret = asyncio.run(async_xhs_generator(multiple_links))
    if ret[-1] == {}: ret = ret[:-2] # incompleted link at the end
    for item in ret:
        try:
            artist = f"{item['作者昵称']}${item['作者ID']}"
            metadata = {
                'source': f"xhs:{item['作品ID']}",
                'from': item['作品链接'],
                'title': item['作品标题'],
                'caption': item['作品描述'],
                'source_url': item['作品链接'],
                'tag': ', '.join(["©:"+artist]+item['作品标签'].split(' ')),
                'raw_detail': item,
            }
            pprint(metadata)
            yield json.dumps(metadata, ensure_ascii=False, indent='\t').encode('UTF-8'), metadata | {'local': 'XHS/' + item['作品ID'] + '/metadata.json'}
            for download_url in item['下载地址']:
                download_url_parsed = urllib.parse.urlparse(download_url)
                if download_url_parsed.hostname == 'sns-video-bd.xhscdn.com':
                    suffix = '.mov'
                elif download_url_parsed.hostname == 'ci.xiaohongshu.com' and 'png' in download_url_parsed.query.lower():
                    suffix = '.png'
                else:
                    raise NotImplementedError(download_url)
                local_path = 'XHS/' + item['作品ID'] + '/' + PurePosixPath(download_url_parsed.path).name + suffix
                metadata_per_media = metadata | {'local': local_path}
                if exists(metadata_per_media):
                    pprint('Skip', download_url)
                    continue
                r = s.get(download_url)
                r.raise_for_status()
                yield r.content, metadata_per_media
        except KeyError:
            print(item)
            raise
    if link_path != '-':
        link_path.unlink()
