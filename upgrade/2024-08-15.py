import sqlite3, re, functools, urllib.parse
from pathlib import Path
from rich import print
import requests
from requests.adapters import HTTPAdapter, Retry
s = requests.Session()
s.mount('http://', HTTPAdapter(max_retries=Retry(backoff_factor=1)))
s.mount('https://', HTTPAdapter(max_retries=Retry(backoff_factor=1)))
from PIL import Image
from jxlpy import JXLImagePlugin
from imagehash import average_hash, phash, dhash, whash

from utils import url2source, process_tags, booru_tag_detail, ImageHash2int
from config import sqlite3_path, local_root

from rich.traceback import install
install(show_locals=True)

db = sqlite3.connect(sqlite3_path)

columns = set(info[1] for info in db.execute("PRAGMA table_info('pixiv');").fetchall())

print(columns)

assert columns.isdisjoint({'pixiv', 'twitter', 'yandere', 'danbooru', 'gelbooru', 'zerochan'})

db.execute('ALTER TABLE pixiv RENAME TO pixiv_tmp')

db.execute(Path('bootstrap.sql').read_text())

skip = True
for rowid, source, from_, source_url, local_, title, caption, tags, thumbnail in db.execute('SELECT rowid, source, `from`, source_url, local, title, caption, tags, thumbnail from pixiv_tmp'):
    # if skip:
    #     if source != 'pixiv:75034246':
    #         continue
    #     else:
    #         skip = False
    img = Image.open(local_root / 'pixiv' / local_)
    insert = {
        'pixiv': None,
        'twitter': None,
        'yandere': None,
        'danbooru': None,
        'gelbooru': None,
        'zerochan': None,
        'unique_source': None,
        'nonunique_source': None,
        'from': from_,
        'source_url': source_url,
        'local': local_,
        'title': title,
        'caption': caption,
        'custom_tags': [],
        'pixiv_tags': [],
        'booru_tags': [],
        'romanized_tags': [],
        'translated_tags': [],
        'ML_tags': None,
        'thumbnail': thumbnail,
        'aHash': ImageHash2int(average_hash(img)),
        'pHash': ImageHash2int(phash(img)),
        'dHash': ImageHash2int(dhash(img)),
        'wHash': ImageHash2int(whash(img)),
    }
    print(rowid, source)
    if source.isdigit():
        insert['pixiv'] = source
        print(f'[red]SPECIAL CASE:[/red] {source} convert to pixiv:{source}')
    else:
        source_site, _, source_id = source.partition(':')
        if source_site in ('http', 'https'):
            insert['nonunique_source'] = source
        else:
            insert[source_site] = source_id

    if tags == '':
        tags = []
    else:
        tags : list[str] = tags.split(', ')
    for tag in filter(lambda tag: '_id:' in tag, tags.copy()):
        source_site, _, source_id = tag.partition('_id:')
        insert[source_site] = source_id
        # print(tag)
        tags.remove(tag)
    
    for tag in tags:
        if tag.startswith('🔞:') or tag.startswith('©:'):
            insert['custom_tags'].append(tag)
        elif booru_tag_detail(tag):
            insert['booru_tags'].append(tag)
        else:
            insert['pixiv_tags'].append(tag)
    
    process_tags(insert)
    
    if from_ is not None:
        source_basedon_from = url2source(from_, coarse=True)
        if source_basedon_from.site.endswith('_source'):
            print(source_basedon_from)
        if insert[source_basedon_from.site] is None:
            insert[source_basedon_from.site] = source_basedon_from.id
    
    if insert['twitter'] is not None:
        insert['twitter'] = insert['twitter'].replace('@', '#')

    print(insert)
    

    try:
        db.execute(f"INSERT INTO pixiv VALUES({', '.join(':'+k for k in insert.keys())})", insert)
    except:
        print(insert)
        db.commit()
        db.close()
        raise

db.commit()
db.close()

import pickle
checkpoint_path = Path('twitter.checkpoint')
if checkpoint_path.exists():
    with open(checkpoint_path, 'rb') as f:
        checkpoint = pickle.load(f)

checkpoint = set(c.removeprefix('twitter:').replace('@', '#') for c in checkpoint)

with open(checkpoint_path, 'wb') as f:
    pickle.dump(checkpoint, f, protocol=5)
