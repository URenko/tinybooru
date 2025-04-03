import functools, io, mimetypes, tempfile, subprocess, shutil, json
mimetypes.add_type('image/jxl', '.jxl')
from collections import OrderedDict
from pathlib import PurePosixPath, Path, PurePath

from rich import print
from PIL import Image
import requests
from imagehash import average_hash, phash, dhash, whash
from requests.adapters import HTTPAdapter, Retry
s = requests.Session()
s.mount('http://', HTTPAdapter(max_retries=Retry(backoff_factor=1)))
s.mount('https://', HTTPAdapter(max_retries=Retry(backoff_factor=1)))
from jxl import jxl
from img_like import ImageHash2int, CLIP_hash, ORB_hash
from utils import process_tags, get_thumb_from_video, long_rating_tag_to_short
from config import local_root
local_root = local_root / 'pixiv'

class TinyBooruImage:
    def __init__(self, metadata: dict):
        self.metadata = OrderedDict({
            'pixiv': None,
            'twitter': None,
            'yandere': None,
            'danbooru': None,
            'gelbooru': None,
            'zerochan': None,
            'unique_source': None,
            'nonunique_source': None,
            'from': None,
            'source_url': None,
            'local': None,
            'title': None,
            'caption': None,
            'custom_tags': [],
            'pixiv_tags': [],
            'booru_tags': [],
            'romanized_tags': [],
            'translated_tags': [],
            'ML_tags': None,
            'thumbnail': None,
            'aHash': None,
            'pHash': None,
            'dHash': None,
            'wHash': None,
            'CLIP_hash': None,
            'ORB_hash': None
        }) | metadata
    
    def fetch_media(self, media: bytes | Path):
        self.media = media
        if self.mime.startswith('video/'):
            assert isinstance(media, PurePath)
            self.image_data = get_thumb_from_video(media)
            self.image_name = PurePath(self.metadata['local']).with_suffix('.png').name
        elif isinstance(media, bytes):
            self.image_data = media
            self.image_name = PurePath(self.metadata['local']).name
        elif isinstance(media, Path): # the `media` path is unstable, read it ASAP
            self.image_data = media.read_bytes()
            self.image_name = media.name
        
        fp, _, _ = self.image_fp()
        try:
            with Image.open(self.image_fp()[0]) as im:
                im.verify()
        finally:
            fp.close()
    
    def save_file(self):
        fpath = local_root / self.metadata['local']
        fpath.parent.mkdir(exist_ok=True)
        
        assert not fpath.exists()

        if isinstance(self.media, bytes):
            fpath.write_bytes(self.media)
        elif isinstance(self.media, PurePath):
            shutil.move(self.media, fpath)

        self.metadata['local'] = PurePath(self.metadata['local']).with_name(jxl(fpath).name).as_posix()
    
    @property
    def path(self) -> Path:
        return local_root / self.metadata['local']
    
    @functools.cached_property
    def mime(self):
        return mimetypes.guess_type(self.metadata['local'], strict=False)[0]
    
    def image_fp(self, normal=False):
        '''return (fp, fname, size)'''
        if hasattr(self, 'image_data'):
            return (
                io.BytesIO(self.image_data),
                self.image_name,
                len(self.image_data)
            )
        
        if self.mime.startswith('video/'):
            self.image_data = get_thumb_from_video(self.path)
            self.image_name = PurePath(self.metadata['local']).with_suffix('.png').name
            return self.image_fp(normal=normal)
        
        if normal and self.path.suffix.lower() == '.jxl':
            _jpg_tmp_file = tempfile.NamedTemporaryFile(suffix='.jpg')
            subprocess.run(["djxl", str(self.path), _jpg_tmp_file.name], check=True)
            return (
                _jpg_tmp_file,
                PurePosixPath(self.metadata['local']).with_suffix('.jpg').name,
                Path(_jpg_tmp_file.name).stat().st_size
            )
        
        return (
            open(self.path, 'rb'),
            PurePosixPath(self.metadata['local']).name,
            self.path.stat().st_size
        )
    
    def calc_hash(self):
        fp, _, _ = self.image_fp()
        try:
            with Image.open(fp) as img:
                self.metadata |= {
                'aHash': ImageHash2int(average_hash(img)),
                'pHash': ImageHash2int(phash(img)),
                'dHash': ImageHash2int(dhash(img)),
                'wHash': ImageHash2int(whash(img)),
                'CLIP_hash': CLIP_hash(img).tobytes(),
                'ORB_hash': ORB_hash(img).tobytes()
            }
        finally:
            fp.close()

    def thumb(self, target_size=5*2**20, format='JPEG'):
        class Thumb:
            def __init__(self, tinybooru_image: TinyBooruImage):
                self.tinybooru_image = tinybooru_image
            
            def __enter__(self):
                origin_image, origin_name, origin_size = self.tinybooru_image.image_fp(normal=True)
                self.origin_image = origin_image
                with Image.open(origin_image) as im:
                    if origin_size < target_size and getattr(im, 'n_frames', 1) <= 1:
                        thumb_buffer = origin_image
                        filename = origin_name
                    else:
                        reduce_factor = 0
                        thumb_size = 2**99 # force start
                        if im.n_frames > 1:
                            im.seek(im.n_frames // 2)
                        if im.mode == 'RGBA':  # cannot write mode RGBA as JPEG
                            im = im.convert('RGB')
                        while thumb_size >= target_size:
                            reduce_factor += 1
                            im_tmp = im.reduce(reduce_factor) if reduce_factor != 1 else im
                            thumb_buffer = io.BytesIO()
                            im_tmp.save(thumb_buffer, format, **({'WEBP': {'method': 6}, 'JPEG': {'optimize': True}}[format]))
                            thumb_size = len(thumb_buffer.getvalue())
                            print(f"{origin_size/2**10:.1f} KB reduced {reduce_factor} times to {thumb_size/2**10:.1f} KB")
                        filename = str(Path(origin_name).with_suffix({'WEBP': '.webp', 'JPEG': '.jpg'}[format]))
                thumb_buffer.seek(0)
                return (thumb_buffer, filename)
            
            def __exit__(self, exc_type, exc_value, traceback):
                self.origin_image.close()
        
        return Thumb(self)
    
    def process_tags(self, thumb_buffer, skip_ML_if_booru_exists=True):
        '''self.thumb is not used inside. Users should manage Thumb manually.'''
        if not skip_ML_if_booru_exists or not self.metadata['booru_tags']:
            ## autotagger
            # r = s.post(
            #     'https://autotagger.donmai.us/evaluate',
            #     files={
            #         'file': thumb_buffer,
            #     },
            #     data={'format': 'json'},
            # )
            # r.raise_for_status()
            # rating = next(t for t in r.json()[0]['tags'] if t.startswith('rating:'))
            # self.metadata['custom_tags'].append(long_rating_tag_to_short(rating))
            # self.metadata['ML_tags'] = [t for t,p in r.json()[0]['tags'].items() if p > 0.5 and not t.startswith('rating:')]
            
            # DeepDanbooru
            r = s.post(
                "https://hysts-deepdanbooru.hf.space/gradio_api/upload",
                files={
                    'files': thumb_buffer,
                }
            )
            j = r.json()
            r.raise_for_status()
            r = s.post(
                "https://hysts-deepdanbooru.hf.space/gradio_api/call/predict",
                json={
                    "data": [
                        {"path": j[0]},
                        0
                    ]
                },
                headers={"Content-Type": "application/json"},
            )
            r.raise_for_status()
            r = s.get(
                f"https://hysts-deepdanbooru.hf.space/gradio_api/call/predict/{r.json()['event_id']}",
                headers={"Content-Type": "application/json"},
            )
            r.raise_for_status()
            j = json.loads(r.text.removeprefix('event: complete\ndata: '))
            rating = next(t for t in j[1] if t.startswith('rating:'))
            self.metadata['custom_tags'].append(long_rating_tag_to_short(rating))
            print(f"custom_tags: {self.metadata['custom_tags']}")
            self.metadata['ML_tags'] = [item['label'] for item in j[0]['confidences'] if item['confidence'] > 0.5 and not item['label'].startswith('rating:')]
            print(f"ML_tags: {self.metadata['ML_tags']}")
        process_tags(self.metadata)
