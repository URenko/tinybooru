import functools, io, mimetypes, tempfile, subprocess
mimetypes.add_type('image/jxl', '.jxl')
from pathlib import PurePosixPath, Path

from rich import print
from PIL import Image

from config import local_root
local_root = local_root / 'pixiv'

class TinyBooruImage:
    def __init__(self, metadata: dict):
        self.metadata = metadata
        self.image_data = None
        self.image_suffix = '.png'
    
    @property
    def path(self) -> Path:
        return local_root / self.metadata['local']
    
    @functools.cached_property
    def mime(self):
        return mimetypes.guess_type(self.metadata['local'], strict=False)[0]
    
    def normal_image_fp(self):
        '''return (fp, fname, size)'''
        if self.image_data is not None:
            return (
                io.BytesIO(self.image_data),
                PurePosixPath(self.metadata['local']).with_suffix(self.image_suffix).name,
                len(self.image_data)
            )
        
        if not self.mime.startswith('image/'):
            raise NotImplementedError
        
        if self.path.suffix.lower() == '.jxl':
            _jpg_tmp_file = tempfile.NamedTemporaryFile(suffix='.jpg')
            subprocess.run(["djxl", str(self.path), _jpg_tmp_file.name], check=True)
            return (
                _jpg_tmp_file,
                PurePosixPath(self.metadata['local']).with_suffix(self.image_suffix).name,
                Path(_jpg_tmp_file.name).stat().st_size
            )
        
        return (
            open(self.path, 'rb'),
            PurePosixPath(self.metadata['local']).with_suffix(self.image_suffix).name,
            self.path.stat().st_size
        )
    
    @functools.lru_cache
    def thumb(self, target_size=5*2**20):
        class Thumb:
            def __init__(self, tinybooru_image: TinyBooruImage):
                self.tinybooru_image = tinybooru_image
            
            def __enter__(self):
                origin_image, origin_name, origin_size = self.tinybooru_image.normal_image_fp()
                self.origin_image = origin_image
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
                return (thumb_buffer, filename)
            
            def __exit__(self, exc_type, exc_value, traceback):
                self.origin_image.close()
        
        return Thumb(self)
    