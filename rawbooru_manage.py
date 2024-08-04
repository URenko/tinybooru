#!/usr/bin/env python3
import sys
from pathlib import Path
p = Path(sys.argv[1])

from jxl import jxl_exists

from config import local_root

local_root = local_root / input("保存到子目录: ")
assert local_root.exists() and local_root.is_dir()

from providers.twitter import twitter_generator

def exists(metadata: dict) -> bool:
    fpath = Path(local_root / metadata['local'])
    return jxl_exists(fpath) or jxl_exists(local_root / fpath.name)

for image_data, metadata in twitter_generator(p, exists, search=False):
    fpath = Path(local_root / metadata['local'])
    fpath.parent.mkdir(exist_ok=True)
    if isinstance(image_data, bytes):
        fpath.write_bytes(image_data)
    elif callable(image_data):
        image_data(str(fpath))
    else:
        raise NotImplementedError(image_data)
