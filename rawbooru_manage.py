#!/usr/bin/env python3
import argparse
from pathlib import Path


from jxl import jxl_exists

from config import local_root

assert __name__ == '__main__'

parser = argparse.ArgumentParser()
parser_group = parser.add_mutually_exclusive_group()
parser_group.add_argument("-t", "--twitter", type=str, help="从 twiiter 导出的 json 文件导入")
parser_group.add_argument("-x", "--xhs", nargs='?', const='-', type=str, help="从以空格分隔的小红书导出的链接导入")
args = parser.parse_args()

local_root = local_root / input("保存到子目录: ")
assert local_root.exists() and local_root.is_dir()


def exists(metadata: dict) -> bool:
    fpath = Path(local_root / metadata['local'])
    return jxl_exists(fpath) or jxl_exists(local_root / fpath.name)


if args.twitter is not None:
    from providers.twitter import twitter_generator
    for image_data, metadata in twitter_generator(Path(args.twitter), exists, search=False):
        fpath = Path(local_root / metadata['local'])
        fpath.parent.mkdir(exist_ok=True)
        if isinstance(image_data, bytes):
            fpath.write_bytes(image_data)
        elif callable(image_data):
            image_data(str(fpath))
        else:
            raise NotImplementedError(image_data)
elif args.xhs:
    from providers.xhs import xhs_generator
    for image_data, metadata in xhs_generator(args.xhs, exists):
        fpath = Path(local_root / metadata['local'])
        fpath.parent.mkdir(exist_ok=True)
        assert isinstance(image_data, bytes)
        fpath.write_bytes(image_data)
