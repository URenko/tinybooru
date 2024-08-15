- 配置: `config.py` 暴露 `local_root: Path`, `sqlite3_path: str`, `ffmpeg: str`
- 添加媒体: `tinybooru_manage.py`, `rawbooru_manage.py`
- 启动服务器: `fastapi run tinybooru.py`

## 数据库字段说明
```
pixiv - zerochan: 源站 ID
unique_source, nonunique_source: 并非来自以上站点时填写的源站
from: 实际从该 URL 网页获取的图片，pixiv 可不填写
source_url: 实际是从该 URL 下载的图片
local: 本地文件名
title: （短）标题
caption: 作者的说明
tags: 以', '分隔
thumbnail: 缩略图 URL
```

## systemd

如果未放置在 `/home/$USER/tinybooru` ，对 tinybooru@.service 和下面作相应修改。

If it is not placed in `/home/$USER/tinybooru`, modify tinybooru@.service and following accordingly.

``` bash
sudo ln -s ~/tinybooru/tinybooru@.service /etc/systemd/system/tinybooru@.service
sudo systemctl daemon-reload
sudo systemctl start tinybooru@$USER
```

