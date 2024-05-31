from pathlib import Path
from pixivpy3 import AppPixivAPI

Papi = AppPixivAPI()
Papi.auth(refresh_token=Path('./refresh_token.txt').read_text())
Path('./refresh_token.txt').write_text(Papi.refresh_token)
