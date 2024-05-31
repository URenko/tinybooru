import time, subprocess
from pathlib import Path
from random import randbytes
from rich import print

local_root = Path('/media/renko/DATA1/Pictures')
remote_root = 'm_Pictures:'
tmp = local_root / 'tmp'

if tmp.exists():
    print('存在 tmp, 删除.')
    tmp.unlink()
    time.sleep(5)

next(local_root.iterdir())

content = randbytes(1024)

print('写入测试...', end='')
tmp.write_bytes(content)
assert tmp.exists()

# time.sleep(1)

assert (_o := subprocess.run(
    ['rclone', 'cat', remote_root+'tmp'],
    stdout=subprocess.PIPE,
    check=True
).stdout) == content, _o
print('[green]OK[/green]')


print('删除测试...', end='')
tmp.unlink()
assert not tmp.exists()

assert 'tmp' not in subprocess.run(
    ['rclone', 'lsf', remote_root],
    capture_output=True,
    check=True,
    text=True
).stdout.splitlines()
print('[green]OK[/green]')
