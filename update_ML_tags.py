import sqlite3, time
from rich import print

from tinybooru_image import TinyBooruImage
from config import sqlite3_path

db = sqlite3.connect(sqlite3_path)

for index, (local_, custom_tags, pixiv_tags, booru_tags) in enumerate(db.execute('SELECT local, custom_tags, pixiv_tags, booru_tags FROM pixiv WHERE ML_tags IS NULL AND booru_tags == ""')):
    tinybooru_image = TinyBooruImage({
        'local': local_,
        'custom_tags': custom_tags.split(', ') if custom_tags != '' else [],
        'pixiv_tags': pixiv_tags.split(', ') if pixiv_tags != '' else [],
        'booru_tags': booru_tags.split(', ') if booru_tags != '' else [],
    })
    print(index, tinybooru_image.path)

    print(dict(tinybooru_image.metadata))

    with tinybooru_image.thumb() as (thumb_buffer, filename):
        tinybooru_image.process_tags(thumb_buffer)

    print(dict(tinybooru_image.metadata))

    db.execute(
        'UPDATE pixiv SET ML_tags=:ML_tags, romanized_tags=:romanized_tags, translated_tags=:translated_tags WHERE local=:local',
        tinybooru_image.metadata,
    )

    if index % 100 == 0:
        db.commit()
        print('Committed!')
    
    # time.sleep(2)
