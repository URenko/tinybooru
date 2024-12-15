import sqlite3
from pathlib import Path
import numpy as np
import faiss

db = sqlite3.connect("file:pixiv.db?mode=ro", check_same_thread=False, uri=True)


d = 256

quantizer = faiss.IndexBinaryFlat(d)
index_ORB = faiss.IndexBinaryIVF(quantizer, d, 65536)
save_filename = 'ORB_faiss_IVF_index'

index_ORB.verbose = True
index_ORB.cp.verbose = True

clustering_index = faiss.index_cpu_to_all_gpus(faiss.IndexFlatL2(d))
index_ORB.clustering_index = clustering_index

vectors = np.concatenate(
    [
        np.frombuffer(ORB_hash, dtype=np.uint8).reshape((-1, 32))
        for rowid, ORB_hash in db.execute(f'SELECT rowid, ORB_hash FROM pixiv')
    ],
    axis=0
)
print(vectors.shape)

index_ORB.train(vectors)


faiss.write_index_binary(index_ORB, save_filename)

Path('ORB_faiss_index').symlink_to(save_filename)
