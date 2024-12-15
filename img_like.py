from rich import print
from imagehash import average_hash, phash, dhash, whash, ImageHash
import timm 
import torch
import torch.nn.functional as F
import numpy as np
import scipy.spatial.distance
import cv2
from python_orb_slam3 import ORBExtractor

def ImageHash2int(imagehash: ImageHash, signed=True):
    ret = sum([2**i for i, v in enumerate(imagehash.hash.flatten()) if v])
    if signed:
        return int.from_bytes(ret.to_bytes(length=8, byteorder='big', signed=False), byteorder='big', signed=True)
    else:
        return ret

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
m = timm.create_model('vit_large_patch14_clip_224.openai', pretrained=True, num_classes=0).to(device)
trans = timm.data.create_transform(**timm.data.resolve_model_data_config(m), is_training=False)

@torch.inference_mode()
def CLIP_hash(a):
    fa = m(trans(a.convert("RGB"))[None,:].to(device))
    fa = F.normalize(fa, p=2, dim=1)
    return fa.cpu().numpy()

@torch.inference_mode()
def ML_sim(a, b, comment=''):
    stack = torch.stack([trans(a.convert("RGB")), trans(b.convert("RGB"))], 0)
    fa = m(stack)
    fa = F.normalize(fa, p=2, dim=1) 
    similarity = fa[0].dot(fa[1]).item()
    if similarity > 0.7117 and similarity <= 0.7123:
        input(f"{comment}\n{similarity=}, 可疑")
    return similarity > 0.7123
 
# threshold = 0.7
# 0.8985 0.8639 0.8199 0.7863
# 0.4829 0.5743 0.6386 0.7117

# identical 阈值
# 待考察: 0.9

orb_extractor = ORBExtractor(
    n_features=500,
    scale_factor=1.2,
    n_levels=8,
    interpolation=cv2.INTER_AREA,
)
def ORB_hash(im):
    cv_im = np.asarray(im.convert("L"))
    _, descriptors = orb_extractor.detectAndCompute(cv_im)
    return descriptors if descriptors is not None else np.empty((0), dtype=np.uint8)

def wilson_score(scores: np.ndarray|list[int]) -> float:
    if not scores: return 0
    scores = 1 - np.asarray(scores) / 256
    mean = np.mean(scores)
    var = np.var(scores)
    total = len(scores)
    p_z = 2.
    score = (
        mean
        + (np.square(p_z) / (2.0 * total))
        - ((p_z / (2.0 * total)) * np.sqrt(4.0 * total * var + np.square(p_z)))
    ) / (1 + np.square(p_z) / total)
    return score

def ORB_sim(a, b):
    descriptors_b = ORB_hash(b)
    scores = tuple(
        256 * min(
            scipy.spatial.distance.hamming(np.unpackbits(descriptor_a), np.unpackbits(descriptor_b))
            for descriptor_b in descriptors_b
        )
        for descriptor_a in ORB_hash(a)
    )
    score = wilson_score(scores)
    return score > 0.74
    

def hamming_distance(a: int, b: int, signed: bool):
    if signed:
        a = int.from_bytes(a.to_bytes(length=8, byteorder='big', signed=True), byteorder='big', signed=False)
        b = int.from_bytes(b.to_bytes(length=8, byteorder='big', signed=True), byteorder='big', signed=False)
    return (a ^ b).bit_count()

def sim(a, b, comment=''):
    a_sim = hamming_distance(*(ImageHash2int(average_hash(x), signed=False) for x in (a, b)), signed=False) < 11
    p_sim = hamming_distance(*(ImageHash2int(phash(x), signed=False) for x in (a, b)), signed=False) < 18
    d_sim = hamming_distance(*(ImageHash2int(dhash(x), signed=False) for x in (a, b)), signed=False) < 18
    w_sim = hamming_distance(*(ImageHash2int(whash(x), signed=False) for x in (a, b)), signed=False) < 10
    ML = ML_sim(a, b, comment=comment)
    ORB = ORB_sim(a, b)
    print(a_sim, p_sim, d_sim, w_sim, ML, ORB)
    if a_sim + p_sim + d_sim + w_sim + ML + ORB >= 1:
        return True
    else:
        return False

if __name__ == '__main__':
    import sys
    from PIL import Image
    print(sim(Image.open(sys.argv[1]), Image.open(sys.argv[2])))