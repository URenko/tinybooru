from rich import print
from imagehash import average_hash, phash, dhash, whash
import timm 
import torch
import torch.nn.functional as F

from utils import ImageHash2int


m = timm.create_model('vit_large_patch14_clip_224.openai', pretrained=True, num_classes=0)
trans = timm.data.create_transform(**timm.data.resolve_model_data_config(m), is_training=False)

@torch.inference_mode()
def ML_sim(a, b, comment=''):
    stack = torch.stack([trans(a.convert("RGB")), trans(b.convert("RGB"))], 0)
    fa = m(stack)
    fa = F.normalize(fa, p=2, dim=1) 
    similarity = fa[0].dot(fa[1]).item()
    if similarity > 0.7117 and similarity <= 0.7136:
        input(f"{comment}\n{similarity=}, 可疑")
    return similarity > 0.7136
 
# threshold = 0.7
# 0.8985 0.8639 0.8199 0.7863
# 0.4829 0.5743 0.6386 0.7117

# identical 阈值
# 待考察: 0.9


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
    print(a_sim, p_sim, d_sim, w_sim, ML)
    if a_sim + p_sim + d_sim + w_sim + ML >= 1:
        return True
    else:
        return False

if __name__ == '__main__':
    import sys
    from PIL import Image
    print(sim(Image.open(sys.argv[1]), Image.open(sys.argv[2])))