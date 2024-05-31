import timm 
import torch
import torch.nn.functional as F
m = timm.create_model('vit_large_patch14_clip_224.openai', pretrained=True, num_classes=0)
trans = timm.data.create_transform(**timm.data.resolve_model_data_config(m), is_training=False)
 
@torch.inference_mode()
def sim(a, b):
    stack = torch.stack([trans(a.convert("RGB")), trans(b.convert("RGB"))], 0)
    fa = m(stack)
    fa = F.normalize(fa, p=2, dim=1) 
    return fa[0].dot(fa[1]).item()
 
threshold = 0.7
# 0.8985 0.8639 0.8199 0.7863
# 0.4829 0.5743 0.6386 0.7117

# identical 阈值
# 待考察: 0.9

if __name__ == '__main__':
    import sys
    from PIL import Image
    print(sim(Image.open(sys.argv[1]), Image.open(sys.argv[2])))