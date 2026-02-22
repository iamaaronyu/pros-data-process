"""构建用于端到端测试的小型图片数据集。"""
from __future__ import annotations

import os
from pathlib import Path


# 最小合法 1x1 像素 PNG（约 68 字节）
MINI_PNG_BYTES = bytes([
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
    0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
    0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53,
    0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41,
    0x54, 0x08, 0xD7, 0x63, 0xF8, 0xFF, 0xFF, 0x3F,
    0x00, 0x05, 0xFE, 0x02, 0xFE, 0xDC, 0xCC, 0x59,
    0xE7, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E,
    0x44, 0xAE, 0x42, 0x60, 0x82,
])


def build_image_dataset(out_dir: str | Path, num_images: int = 3, ext: str = "png") -> list[str]:
    """
    在 out_dir 下生成 num_images 个最小合法图片文件，返回路径列表。
    用于端到端测试，无需真实图片内容。
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for i in range(num_images):
        path = out_dir / f"image_{i:02d}.{ext}"
        path.write_bytes(MINI_PNG_BYTES)
        paths.append(str(path))
    return paths


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="构建 E2E 测试用图片数据集")
    p.add_argument("--out", default="tests/fixtures/images", help="输出目录")
    p.add_argument("-n", "--num", type=int, default=3, help="图片数量")
    args = p.parse_args()
    paths = build_image_dataset(args.out, args.num)
    print(f"Created {len(paths)} images under {args.out}")
    for p in paths:
        print(f"  {p}")
