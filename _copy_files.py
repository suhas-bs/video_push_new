"""Copies remaining files from outputs into this folder — delete after use."""
import shutil, os

src = "/sessions/modest-optimistic-turing/mnt/outputs/"
dst = "/sessions/modest-optimistic-turing/mnt/Video_API/"

for fname in ["meta_api.py", "requirements.txt", ".gitignore", "meta_video_push_tool.html"]:
    shutil.copy2(src + fname, dst + fname)
    print(f"  copied  {fname}")
