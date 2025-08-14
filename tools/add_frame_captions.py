import argparse
import json
import os
import re
from typing import Dict, List, Tuple


FRAME_BLOCK_REGEX = re.compile(r"<Frame\b[^>]*>.*?</Frame>", re.DOTALL)
FRAME_OPEN_TAG_REGEX = re.compile(r"<Frame\b[^>]*>", re.DOTALL)
IMG_SRC_REGEX = re.compile(r"<img\b[^>]*\bsrc\s*=\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)


def load_media_captions(media_jsonl_path: str) -> Dict[str, str]:
    captions: Dict[str, str] = {}
    with open(media_jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            media_id = obj.get("media_id")
            caption = obj.get("caption")
            if isinstance(media_id, str) and isinstance(caption, str) and media_id:
                captions[media_id] = caption
    return captions


def is_images_path(src: str) -> bool:
    # Consider any path that contains '/images/' or starts with '/images' or 'images/'
    normalized = src.replace("\\", "/")
    return (
        "/images/" in normalized
        or normalized.startswith("/images")
        or normalized.startswith("images/")
        or "/images%2F" in normalized  # URL-encoded folder edge case
    )


def escape_caption_for_jsx(value: str) -> str:
    # Replace double quotes and newlines for safe JSX attribute
    value = value.replace("\r", " ").replace("\n", " ")
    value = value.replace('"', "&quot;")
    return value.strip()


def add_caption_to_frame_open_tag(open_tag: str, caption: str) -> str:
    # If caption already present, return unchanged
    if re.search(r"\bcaption\s*=", open_tag):
        return open_tag
    safe_caption = escape_caption_for_jsx(caption)
    # Insert before closing '>' while preserving spacing/formatting
    if open_tag.endswith(">"):
        return open_tag[:-1] + f' caption="{safe_caption}">'
    return open_tag


def process_frame_block(block: str, media_captions: Dict[str, str]) -> Tuple[str, bool]:
    """
    Return (new_block, changed)
    """
    # Find first image src in the block that points to images folder
    matches = list(IMG_SRC_REGEX.finditer(block))
    if not matches:
        return block, False

    matched_caption: str = ""
    matched_idx: int = -1
    for i, m in enumerate(matches):
        src = m.group(1)
        if not is_images_path(src):
            continue
        media_id = os.path.basename(src.split("?")[0])  # drop querystring if present
        if media_id in media_captions and media_captions[media_id]:
            matched_caption = media_captions[media_id]
            matched_idx = i
            break

    if matched_idx == -1:
        return block, False

    # Extract open tag
    open_tag_match = FRAME_OPEN_TAG_REGEX.search(block)
    if not open_tag_match:
        return block, False
    open_tag = open_tag_match.group(0)
    new_open_tag = add_caption_to_frame_open_tag(open_tag, matched_caption)
    if new_open_tag == open_tag:
        return block, False
    # Replace only this open tag occurrence
    start, end = open_tag_match.span()
    new_block = block[:start] + new_open_tag + block[end:]
    return new_block, True


def process_file(path: str, media_captions: Dict[str, str]) -> Tuple[bool, int]:
    with open(path, "r", encoding="utf-8") as f:
        original = f.read()

    changed = False
    replacements = 0

    # Iterate over frame blocks
    def _replace(match: re.Match) -> str:
        nonlocal changed, replacements
        block = match.group(0)
        new_block, did_change = process_frame_block(block, media_captions)
        if did_change:
            changed = True
            replacements += 1
        return new_block

    new_content = FRAME_BLOCK_REGEX.sub(_replace, original)

    if changed and new_content != original:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(new_content)
    return changed, replacements


def find_mdx_files(root: str) -> List[str]:
    mdx_files: List[str] = []
    for base, _dirs, files in os.walk(root):
        for name in files:
            if name.lower().endswith(".mdx"):
                mdx_files.append(os.path.join(base, name))
    return mdx_files


def main() -> None:
    parser = argparse.ArgumentParser(description="Add caption props to <Frame> tags based on media.jsonl captions")
    parser.add_argument("--media", "-m", required=True, help="Path to media.jsonl file")
    parser.add_argument("--root", "-r", default=".", help="Root directory to scan for .mdx files")
    args = parser.parse_args()

    media_captions = load_media_captions(args.media)
    mdx_files = find_mdx_files(args.root)

    total_files_changed = 0
    total_replacements = 0

    for path in mdx_files:
        changed, replacements = process_file(path, media_captions)
        if changed:
            total_files_changed += 1
            total_replacements += replacements
            print(f"Updated {path} ({replacements} frame{'s' if replacements != 1 else ''})")

    print(f"Done. Files changed: {total_files_changed}. Frames updated: {total_replacements}.")


if __name__ == "__main__":
    main()


