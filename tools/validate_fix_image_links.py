import argparse
import re
from pathlib import Path
from typing import Dict, List, Tuple


IMG_SRC_HTML = re.compile(r"<img\s+[^>]*src=[\"\']([^\"\']+)[\"\']", re.IGNORECASE)
IMG_SRC_MD = re.compile(r"!\[[^\]]*\]\(([^)]+)\)")


def build_image_index(images_dir: Path) -> Dict[str, List[Path]]:
    index: Dict[str, List[Path]] = {}
    for p in images_dir.rglob("*"):
        if not p.is_file():
            continue
        key = p.name.lower()
        index.setdefault(key, []).append(p)
    return index


def expected_namespace_for_mdx(mdx_path: Path, root: Path) -> str:
    try:
        rel = mdx_path.relative_to(root)
        top = rel.parts[0].lower()
        if top == "rediq":
            return "rediq"
        if top == "radix":
            return "radix"
    except Exception:
        pass
    return "general"


def validate_and_fix_content(
    content: str,
    images_dir: Path,
    image_index: Dict[str, List[Path]],
    mdx_namespace: str,
    prefer_actual_location: bool,
) -> Tuple[str, int, int, int, List[str], List[str]]:
    """Return (updated_content, fixed_count, still_missing, ambiguous, missing_urls, ambiguous_urls)."""

    fixed = 0
    missing = 0
    ambiguous = 0
    missing_urls: List[str] = []
    ambiguous_urls: List[str] = []

    def normalize_url(url: str) -> str:
        return url.strip()

    def rewrite_url(url: str) -> Tuple[str, bool, bool, bool]:
        """Try to rewrite a single URL.

        Returns: (new_url, did_fix, is_missing, is_ambiguous)
        """
        norm = normalize_url(url)
        if not (norm.startswith("/images/") or norm.startswith("images/")):
            return (url, False, False, False)

        # Extract filename
        parts = norm.split("/")
        filename = parts[-1]
        if not filename:
            return (url, False, False, False)

        candidates = image_index.get(filename.lower(), [])
        if len(candidates) == 0:
            return (url, False, True, False)
        if len(candidates) > 1:
            # Prefer one under images/<mdx_namespace>/ if present
            preferred = [p for p in candidates if f"/images/{mdx_namespace}/" in str(p.as_posix())]
            chosen = preferred[0] if preferred else candidates[0]
            # If multiple and not same namespace, mark ambiguous but still fix to chosen
            chosen_rel = chosen.relative_to(images_dir).as_posix()
            new_url = f"/images/{chosen_rel}"
            return (new_url, True, False, True)

        chosen = candidates[0]
        chosen_rel = chosen.relative_to(images_dir).as_posix()
        new_url = f"/images/{chosen_rel}"

        # If prefer_actual_location is False and the URL already matches namespace, keep as-is
        if not prefer_actual_location:
            # Compare namespaces
            try:
                current_ns = parts[2] if norm.startswith("/images/") and len(parts) >= 4 else None
            except Exception:
                current_ns = None
            actual_ns = chosen_rel.split("/")[0] if "/" in chosen_rel else None
            if current_ns and actual_ns and current_ns == actual_ns:
                return (url, False, False, False)

        if new_url != norm:
            return (new_url, True, False, False)
        return (url, False, False, False)

    def sub_html(m: re.Match) -> str:
        nonlocal fixed, missing, ambiguous
        url = m.group(1)
        new_url, did_fix, is_missing, is_ambiguous = rewrite_url(url)
        if is_missing:
            missing += 1
            missing_urls.append(url)
        if is_ambiguous:
            ambiguous += 1
            ambiguous_urls.append(url)
        if did_fix:
            fixed += 1
            return m.group(0).replace(url, new_url)
        return m.group(0)

    def sub_md(m: re.Match) -> str:
        nonlocal fixed, missing, ambiguous
        url = m.group(1)
        new_url, did_fix, is_missing, is_ambiguous = rewrite_url(url)
        if is_missing:
            missing += 1
            missing_urls.append(url)
        if is_ambiguous:
            ambiguous += 1
            ambiguous_urls.append(url)
        if did_fix:
            fixed += 1
            return m.group(0).replace(url, new_url)
        return m.group(0)

    updated = IMG_SRC_HTML.sub(sub_html, content)
    updated = IMG_SRC_MD.sub(sub_md, updated)
    return updated, fixed, missing, ambiguous, missing_urls, ambiguous_urls


def run(root: Path, images_dir: Path, write: bool, prefer_actual_location: bool) -> None:
    image_index = build_image_index(images_dir)

    mdx_files = list(root.rglob("*.mdx"))
    total_fixed = 0
    total_missing = 0
    total_ambiguous = 0
    changed_files = 0
    missing_by_file: Dict[Path, List[str]] = {}
    ambiguous_by_file: Dict[Path, List[str]] = {}

    for mdx in mdx_files:
        try:
            content = mdx.read_text(encoding="utf-8")
        except Exception:
            continue
        mdx_ns = expected_namespace_for_mdx(mdx, root)
        updated, fixed, missing, ambiguous, missing_urls, ambiguous_urls = validate_and_fix_content(
            content,
            images_dir,
            image_index,
            mdx_ns,
            prefer_actual_location,
        )
        if fixed > 0 and updated != content:
            changed_files += 1
            if write:
                mdx.write_text(updated, encoding="utf-8")
        total_fixed += fixed
        total_missing += missing
        total_ambiguous += ambiguous
        if missing_urls:
            # Deduplicate while preserving order
            seen = set()
            deduped = []
            for u in missing_urls:
                if u not in seen:
                    seen.add(u)
                    deduped.append(u)
            missing_by_file[mdx] = deduped
        if ambiguous_urls:
            seen_a = set()
            deduped_a = []
            for u in ambiguous_urls:
                if u not in seen_a:
                    seen_a.add(u)
                    deduped_a.append(u)
            ambiguous_by_file[mdx] = deduped_a

    print(f"Files changed: {changed_files}")
    print(f"Links fixed: {total_fixed}")
    print(f"Missing images (no local match): {total_missing}")
    print(f"Ambiguous matches (multiple local copies): {total_ambiguous}")

    if total_missing > 0:
        print("\nMissing list:")
        for mdx, urls in missing_by_file.items():
            rel = mdx.relative_to(root)
            print(f"- {rel}")
            for u in urls:
                print(f"  * {u}")

    if total_ambiguous > 0:
        print("\nAmbiguous list:")
        for mdx, urls in ambiguous_by_file.items():
            rel = mdx.relative_to(root)
            print(f"- {rel}")
            for u in urls:
                print(f"  * {u}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate and fix image links in MDX files against local images")
    parser.add_argument("--root", default=".", help="Docs root (default: .)")
    parser.add_argument("--images-dir", default="./images", help="Path to images directory")
    parser.add_argument("--write", action="store_true", help="Write changes to files (default is dry-run)")
    parser.add_argument(
        "--prefer-actual-location",
        action="store_true",
        help="Rewrite to where the file actually exists instead of keeping current namespace if valid",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    images_dir = Path(args.images_dir).resolve()
    if not images_dir.exists():
        raise SystemExit(f"Images directory not found: {images_dir}")

    run(root=root, images_dir=images_dir, write=args.write, prefer_actual_location=args.prefer_actual_location)


if __name__ == "__main__":
    main()


