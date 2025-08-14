import argparse
import concurrent.futures
import json
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

try:
    import boto3
    from botocore.exceptions import ClientError
except Exception:  # pragma: no cover
    boto3 = None
    ClientError = Exception


def load_media_items(jsonl_path: Path) -> List[Dict]:
    items: List[Dict] = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict) and obj.get("media_id"):
                    items.append(obj)
            except json.JSONDecodeError:
                # Skip malformed lines but continue
                print(f"Warning: Skipping malformed JSON on line {line_num}")
                continue
    return items


def build_local_stem_index(images_dir: Path) -> Set[str]:
    stems: Set[str] = set()
    for path in images_dir.rglob("*"):
        if path.is_file():
            stems.add(path.stem)
    return stems


def find_local_by_media_id(images_dir: Path, media_id: str) -> Optional[Path]:
    has_ext = "." in (media_id or "")
    stem = media_id.rsplit(".", 1)[0] if has_ext else media_id
    for path in images_dir.rglob("*"):
        if not path.is_file():
            continue
        if has_ext:
            if path.name == media_id or path.stem == stem:
                return path
        else:
            if path.stem == media_id:
                return path
    return None


def decide_namespace(source_article_id: str) -> str:
    lowered = (source_article_id or "").lower()
    if ":rediq:" in lowered:
        return "rediq"
    if ":radix:" in lowered:
        return "radix"
    # Fallback: try to infer with substring, default to radix
    if "rediq" in lowered:
        return "rediq"
    return "radix"


def is_in_dir(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False


def s3_client(region_name: Optional[str] = None):
    if boto3 is None:
        raise RuntimeError(
            "boto3 is required. Install with: pip install boto3"
        )
    return boto3.client("s3", region_name=region_name)


def s3_head_object(client, bucket: str, key: str, verbose: bool = False) -> Optional[Dict]:
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if verbose:
            err = getattr(e, "response", {}).get("Error", {})
            code = err.get("Code")
            msg = err.get("Message")
            print(f"  - head_object error for s3://{bucket}/{key}: {code} {msg}")
        if getattr(e, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
            return None
        # Some SDKs raise with Code '404' but still as ClientError
        code = getattr(e, "response", {}).get("Error", {}).get("Code")
        if code in {"404", "NotFound", "NoSuchKey"}:
            return None
        return None


def s3_find_key_for_media(client, bucket: str, prefix: str, media_id: str, ext: Optional[str], verbose: bool = False) -> Optional[str]:
    # If media_id already contains an extension, try that exact key first
    id_has_ext = "." in (media_id or "")
    if id_has_ext:
        exact_key = f"{prefix}{media_id}"
        if verbose:
            print(f"  - probing exact {exact_key}")
        if s3_head_object(client, bucket, exact_key, verbose=verbose):
            return exact_key

    # Try direct match with provided ext (only if id didn't include it)
    if not id_has_ext and ext:
        normalized_exts: List[str] = []
        e = ext.lstrip(".")
        normalized_exts = [e]
        # Common alias handling
        if e.lower() == "jpg":
            normalized_exts.append("jpeg")
        if e.lower() == "jpeg":
            normalized_exts.append("jpg")
        for e2 in normalized_exts:
            key = f"{prefix}{media_id}.{e2}"
            if verbose:
                print(f"  - probing {key}")
            if s3_head_object(client, bucket, key, verbose=verbose):
                return key

    # Fallback: list by prefix and pick first
    list_prefix = f"{prefix}{media_id}"
    if not id_has_ext:
        list_prefix = f"{list_prefix}."
    try:
        if verbose:
            print(f"  - listing with prefix {list_prefix}")
        resp = client.list_objects_v2(Bucket=bucket, Prefix=list_prefix, MaxKeys=25)
        contents = resp.get("Contents", [])
        if contents:
            keys = [c["Key"] for c in contents]
            preferred_ext_order = ["png", "jpg", "jpeg", "gif", "webp", "bmp"]
            def sort_key(k: str) -> Tuple[int, str]:
                ext_found = k.rsplit(".", 1)[-1].lower() if "." in k else ""
                try:
                    idx = preferred_ext_order.index(ext_found)
                except ValueError:
                    idx = len(preferred_ext_order)
                return (idx, k)
            keys.sort(key=sort_key)
            return keys[0]
    except ClientError as e:
        if verbose:
            err = getattr(e, "response", {}).get("Error", {})
            code = err.get("Code")
            msg = err.get("Message")
            print(f"  - list_objects_v2 error for prefix {list_prefix}: {code} {msg}")
        return None
    return None


def download_s3_object(client, bucket: str, key: str, dest_path: Path) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(bucket, key, str(dest_path))


def ensure_ext_from_key(key: str) -> str:
    return f".{key.rsplit('.', 1)[-1]}" if "." in key else ""


def process_item(
    item: Dict,
    images_dir: Path,
    radix_dir: Path,
    rediq_dir: Path,
    bucket: str,
    prefix: str,
    region_name: Optional[str],
    dry_run: bool,
    verbose: bool,
) -> Tuple[str, str]:
    media_id = item.get("media_id")
    ext = item.get("ext") or ""
    source_article_id = item.get("source_article_id") or ""

    if not media_id:
        return ("skipped", "Missing media_id")

    # Local check by stem across all of images
    local_found = find_local_by_media_id(images_dir, media_id)
    # Decide namespace for destination
    namespace = decide_namespace(source_article_id)
    dest_root = rediq_dir if namespace == "rediq" else radix_dir
    if local_found:
        # If found but in wrong namespace, move/correct if needed
        if not is_in_dir(local_found, dest_root):
            dest_path = dest_root / local_found.name
            if dry_run:
                return ("would_move", f"{local_found} -> {dest_path}")
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            if dest_path.exists():
                return (
                    "exists_but_wrong_namespace",
                    f"{local_found} should be {dest_path} (dest already exists)",
                )
            local_found.rename(dest_path)
            return ("moved", f"{local_found} -> {dest_path}")
        return ("exists_local", str(local_found))


    client = s3_client(region_name)
    key = s3_find_key_for_media(client, bucket, prefix, media_id, ext, verbose=verbose)
    if not key:
        return ("missing_s3", f"{media_id}")

    # Use the basename from S3 key for local filename to avoid double extensions
    dest_filename = os.path.basename(key)
    dest_path = dest_root / dest_filename
    if dry_run:
        return ("would_download", f"s3://{bucket}/{key} -> {dest_path}")

    download_s3_object(client, bucket, key, dest_path)
    return ("downloaded", f"{dest_path}")


def run(
    media_jsonl: Path,
    images_dir: Path,
    bucket: str,
    prefix: str,
    region_name: Optional[str],
    max_workers: int,
    dry_run: bool,
    verbose: bool,
) -> None:
    items = load_media_items(media_jsonl)
    print(f"Loaded {len(items)} media items from {media_jsonl}")

    radix_dir = images_dir / "radix"
    rediq_dir = images_dir / "rediq"

    # Preflight: verify bucket/prefix access
    try:
        client = s3_client(region_name)
        probe = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        count = probe.get("KeyCount", 0)
        first_key = (probe.get("Contents", [{}])[0] or {}).get("Key") if count else None
        msg = f"S3 access OK: {bucket}/{prefix} (KeyCount={count}{', first=' + first_key if first_key else ''})"
        print(msg)
    except ClientError as e:
        err = getattr(e, "response", {}).get("Error", {})
        code = err.get("Code")
        msg = err.get("Message")
        print(f"S3 access error for s3://{bucket}/{prefix}: {code} {msg}")
        if code in {"AccessDenied", "AccessForbidden", "Unauthorized"}:
            print("Hint: Your AWS credentials may not have s3:ListBucket permissions for this bucket/prefix.")

    results: Dict[str, int] = {
        "exists_local": 0,
        "downloaded": 0,
        "missing_s3": 0,
        "would_download": 0,
        "would_move": 0,
        "moved": 0,
        "exists_but_wrong_namespace": 0,
        "skipped": 0,
        "error": 0,
    }

    def worker(item: Dict) -> Tuple[str, str, str]:
        media_id = item.get("media_id", "")
        try:
            status, detail = process_item(
                item,
                images_dir,
                radix_dir,
                rediq_dir,
                bucket,
                prefix,
                region_name,
                dry_run,
                verbose,
            )
            return (media_id, status, detail)
        except Exception as e:  # pragma: no cover
            return (media_id, "error", str(e))

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker, item) for item in items]
        for fut in concurrent.futures.as_completed(futures):
            media_id, status, detail = fut.result()
            results[status] = results.get(status, 0) + 1
            print(f"[{status}] {media_id} - {detail}")

    print("\nSummary:")
    for k, v in results.items():
        print(f"- {k}: {v}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill missing images from S3 using media.jsonl")
    parser.add_argument(
        "--media-jsonl",
        required=True,
        help="Path to media.jsonl file (JSON lines)",
    )
    parser.add_argument(
        "--images-dir",
        default=str(Path(__file__).resolve().parents[1] / "images"),
        help="Path to local images directory (contains radix/ and rediq/)",
    )
    parser.add_argument(
        "--bucket",
        default="combined-help-center-images",
        help="S3 bucket name",
    )
    parser.add_argument(
        "--prefix",
        default="kb/media/",
        help="S3 key prefix for media objects",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="AWS region (optional)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Max concurrent workers",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report actions; do not download",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose S3 probing/logging",
    )
    args = parser.parse_args()

    media_jsonl = Path(args.media_jsonl).expanduser().resolve()
    images_dir = Path(args.images_dir).expanduser().resolve()

    if not media_jsonl.exists():
        raise SystemExit(f"media.jsonl not found: {media_jsonl}")
    if not images_dir.exists():
        raise SystemExit(f"images directory not found: {images_dir}")

    run(
        media_jsonl=media_jsonl,
        images_dir=images_dir,
        bucket=args.bucket,
        prefix=args.prefix,
        region_name=args.region,
        max_workers=args.workers,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()


