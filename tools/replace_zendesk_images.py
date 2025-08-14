import json
import os
import re
from pathlib import Path
from typing import Dict, Tuple


# Note: Not used directly; kept for reference if needed later
ZENDESK_URL_PATTERN = re.compile(
	 r"https?://(?:help\.(?:radix|rediq)\.com|rediq\.zendesk\.com)/[\w\-/]*/?(?:attachments|articles)/[^)\s'\"]*?([A-Za-z0-9_\-]+\.(?:png|jpg|jpeg|gif))",
	 re.IGNORECASE,
)


def load_media_map(media_jsonl_path: str) -> Dict[str, str]:
	"""Load mapping of original_name (lowercase) -> media_id from media.jsonl."""
	media_map: Dict[str, str] = {}
	with open(media_jsonl_path, "r", encoding="utf-8") as f:
		for line in f:
			line = line.strip()
			if not line:
				continue
			try:
				obj = json.loads(line)
			except Exception:
				continue
			orig = str(obj.get("original_name") or "").strip()
			media_id = obj.get("media_id")
			if orig and media_id:
				media_map[orig.lower()] = media_id
	return media_map


def replace_urls_in_content(content: str, product: str, media_map: Dict[str, str]) -> Tuple[str, int]:
	"""Replace Zendesk image URLs with local /images/<product>/<media_id> paths.

	Returns updated content and number of replacements made.
	"""
	replacements = 0

	def repl_img_tag(match: re.Match) -> str:
		filename = match.group(1)
		media_id = media_map.get(filename.lower())
		if not media_id:
			return match.group(0)
		return match.group(0).replace(match.group(0), f"/images/{product}/{media_id}")

	# First, handle <img src="..."> patterns
	def img_src_sub(m: re.Match) -> str:
		full = m.group(0)
		url = m.group(1)
		# Extract filename from URL
		mfile = re.search(r"([A-Za-z0-9_\-]+\.(?:png|jpg|jpeg|gif))", url, re.IGNORECASE)
		if not mfile:
			return full
		filename = mfile.group(1)
		media_id = media_map.get(filename.lower())
		if not media_id:
			return full
		nonlocal replacements
		replacements += 1
		return full.replace(url, f"/images/{product}/{media_id}")

	content = re.sub(r"<img\s+[^>]*src=\"([^\"]+)\"", img_src_sub, content, flags=re.IGNORECASE)

	# Markdown images ![alt](URL)
	def md_img_sub(m: re.Match) -> str:
		alt = m.group(1)
		url = m.group(2)
		mfile = re.search(r"([A-Za-z0-9_\-]+\.(?:png|jpg|jpeg|gif))", url, re.IGNORECASE)
		if not mfile:
			return m.group(0)
		filename = mfile.group(1)
		media_id = media_map.get(filename.lower())
		if not media_id:
			return m.group(0)
		nonlocal replacements
		replacements += 1
		return f"![{alt}](/images/{product}/{media_id})"

	content = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", md_img_sub, content)

	# Linked images: [![alt](URL)](URL2) -> ![alt](/images/...)
	def linked_img_sub(m: re.Match) -> str:
		alt = m.group(1)
		inner_url = m.group(2)
		mfile = re.search(r"([A-Za-z0-9_\-]+\.(?:png|jpg|jpeg|gif))", inner_url, re.IGNORECASE)
		if not mfile:
			return m.group(0)
		filename = mfile.group(1)
		media_id = media_map.get(filename.lower())
		if not media_id:
			return m.group(0)
		nonlocal replacements
		replacements += 1
		return f"![{alt}](/images/{product}/{media_id})"

	content = re.sub(r"\[!\[([^\]]*)\]\(([^)]+)\)\]\([^)]+\)", linked_img_sub, content)

	return content, replacements


def main():
	import argparse
	parser = argparse.ArgumentParser(description="Replace Zendesk image URLs with local images paths")
	parser.add_argument("media_jsonl", help="Path to media.jsonl with original_name -> media_id")
	parser.add_argument("root", nargs="?", default=".", help="Docs root (default: .)")
	args = parser.parse_args()

	media_map = load_media_map(args.media_jsonl)
	root = Path(args.root)

	mdx_files = list(root.rglob("*.mdx"))
	total_changed = 0
	total_replacements = 0
	for mdx in mdx_files:
		try:
			text = mdx.read_text(encoding="utf-8")
		except Exception:
			continue
		# Determine product from path prefix
		rel = mdx.relative_to(root)
		parts = rel.parts
		product = "rediq" if parts and parts[0].lower() == "rediq" else "radix" if parts and parts[0].lower() == "radix" else "general"
		updated, replacements = replace_urls_in_content(text, product, media_map)
		if replacements > 0 and updated != text:
			mdx.write_text(updated, encoding="utf-8")
			total_changed += 1
			total_replacements += replacements

	print(f"Updated {total_changed} files, replaced {total_replacements} image URL(s)")


if __name__ == "__main__":
	main()


