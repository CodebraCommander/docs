import json
import os
import re
from typing import Any, Dict, List, Tuple, Union


Page = Union[str, Dict[str, Any]]


def read_title_for_page(path_without_ext: str) -> str:
	"""Attempt to read a human title from the MDX file.

	Strategies:
	- Look for first markdown heading '# ' or '## ' line
	- Fallback to filename slug
	"""
	candidate_paths = [f"{path_without_ext}.mdx", f"{path_without_ext}.md"]
	for p in candidate_paths:
		if os.path.exists(p):
			try:
				with open(p, "r", encoding="utf-8") as f:
					for line in f:
						line_stripped = line.strip()
						if line_stripped.startswith("# ") or line_stripped.startswith("## "):
							return line_stripped.lstrip("# ").strip()
			except Exception:
				pass
	# Fallback to last segment slug
	return os.path.basename(path_without_ext)


def compute_priority(slug: str, title: str, group_name: str) -> int:
	"""Lower score means earlier appearance.
	Combine generic heuristics and group-specific tweaks.
	"""
	slug_l = slug.lower()
	title_l = title.lower()

	score = 1000

	def bump(delta: int):
		nonlocal score
		score += delta

	# Generic early items
	if any(k in slug_l or k in title_l for k in ["overview", "introduction", "what is", "about"]):
		bump(-600)
	if any(k in slug_l for k in ["getting-started", "gettingstarted", "setup", "set-up", "installation", "account-setup", "accountsetup"]):
		bump(-500)

	# Generic late items
	if any(k in slug_l or k in title_l for k in ["faq", "faqs", "troubleshooting"]):
		bump(+250)
	if "beta" in slug_l:
		bump(+120)
	if any(k in slug_l for k in ["delete", "remov", "deprecate"]):
		bump(+60)

	# Group-specific ordering
	if group_name.lower() == "deals":
		# Core navigation flow for deals
		# Deal log, page, headers, name tab, action menu, settings/filters, create, share, reports, special cases
		if "deal-log" in slug_l:
			bump(-400)
		if "deal-page" in slug_l:
			bump(-390)
		if "deal-headers" in slug_l:
			bump(-380)
		if "name-tab" in slug_l:
			bump(-370)
		if "deal-action-menu" in slug_l:
			bump(-360)
		if "search-setting-filters" in slug_l or ("filter" in slug_l and "deal" in slug_l):
			bump(-350)
		if "how-to-create" in slug_l or ("create" in slug_l and "deal" in slug_l):
			bump(-340)
		if "share-deal" in slug_l:
			bump(-330)
		# Reports later
		if any(k in slug_l for k in ["comp-report", "expense-and-rent-comp-report", "pipeline-report"]):
			bump(+40)
		# Segment-specific late items
		if any(k in slug_l for k in ["student-housing", "single-family", "sfr-"]):
			bump(+80)

	return score


def reorder_pages(pages: List[Page], group_name: str) -> List[Page]:
	# Build sortable list with computed score; keep stable by original index
	scored: List[Tuple[int, int, Page]] = []
	for idx, item in enumerate(pages):
		if isinstance(item, str):
			slug = item
			title = read_title_for_page(item)
			score = compute_priority(slug, title, group_name)
			scored.append((score, idx, item))
		else:
			# Nested subgroup
			subgroup_name = item.get("group", group_name)
			subpages = item.get("pages", [])
			item["pages"] = reorder_pages(subpages, subgroup_name)
			# Score subgroup itself by generic heuristics on group name
			score = compute_priority(subgroup_name, subgroup_name, group_name)
			scored.append((score, idx, item))

	# Sort by (score, original_index) for stable ordering
	scored.sort(key=lambda t: (t[0], t[1]))
	return [it for _, __, it in scored]


def transform_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
	if "navigation" not in doc or "tabs" not in doc["navigation"]:
		return doc
	for tab in doc["navigation"]["tabs"]:
		groups = tab.get("groups", [])
		for group in groups:
			group_name = group.get("group", "")
			pages = group.get("pages")
			if isinstance(pages, list):
				group["pages"] = reorder_pages(pages, group_name)
	return doc


def main():
	with open("docs.json", "r", encoding="utf-8") as f:
		doc = json.load(f)

	doc = transform_doc(doc)

	with open("docs.json", "w", encoding="utf-8") as f:
		json.dump(doc, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
	main()


