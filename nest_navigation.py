import json
from collections import OrderedDict


def build_nested_pages(pages):
	"""Return a pages array where items are either strings or subgroup objects.

	Rules:
	- Paths are expected as 'product/topGroup/[subgroup/...]slug'
	- If a path has at least 2 segments after product (i.e. includes a subgroup),
	  group those pages by the first remaining segment after 'product/topGroup'.
	- Preserve original order of first appearance for subgroups and pages.
	"""
	# Maintain insertion order for subgroups and also collect top-level pages
	subgroup_to_pages = OrderedDict()
	root_level_pages = []

	for p in pages:
		if not isinstance(p, str):
			# Already a nested object; keep as-is
			root_level_pages.append(p)
			continue

		segments = p.split('/')
		if len(segments) < 3:
			# Not enough segments to create a subgroup; keep at root
			root_level_pages.append(p)
			continue

		# product = segments[0]
		# top_group = segments[1]
		rest = segments[2:]
		if len(rest) == 1:
			# No subgroup, just a slug
			root_level_pages.append(p)
			continue

		subgroup = rest[0]
		if subgroup not in subgroup_to_pages:
			subgroup_to_pages[subgroup] = []
		subgroup_to_pages[subgroup].append(p)

	# Compose final pages array: keep any top-level strings/objects first,
	# then append subgroup objects in the order they appeared.
	result_pages = list(root_level_pages)
	for subgroup, paths in subgroup_to_pages.items():
		result_pages.append({
			"group": subgroup,
			"pages": paths,
		})

	return result_pages


def transform_docs_json(doc):
	if "navigation" not in doc or "tabs" not in doc["navigation"]:
		return doc

	for tab in doc["navigation"]["tabs"]:
		groups = tab.get("groups", [])
		for group in groups:
			pages = group.get("pages")
			if not isinstance(pages, list):
				continue
			# Skip if pages are already nested objects (best-effort check)
			has_strings = any(isinstance(x, str) for x in pages)
			if not has_strings:
				continue
			group["pages"] = build_nested_pages(pages)

	return doc


def main():
	with open("docs.json", "r", encoding="utf-8") as f:
		doc = json.load(f)

	doc = transform_docs_json(doc)

	with open("docs.json", "w", encoding="utf-8") as f:
		json.dump(doc, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
	main()


