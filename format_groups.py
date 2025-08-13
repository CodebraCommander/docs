import json
from typing import Any, Dict, List, Union


Page = Union[str, Dict[str, Any]]


# Explicit display name overrides for subgroups
SUBGROUP_DISPLAY_NAME = {
	# redIQ > dataIQ
	"deals": "Deals",
	"rent-rolls": "Rent Rolls",
	"operating-statements": "Operating Statements",
	"smartmap": "Smartmap+",
	"firstpass": "FirstPass",
	"radix-research": "Radix Research",
	"settings-and-admin": "Settings and Admin",
	# redIQ > valuationIQ
	"about": "About",
	"how-to-use-the-model": "How to Use the Model",
	"troubleshooting": "Troubleshooting",
	# redIQ > QuickSync
	"getting-started": "Getting Started",
	"rent-roll": "Rent Roll",
	"best-practices-and-tips": "Best Practices and Tips",
	"video-tutorials": "Video Tutorials",
}


def default_title_case(slug: str) -> str:
	# Replace dashes with spaces and title-case words
	words = slug.replace("_", "-").split("-")
	return " ".join(w.capitalize() for w in words if w)


def display_name_for_subgroup(slug: str) -> str:
	return SUBGROUP_DISPLAY_NAME.get(slug, default_title_case(slug))


def reorder_groups(groups: List[Dict[str, Any]], desired_order: List[str]) -> List[Dict[str, Any]]:
	index = {name: i for i, name in enumerate(desired_order)}
	# Stable sort by desired index, unknowns go to end preserving existing order
	def sort_key(g: Dict[str, Any]):
		name = g.get("group", "")
		return (index.get(name, 10_000),)
	return sorted(groups, key=sort_key)


def infer_slug_from_item(item: Page) -> str:
	if isinstance(item, dict):
		subpages = item.get("pages")
		if isinstance(subpages, list) and subpages:
			first = subpages[0]
			if isinstance(first, str):
				parts = first.split("/")
				if len(parts) >= 3:
					# parts[0]=product, parts[1]=topGroup, parts[2]=subgroup slug
					return parts[2].lower()
		# Fallback: normalize current name into slug-ish
		name = str(item.get("group", "")).strip().lower().replace(" ", "-")
		return name
	return ""


def reorder_subgroups(pages: List[Page], desired_order_by_slug: List[str]) -> List[Page]:
	index = {slug: i for i, slug in enumerate(desired_order_by_slug)}
	# Stable sort, using inferred slug from first page path
	def sort_key(item: Page):
		if isinstance(item, dict):
			slug = infer_slug_from_item(item)
			return (index.get(slug, 10_000),)
		return (9_999,)
	return sorted(pages, key=sort_key)


def transform(doc: Dict[str, Any]) -> Dict[str, Any]:
	if "navigation" not in doc or "tabs" not in doc["navigation"]:
		return doc

	for tab in doc["navigation"]["tabs"]:
		# First, apply top-level group ordering per tab
		if tab.get("tab") == "redIQ":
			desired_group_order = [
				"dataIQ",
				"valuationIQ",
				"QuickSync",
				"Training Resources",
				"Contacting Support",
			]
			if "groups" in tab:
				tab["groups"] = reorder_groups(tab["groups"], desired_group_order)
		elif tab.get("tab") == "Radix":
			desired_group_order = [
				"Getting Started",
				"RealRents",
				"Benchmark",
				"Research",
				"Proforma",
				"Integrations",
				"API Reference",
				"Legacy",
			]
			if "groups" in tab:
				tab["groups"] = reorder_groups(tab["groups"], desired_group_order)

		# Then, walk groups to title-case subgroup names and order subgroups where specified
		for group in tab.get("groups", []):
			group_name = group.get("group", "")
			pages = group.get("pages")
			if not isinstance(pages, list):
				continue

			# For specific parent groups, apply desired subgroup order by slug
			if tab.get("tab") == "redIQ" and group_name == "dataIQ":
				desired_subgroup_slugs = [
					"deals",
					"rent-rolls",
					"operating-statements",
					"smartmap",
					"firstpass",
					"radix-research",
					"settings-and-admin",
				]
				pages = reorder_subgroups(pages, desired_subgroup_slugs)
			elif tab.get("tab") == "redIQ" and group_name == "valuationIQ":
				desired_subgroup_slugs = [
					"about",
					"how-to-use-the-model",
					"troubleshooting",
				]
				pages = reorder_subgroups(pages, desired_subgroup_slugs)
			elif tab.get("tab") == "redIQ" and group_name == "QuickSync":
				desired_subgroup_slugs = [
					"getting-started",
					"rent-roll",
					"best-practices-and-tips",
					"video-tutorials",
				]
				pages = reorder_subgroups(pages, desired_subgroup_slugs)

			# Rename subgroup display names based on inferred slug (from first page path)
			new_pages: List[Page] = []
			for item in pages:
				if isinstance(item, dict) and "group" in item:
					slug = infer_slug_from_item(item)
					item["group"] = display_name_for_subgroup(slug)
					# Recurse into deeper levels
					subpages = item.get("pages")
					if isinstance(subpages, list):
						# No special order requested beyond first level; still title-case deeper subgroup names using slug inference
						deep_items: List[Page] = []
						for deep in subpages:
							if isinstance(deep, dict) and "group" in deep:
								deep_slug = infer_slug_from_item(deep)
								deep["group"] = display_name_for_subgroup(deep_slug)
							deep_items.append(deep)
						item["pages"] = deep_items
				new_pages.append(item)
			group["pages"] = new_pages

	return doc


def main():
	with open("docs.json", "r", encoding="utf-8") as f:
		doc = json.load(f)

	doc = transform(doc)

	with open("docs.json", "w", encoding="utf-8") as f:
		json.dump(doc, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
	main()


