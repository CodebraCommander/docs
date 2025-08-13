import os
import re


TARGET_DIRS = ["radix", "rediq"]


def read_text(path: str) -> str:
	with open(path, "r", encoding="utf-8") as f:
		return f.read()


def extract_frontmatter(content: str):
	match = re.match(r"^---\r?\n([\s\S]*?)\r?\n---\r?\n", content)
	if not match:
		return None
	return match.group(1)


def get_yaml_value(yaml_text: str, key: str):
	m = re.search(rf"^(?P<i>[\t ]*){re.escape(key)}:\s*(?P<v>.*)$", yaml_text, re.MULTILINE)
	return None if not m else m.group("v").strip()


def unquote(value: str) -> str:
	if not value:
		return value
	if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
		return value[1:-1]
	return value


def find_issues():
	missing_sidebar = []
	too_long_sidebar = []
	for base in TARGET_DIRS:
		if not os.path.isdir(base):
			continue
		for root, _dirs, files in os.walk(base):
			for file in files:
				if not file.lower().endswith(".mdx"):
					continue
				path = os.path.join(root, file)
				try:
					fm = extract_frontmatter(read_text(path))
					if fm is None:
						continue
					title = unquote(get_yaml_value(fm, "title") or "")
					sidebar = unquote(get_yaml_value(fm, "sidebarTitle") or "")
					if len(title) > 28 and not sidebar:
						missing_sidebar.append((path, len(title), title))
					elif sidebar and len(sidebar) > 28:
						too_long_sidebar.append((path, len(sidebar), sidebar))
				except Exception:
					pass
	return missing_sidebar, too_long_sidebar


def main():
	missing, too_long = find_issues()
	print(f"Files with title > 28 and missing sidebarTitle: {len(missing)}")
	for p, n, t in missing:
		print(f" - {p} ({n}): {t}")
	print("")
	print(f"Files with sidebarTitle > 28: {len(too_long)}")
	for p, n, t in too_long:
		print(f" - {p} ({n}): {t}")


if __name__ == "__main__":
	main()


