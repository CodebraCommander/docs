import os
import re


WORKING_DIRS = [
	"radix",
	"rediq",
]


def read_text(file_path: str) -> str:
	with open(file_path, "r", encoding="utf-8") as f:
		return f.read()


def write_text(file_path: str, content: str) -> None:
	with open(file_path, "w", encoding="utf-8") as f:
		f.write(content)


def extract_frontmatter(content: str):
	# Matches frontmatter delimited by '---' at the start of the file
	match = re.match(r"^---\r?\n([\s\S]*?)\r?\n---\r?\n", content)
	if not match:
		return None, None, None
	start, end = match.span()
	frontmatter = match.group(1)
	body = content[end:]
	return frontmatter, start, body


def get_yaml_value_block(yaml_text: str, key: str):
	# Capture lines like: key: value
	pattern = re.compile(rf"^(?P<indent>[\t ]*){re.escape(key)}:\s*(?P<value>.*)$", re.MULTILINE)
	return pattern.search(yaml_text)


def clean_title_for_sidebar(title: str) -> str:
	original = title.strip()
	# Prefer removing common long words that don't add much in sidebar labels
	replacements = [
		("Report Overview", ""),
		("Overview", ""),
		("Report", ""),
		("Video", ""),
		("Tutorials", ""),
		("Tutorial", ""),
		("Guide", ""),
		("Beta", ""),
	]
	s = original
	for old, new in replacements:
		s = re.sub(rf"\b{re.escape(old)}\b", new, s, flags=re.IGNORECASE)

	# Normalize separators and whitespace
	s = re.sub(r"\s*[-–—]\s*", " ", s)
	s = re.sub(r"\s+/\s+", "/", s)
	s = re.sub(r"\s+", " ", s).strip(" -–—:;.,\t\n\r")

	# If removal made it empty, fall back to original
	if not s:
		s = original

	# If already short enough, return
	if len(s) <= 28:
		return s

	# Keep leading "How to " when possible
	prefix = "How to " if s.lower().startswith("how to ") else ""
	base = s[len(prefix):] if prefix else s

	# Compute max length for base before adding ellipsis and prefix
	max_len = 28
	reserve = len(prefix) + 3  # for prefix + '...'
	cut_len = max_len - reserve
	if cut_len <= 0:
		# Fallback to plain truncation with ellipsis
		return (s[:25].rstrip() + "...")[:28]

	# Cut at word boundary within cut_len
	trim_candidate = base[:cut_len]
	space_idx = trim_candidate.rfind(" ")
	if space_idx > 0:
		trim_candidate = trim_candidate[:space_idx]

	trim_candidate = trim_candidate.rstrip(" -–—:;.,")
	shortened = f"{prefix}{trim_candidate}..."
	# Safety clamp
	return shortened[:28]


def ensure_quoted(value: str) -> str:
	# Always use double quotes, escape internal quotes
	escaped = value.replace('"', '\\"')
	return f'"{escaped}"'


def upsert_sidebar_title(frontmatter: str) -> str:
	# Find title
	title_match = get_yaml_value_block(frontmatter, "title")
	if not title_match:
		return frontmatter  # no title; do nothing

	title_line = title_match.group(0)
	title_value = title_match.group("value").strip()

	# Strip quotes if present
	if (title_value.startswith('"') and title_value.endswith('"')) or (
			title_value.startswith("'") and title_value.endswith("'")
	):
		title_value_unquoted = title_value[1:-1]
	else:
		title_value_unquoted = title_value

	# If title fits, no need to add sidebarTitle unless one already exists and violates length
	sidebar_match = get_yaml_value_block(frontmatter, "sidebarTitle")

	def apply_set(sidebar_text: str) -> str:
		label = clean_title_for_sidebar(title_value_unquoted)
		quoted_label = ensure_quoted(label)
		if sidebar_text:
			return re.sub(r"^sidebarTitle:\s*.*$", f"sidebarTitle: {quoted_label}", frontmatter, flags=re.MULTILINE)
		# Insert sidebarTitle before title line if possible
		lines = frontmatter.splitlines()
		for idx, line in enumerate(lines):
			if re.match(r"^title:\s*", line):
				lines.insert(idx, f"sidebarTitle: {quoted_label}")
				return "\n".join(lines)
		# If no explicit title line match (unlikely), append at end
		return frontmatter.rstrip() + f"\nsidebarTitle: {quoted_label}\n"

	# Decide whether to set/update sidebarTitle
	if len(title_value_unquoted) > 28:
		return apply_set(sidebar_match.group(0) if sidebar_match else None)

	# If title <= 28, only adjust if an existing sidebarTitle exceeds 28
	if sidebar_match:
		sidebar_value = sidebar_match.group("value").strip()
		if (sidebar_value.startswith('"') and sidebar_value.endswith('"')) or (
				sidebar_value.startswith("'") and sidebar_value.endswith("'")
		):
			sidebar_unquoted = sidebar_value[1:-1]
		else:
			sidebar_unquoted = sidebar_value
		if len(sidebar_unquoted) > 28:
			return apply_set(sidebar_match.group(0))

	return frontmatter


def process_file(path: str) -> bool:
	content = read_text(path)
	frontmatter, start, body = extract_frontmatter(content)
	if frontmatter is None:
		return False

	updated_frontmatter = upsert_sidebar_title(frontmatter)
	if updated_frontmatter == frontmatter:
		return False

	new_content = "---\n" + updated_frontmatter + "\n---\n" + body
	write_text(path, new_content)
	return True


def is_mdx_file(filename: str) -> bool:
	return filename.lower().endswith(".mdx")


def main() -> None:
	updated_files = []
	for base in WORKING_DIRS:
		if not os.path.isdir(base):
			continue
		for root, _dirs, files in os.walk(base):
			for file in files:
				if not is_mdx_file(file):
					continue
				full_path = os.path.join(root, file)
				try:
					if process_file(full_path):
						updated_files.append(full_path)
				except Exception as e:
					print(f"[ERROR] {full_path}: {e}")

	if updated_files:
		print(f"Updated {len(updated_files)} files:")
		for p in updated_files:
			print(f" - {p}")
	else:
		print("No changes needed.")


if __name__ == "__main__":
	main()


