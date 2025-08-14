#!/usr/bin/env python3
"""
Simple script to remove .mdx extensions from markdown links in MDX files.
This script processes files one at a time and only targets actual markdown links.
"""

import os
import re
import glob

def fix_mdx_extensions_in_file(file_path):
    """Fix .mdx extensions in markdown links in a single MDX file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        try:
            with open(file_path, 'r', encoding='latin-1') as f:
                content = f.read()
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return False
    
    original_content = content
    
    # Very specific pattern to match ONLY markdown links ending with .mdx)
    # This looks for [text](path.mdx) pattern - actual markdown links
    # The pattern ensures we have:
    # 1. [text] - link text in brackets
    # 2. (path.mdx) - path in parentheses ending with .mdx
    pattern = r'(\[[^\]]+\]\()([^)]+)\.mdx\)'
    
    def replace_link(match):
        link_text = match.group(1)  # [text](
        link_path = match.group(2)  # path (without .mdx)
        return f"{link_text}{link_path})"
    
    # Replace all occurrences
    content = re.sub(pattern, replace_link, content)
    
    # Write back if content changed
    if content != original_content:
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        except Exception as e:
            print(f"Error writing {file_path}: {e}")
            return False
    
    return False

def main():
    # Configuration
    docs_dir = r"C:\Users\BenBeggs\Documents\GitHub\docs"
    
    # Find all MDX files
    mdx_files = glob.glob("**/*.mdx", root_dir=docs_dir, recursive=True)
    print(f"Found {len(mdx_files)} MDX files")
    
    fixed_count = 0
    
    for i, mdx_file in enumerate(mdx_files, 1):
        full_path = os.path.join(docs_dir, mdx_file)
        print(f"Processing {i}/{len(mdx_files)}: {mdx_file}")
        
        if fix_mdx_extensions_in_file(full_path):
            fixed_count += 1
            print(f"  ✓ Fixed .mdx extensions in {mdx_file}")
    
    print(f"\nSummary: Fixed .mdx extensions in {fixed_count} files")

if __name__ == "__main__":
    main()
