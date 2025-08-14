#!/usr/bin/env python3
"""
Script to fix broken kb://article/ links in MDX files.

This script reads the articles.jsonl file to create a mapping from article_id to slug,
then finds and replaces broken kb://article/ links with proper relative paths.
"""

import json
import os
import re
import glob
from pathlib import Path

def load_articles_mapping(jsonl_file):
    """Load articles.jsonl and create a mapping from article_id to slug."""
    mapping = {}
    
    with open(jsonl_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    article = json.loads(line)
                    article_id = article.get('article_id')
                    slug = article.get('slug')
                    if article_id and slug:
                        mapping[article_id] = slug
                except json.JSONDecodeError:
                    print(f"Warning: Could not parse JSON line: {line[:100]}...")
                    continue
    
    return mapping

def find_mdx_file_by_slug(slug, base_dir):
    """Find the MDX file that matches the given slug."""
    # Search for files with the slug as the filename (without extension)
    pattern = f"**/{slug}.mdx"
    matches = glob.glob(pattern, root_dir=base_dir, recursive=True)
    
    if matches:
        # Return the first match as a relative path
        return matches[0]
    
    # If no exact match, try to find files that contain the slug in their name
    pattern = f"**/*{slug}*.mdx"
    matches = glob.glob(pattern, root_dir=base_dir, recursive=True)
    
    if matches:
        return matches[0]
    
    return None

def fix_links_in_file(file_path, articles_mapping, base_dir):
    """Fix broken kb://article/ links in a single MDX file."""
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
    
    # Pattern to match kb://article/ links
    pattern = r'kb://article/([^)\s]+)'
    
    def replace_link(match):
        article_id = match.group(1)
        
        if article_id in articles_mapping:
            slug = articles_mapping[article_id]
            mdx_file = find_mdx_file_by_slug(slug, base_dir)
            
            if mdx_file:
                # Convert to relative path from the current file
                current_file_dir = os.path.dirname(file_path)
                target_file_path = os.path.join(base_dir, mdx_file)
                
                # Calculate relative path
                try:
                    relative_path = os.path.relpath(target_file_path, current_file_dir)
                    # Convert Windows path separators to forward slashes for web compatibility
                    relative_path = relative_path.replace('\\', '/')
                    return relative_path
                except ValueError:
                    # If files are on different drives, use absolute path
                    return f"/{mdx_file}"
            else:
                print(f"Warning: Could not find MDX file for slug '{slug}' (article_id: {article_id})")
                return match.group(0)  # Keep original if no match found
        else:
            print(f"Warning: Article ID '{article_id}' not found in articles mapping")
            return match.group(0)  # Keep original if not in mapping
    
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
    articles_jsonl = r"C:\Users\BenBeggs\Downloads\articles.jsonl"
    docs_dir = r"C:\Users\BenBeggs\Documents\GitHub\docs"
    
    print("Loading articles mapping...")
    articles_mapping = load_articles_mapping(articles_jsonl)
    print(f"Loaded {len(articles_mapping)} article mappings")
    
    # Find all MDX files
    mdx_files = glob.glob("**/*.mdx", root_dir=docs_dir, recursive=True)
    print(f"Found {len(mdx_files)} MDX files")
    
    fixed_count = 0
    
    for mdx_file in mdx_files:
        full_path = os.path.join(docs_dir, mdx_file)
        print(f"Processing: {mdx_file}")
        
        if fix_links_in_file(full_path, articles_mapping, docs_dir):
            fixed_count += 1
            print(f"  ✓ Fixed links in {mdx_file}")
    
    print(f"\nSummary: Fixed links in {fixed_count} files")

if __name__ == "__main__":
    main()
