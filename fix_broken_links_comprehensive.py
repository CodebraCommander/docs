#!/usr/bin/env python3
"""
Comprehensive script to fix broken kb://article/ links in MDX files.

This script reads the articles.jsonl file to create a mapping from article_id to slug,
then finds and replaces broken kb://article/ links with proper relative paths.
It also provides detailed reporting on what was fixed and what couldn't be fixed.
"""

import json
import os
import re
import glob
from pathlib import Path
from collections import defaultdict

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

def fix_links_in_file(file_path, articles_mapping, base_dir, stats):
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
                    
                    # Remove .mdx extension for proper markdown links
                    if relative_path.endswith('.mdx'):
                        relative_path = relative_path[:-4]
                    
                    stats['fixed_links'] += 1
                    return relative_path
                except ValueError:
                    # If files are on different drives, use absolute path
                    path_without_ext = mdx_file[:-4] if mdx_file.endswith('.mdx') else mdx_file
                    stats['fixed_links'] += 1
                    return f"/{path_without_ext}"
            else:
                stats['unfound_files'][slug] = stats['unfound_files'].get(slug, 0) + 1
                stats['unfound_article_ids'].add(article_id)
                return match.group(0)  # Keep original if no match found
        else:
            stats['unmapped_article_ids'].add(article_id)
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
    
    # Statistics tracking
    stats = {
        'fixed_links': 0,
        'unfound_files': defaultdict(int),
        'unfound_article_ids': set(),
        'unmapped_article_ids': set(),
        'files_processed': 0,
        'files_modified': 0
    }
    
    print("Loading articles mapping...")
    articles_mapping = load_articles_mapping(articles_jsonl)
    print(f"Loaded {len(articles_mapping)} article mappings")
    
    # Find all MDX files
    mdx_files = glob.glob("**/*.mdx", root_dir=docs_dir, recursive=True)
    print(f"Found {len(mdx_files)} MDX files")
    
    for mdx_file in mdx_files:
        full_path = os.path.join(docs_dir, mdx_file)
        stats['files_processed'] += 1
        
        if fix_links_in_file(full_path, articles_mapping, docs_dir, stats):
            stats['files_modified'] += 1
            print(f"  ✓ Fixed links in {mdx_file}")
    
    # Print summary
    print(f"\n=== SUMMARY ===")
    print(f"Files processed: {stats['files_processed']}")
    print(f"Files modified: {stats['files_modified']}")
    print(f"Total links fixed: {stats['fixed_links']}")
    
    if stats['unfound_files']:
        print(f"\n=== UNFOUND FILES ===")
        for slug, count in sorted(stats['unfound_files'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {slug}: {count} references")
    
    if stats['unfound_article_ids']:
        print(f"\n=== UNFOUND ARTICLE IDs ===")
        for article_id in sorted(stats['unfound_article_ids']):
            print(f"  {article_id}")
    
    if stats['unmapped_article_ids']:
        print(f"\n=== UNMAPPED ARTICLE IDs ===")
        for article_id in sorted(stats['unmapped_article_ids']):
            print(f"  {article_id}")
    
    # Save detailed report
    report_file = "link_fix_report.txt"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("=== LINK FIX REPORT ===\n\n")
        f.write(f"Files processed: {stats['files_processed']}\n")
        f.write(f"Files modified: {stats['files_modified']}\n")
        f.write(f"Total links fixed: {stats['fixed_links']}\n\n")
        
        if stats['unfound_files']:
            f.write("=== UNFOUND FILES ===\n")
            for slug, count in sorted(stats['unfound_files'].items(), key=lambda x: x[1], reverse=True):
                f.write(f"  {slug}: {count} references\n")
            f.write("\n")
        
        if stats['unfound_article_ids']:
            f.write("=== UNFOUND ARTICLE IDs ===\n")
            for article_id in sorted(stats['unfound_article_ids']):
                f.write(f"  {article_id}\n")
            f.write("\n")
        
        if stats['unmapped_article_ids']:
            f.write("=== UNMAPPED ARTICLE IDs ===\n")
            for article_id in sorted(stats['unmapped_article_ids']):
                f.write(f"  {article_id}\n")
    
    print(f"\nDetailed report saved to: {report_file}")

if __name__ == "__main__":
    main()
