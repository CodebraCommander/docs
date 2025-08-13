#!/usr/bin/env python3
"""
S3 to Mintlify Documentation Migration Script
Converts S3-based knowledge base to Mintlify GitHub repository format
"""

import json
import os
import re
import yaml
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import boto3
from botocore.exceptions import ClientError

class MintlifyMigrator:
    """Migrates S3-based documentation to Mintlify format"""
    
    def __init__(self, bucket: str, prefix: str, output_dir: str, aws_profile: Optional[str] = None):
        """
        Initialize the migrator
        
        Args:
            bucket: S3 bucket name
            prefix: S3 prefix (e.g., 'kb/')
            output_dir: Local output directory for Mintlify docs
            aws_profile: Optional AWS profile name
        """
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
            self.s3_client = session.client('s3')
        else:
            self.s3_client = boto3.client('s3')
            
        self.bucket = bucket
        self.prefix = prefix
        self.output_dir = Path(output_dir)
        
        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / 'images').mkdir(exist_ok=True)
        (self.output_dir / 'snippets').mkdir(exist_ok=True)
        
        # Track processed items
        self.articles = {}
        self.media_map = {}
        self.navigation_structure = {'radix': {}, 'rediq': {}}
        self.migration_stats = {
            'articles_processed': 0,
            'media_processed': 0,
            'errors': [],
            'warnings': []
        }

    def run_migration(self):
        """Execute the complete migration process"""
        print("🚀 Starting Mintlify migration...")
        
        # Step 1: Load articles and metadata
        print("\n📚 Loading articles from S3...")
        self.load_articles()
        
        # Step 2: Load media metadata
        print("\n🖼️  Loading media metadata...")
        self.load_media_metadata()
        
        # Step 3: Process and convert articles
        print("\n✨ Converting articles to MDX...")
        self.convert_articles()
        
        # Step 4: Download and organize media files
        print("\n📥 Downloading media files...")
        self.download_media_files()
        
        # Step 5: Generate navigation configuration
        print("\n🗺️  Generating navigation structure...")
        self.generate_navigation_config()
        
        # Step 6: Create index page
        print("\n📝 Creating index page...")
        self.create_index_page()
        
        # Step 7: Validation
        print("\n✅ Validating migration...")
        self.validate_migration()
        
        # Print summary
        self.print_summary()

    def load_articles(self):
        """Load all articles from S3"""
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=self.bucket,
            Prefix=f"{self.prefix}articles/"
        )
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                
                # Process metadata.json files
                if key.endswith('metadata.json'):
                    article_id = key.split('/')[-2]
                    
                    try:
                        # Download metadata
                        response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                        metadata = json.loads(response['Body'].read())
                        
                        # Download content
                        content_key = key.replace('metadata.json', 'content.md')
                        try:
                            content_response = self.s3_client.get_object(
                                Bucket=self.bucket, 
                                Key=content_key
                            )
                            content = content_response['Body'].read().decode('utf-8')
                        except ClientError:
                            content = ""
                            self.migration_stats['warnings'].append(
                                f"No content.md found for {article_id}"
                            )
                        
                        # Store article data
                        self.articles[article_id] = {
                            'metadata': metadata,
                            'content': content,
                            'article_id': article_id
                        }
                        
                        print(f"  ✓ Loaded: {metadata.get('title', article_id)}")
                        
                    except Exception as e:
                        self.migration_stats['errors'].append(
                            f"Error loading article {article_id}: {str(e)}"
                        )
                        print(f"  ✗ Error loading {article_id}: {str(e)}")

    def load_media_metadata(self):
        """Load media metadata from S3"""
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=self.bucket,
            Prefix=f"{self.prefix}media/"
        )
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                
                if key.endswith('metadata.json'):
                    media_id = key.split('/')[-2]
                    
                    try:
                        response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                        metadata = json.loads(response['Body'].read())
                        
                        # Find actual media file
                        media_pattern = re.match(r'(.+)/([^/]+)/metadata\.json$', key)
                        if media_pattern:
                            base_path = media_pattern.group(1)
                            
                            # Construct media file path
                            # Try to find the actual file
                            self.media_map[media_id] = {
                                'metadata': metadata,
                                's3_prefix': base_path,
                                'media_id': media_id
                            }
                            
                    except Exception as e:
                        print(f"  Warning: Could not load media metadata for {media_id}: {e}")

    def convert_articles(self):
        """Convert all articles to MDX format"""
        for article_id, article_data in self.articles.items():
            try:
                file_path, mdx_content = self.transform_article_to_mdx(
                    article_id,
                    article_data['metadata'],
                    article_data['content']
                )
                
                # Save MDX file
                full_path = self.output_dir / file_path
                full_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(full_path, 'w', encoding='utf-8') as f:
                    f.write(mdx_content)
                
                # Store file path for navigation
                article_data['file_path'] = file_path
                
                self.migration_stats['articles_processed'] += 1
                print(f"  ✓ Converted: {article_data['metadata'].get('title', article_id)}")
                
            except Exception as e:
                self.migration_stats['errors'].append(
                    f"Error converting article {article_id}: {str(e)}"
                )
                print(f"  ✗ Error converting {article_id}: {str(e)}")

    def transform_article_to_mdx(self, article_id: str, metadata: Dict, content: str) -> Tuple[str, str]:
        """Transform S3 article to Mintlify MDX format"""
        
        # Parse existing frontmatter if present
        frontmatter = {}
        content_body = content
        
        if content.startswith('---'):
            parts = content.split('---', 2)
            if len(parts) >= 3:
                try:
                    frontmatter = yaml.safe_load(parts[1])
                    content_body = parts[2].strip()
                except:
                    pass
        
        # Build Mintlify frontmatter (metadata.json is canonical)
        mintlify_frontmatter = {
            'title': metadata.get('title', frontmatter.get('title', 'Untitled')),
            'description': self._generate_description(metadata, frontmatter, content_body),
        }
        
        # Add optional fields
        if metadata.get('tags') and len(metadata['tags']) > 0:
            # Use first tag as sidebar tag
            mintlify_frontmatter['tag'] = metadata['tags'][0].upper()
        
        # Generate sidebarTitle if main title is long
        if len(mintlify_frontmatter['title']) > 30:
            mintlify_frontmatter['sidebarTitle'] = self._truncate_title(mintlify_frontmatter['title'])
        
        # Add icon based on category
        category = metadata.get('category', '').lower()
        icon_map = {
            'reports': 'chart-line',
            'settings': 'gear', 
            'tutorials': 'graduation-cap',
            'troubleshooting': 'wrench',
            'api': 'code',
            'getting-started': 'rocket',
            'general': 'book'
        }
        if category in icon_map:
            mintlify_frontmatter['icon'] = icon_map[category]
        
        # Convert content body
        mdx_content = self._convert_content_to_mdx(content_body, metadata)
        
        # Generate final MDX
        mdx_output = f"---\n{yaml.dump(mintlify_frontmatter, default_flow_style=False, allow_unicode=True)}---\n\n{mdx_content}"
        
        # Generate file path
        product = metadata.get('product', 'general').lower()
        category = self._sanitize_path(metadata.get('category', 'uncategorized'))
        section = metadata.get('section')
        
        if section:
            file_path = f"{product}/{category}/{self._sanitize_path(section)}/{self._generate_filename(metadata['title'])}.mdx"
        else:
            file_path = f"{product}/{category}/{self._generate_filename(metadata['title'])}.mdx"
        
        return file_path, mdx_output

    def _convert_content_to_mdx(self, content: str, metadata: Dict) -> str:
        """Convert markdown content to MDX with Mintlify components"""
        
        mdx = content
        
        # Convert media references
        mdx = self._convert_media_references(mdx, metadata)
        
        # Add Mintlify components
        mdx = self._enhance_with_components(mdx)
        
        # Add suggested queries section if present
        if metadata.get('suggested_queries'):
            mdx = self._add_suggested_queries(mdx, metadata['suggested_queries'])
        
        return mdx

    def _convert_media_references(self, content: str, metadata: Dict) -> str:
        """Convert kb://media/{id} references to local paths"""
        
        def replace_media(match):
            media_ref = match.group(1)
            media_id = media_ref.split('/')[-1] if '/' in media_ref else media_ref
            
            # Determine product for organizing images
            product = metadata.get('product', 'general').lower()
            
            # Generate local path
            # Assume .png extension if not specified
            if '.' not in media_id:
                media_id += '.png'
            
            return f"/images/{product}/{media_id}"
        
        # Replace kb:// references
        content = re.sub(r'kb://media/([^)]+)', replace_media, content)
        
        # Wrap standalone images in Frame components
        content = re.sub(
            r'^!\[([^\]]*)\]\(([^)]+)\)$',
            r'<Frame>\n  <img src="\2" alt="\1" />\n</Frame>',
            content,
            flags=re.MULTILINE
        )
        
        return content

    def _enhance_with_components(self, content: str) -> str:
        """Add Mintlify-specific MDX components"""
        
        # Convert note patterns to Note components
        content = re.sub(
            r'^> \*\*Note:\*\* (.+)$',
            r'<Note>\n  \1\n</Note>',
            content,
            flags=re.MULTILINE
        )
        
        # Convert warning patterns
        content = re.sub(
            r'^> \*\*Warning:\*\* (.+)$', 
            r'<Warning>\n  \1\n</Warning>',
            content,
            flags=re.MULTILINE
        )
        
        # Convert tip patterns
        content = re.sub(
            r'^> \*\*Tip:\*\* (.+)$',
            r'<Tip>\n  \1\n</Tip>',
            content,
            flags=re.MULTILINE
        )
        
        # Convert numbered steps to Steps component (if 3+ steps)
        content = self._convert_to_steps(content)
        
        return content

    def _convert_to_steps(self, content: str) -> str:
        """Convert numbered lists to Steps components"""
        
        # Find numbered list blocks
        pattern = r'((?:^\d+\. .+$\n?)+)'
        
        def replace_steps(match):
            steps_text = match.group(1)
            steps = re.findall(r'^\d+\. (.+)$', steps_text, re.MULTILINE)
            
            if len(steps) < 3:  # Only convert if 3+ steps
                return match.group(0)
            
            mdx_steps = ['<Steps>']
            for step in steps:
                # Simple conversion - could be enhanced
                mdx_steps.append(f'  <Step title="{step.strip()}">')
                mdx_steps.append('  </Step>')
            mdx_steps.append('</Steps>\n')
            
            return '\n'.join(mdx_steps)
        
        return re.sub(pattern, replace_steps, content, flags=re.MULTILINE)

    def _add_suggested_queries(self, content: str, queries: List[str]) -> str:
        """Add suggested queries as FAQ section"""
        
        if not queries:
            return content
        
        faq_section = "\n\n## Frequently Asked Questions\n\n<AccordionGroup>\n"
        
        for query in queries:
            # Generate a simple answer prompt
            faq_section += f'  <Accordion title="{query}">\n'
            faq_section += f'    This page provides information to help answer this question.\n'
            faq_section += '  </Accordion>\n'
        
        faq_section += '</AccordionGroup>'
        
        return content + faq_section

    def download_media_files(self):
        """Download media files from S3"""
        
        # Get list of all media files referenced in articles
        referenced_media = set()
        for article in self.articles.values():
            media_ids = article['metadata'].get('media_ids', [])
            referenced_media.update(media_ids)
        
        # Download referenced media
        for media_id in referenced_media:
            try:
                # Try to find the media file in S3
                # First check YYYY/MM structure
                found = False
                
                # List potential locations
                search_prefixes = [
                    f"{self.prefix}media/",
                ]
                
                for search_prefix in search_prefixes:
                    paginator = self.s3_client.get_paginator('list_objects_v2')
                    pages = paginator.paginate(
                        Bucket=self.bucket,
                        Prefix=search_prefix
                    )
                    
                    for page in pages:
                        if 'Contents' not in page:
                            continue
                        
                        for obj in page['Contents']:
                            key = obj['Key']
                            
                            # Check if this is our media file
                            if media_id in key and not key.endswith('metadata.json'):
                                # Determine product folder
                                product = 'general'
                                for article in self.articles.values():
                                    if media_id in article['metadata'].get('media_ids', []):
                                        product = article['metadata'].get('product', 'general').lower()
                                        break
                                
                                # Download file
                                filename = key.split('/')[-1]
                                local_path = self.output_dir / 'images' / product / filename
                                local_path.parent.mkdir(parents=True, exist_ok=True)
                                
                                self.s3_client.download_file(
                                    self.bucket,
                                    key, 
                                    str(local_path)
                                )
                                
                                self.migration_stats['media_processed'] += 1
                                print(f"  ✓ Downloaded: {filename}")
                                found = True
                                break
                        
                        if found:
                            break
                    
                    if found:
                        break
                
                if not found:
                    self.migration_stats['warnings'].append(f"Media not found: {media_id}")
                    
            except Exception as e:
                self.migration_stats['errors'].append(f"Error downloading media {media_id}: {str(e)}")

    def generate_navigation_config(self):
        """Generate docs.json configuration file"""
        
        config = {
            "$schema": "https://mintlify.com/schema.json",
            "name": "Documentation",
            "logo": {
                "dark": "/logo/dark.svg",
                "light": "/logo/light.svg"
            },
            "favicon": "/favicon.svg",
            "colors": {
                "primary": "#0D9373",
                "light": "#07C983",
                "dark": "#0D9373",
                "anchors": {
                    "from": "#0D9373",
                    "to": "#07C983"
                }
            },
            "topbarLinks": [
                {
                    "name": "Support",
                    "url": "mailto:support@example.com"
                }
            ],
            "topbarCtaButton": {
                "name": "Dashboard",
                "url": "https://dashboard.example.com"
            },
            "tabs": [
                {
                    "name": "Radix",
                    "url": "radix"
                },
                {
                    "name": "Rediq",
                    "url": "rediq"
                }
            ],
            "navigation": []
        }
        
        # Build navigation for each product
        for product in ['radix', 'rediq']:
            product_groups = []
            
            # Group articles by category
            categories = {}
            for article_id, article_data in self.articles.items():
                if article_data['metadata'].get('product', '').lower() != product:
                    continue
                
                if 'file_path' not in article_data:
                    continue
                
                category = article_data['metadata'].get('category', 'uncategorized')
                
                if category not in categories:
                    categories[category] = []
                
                # Remove .mdx extension for navigation
                page_path = article_data['file_path'].replace('.mdx', '')
                categories[category].append({
                    'path': page_path,
                    'title': article_data['metadata'].get('title', 'Untitled'),
                    'section': article_data['metadata'].get('section')
                })
            
            # Create groups for each category
            for category in sorted(categories.keys()):
                pages = categories[category]
                
                # Sort pages by title
                pages.sort(key=lambda x: x['title'])
                
                # Extract just the paths
                page_paths = [p['path'] for p in pages]
                
                if page_paths:
                    group = {
                        "group": self._format_group_name(category),
                        "pages": page_paths
                    }
                    product_groups.append(group)
            
            # Add to navigation if there are groups
            if product_groups:
                config['navigation'].extend(product_groups)
        
        # Add introduction at the beginning
        config['navigation'].insert(0, {
            "group": "Getting Started",
            "pages": ["index"]
        })
        
        # Save docs.json
        docs_json_path = self.output_dir / 'docs.json'
        with open(docs_json_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        print(f"  ✓ Generated docs.json with {len(config['navigation'])} groups")

    def create_index_page(self):
        """Create the main index.mdx page"""
        
        index_content = """---
title: Documentation
description: Welcome to our comprehensive documentation
---

## Welcome to Our Documentation

<CardGroup cols={2}>
  <Card title="Radix Documentation" icon="chart-line" href="/radix">
    Explore our Radix platform documentation and guides
  </Card>
  <Card title="Rediq Documentation" icon="database" href="/rediq">
    Learn about Rediq features and capabilities
  </Card>
</CardGroup>

## Quick Links

<CardGroup cols={3}>
  <Card title="Getting Started" icon="rocket" href="/getting-started">
    New to our platform? Start here
  </Card>
  <Card title="API Reference" icon="code" href="/api-reference">
    Explore our API documentation
  </Card>
  <Card title="Support" icon="life-ring" href="mailto:support@example.com">
    Need help? Contact our support team
  </Card>
</CardGroup>

## Popular Topics

Browse our most frequently accessed documentation:

- [Creating Reports](/radix/reports/create-portfolio-report)
- [Configuration Settings](/radix/settings/general-settings)
- [Troubleshooting Guide](/troubleshooting/common-issues)
"""
        
        index_path = self.output_dir / 'index.mdx'
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write(index_content)
        
        print("  ✓ Created index.mdx")

    def validate_migration(self):
        """Validate the migrated content"""
        
        # Check all MDX files
        mdx_files = list(self.output_dir.rglob('*.mdx'))
        
        for mdx_file in mdx_files:
            try:
                with open(mdx_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Check for frontmatter
                if not content.startswith('---'):
                    self.migration_stats['warnings'].append(f"Missing frontmatter: {mdx_file}")
                    continue
                
                # Parse frontmatter
                parts = content.split('---', 2)
                if len(parts) >= 3:
                    fm = yaml.safe_load(parts[1])
                    
                    # Check required fields
                    if 'title' not in fm:
                        self.migration_stats['errors'].append(f"Missing title: {mdx_file}")
                    if 'description' not in fm:
                        self.migration_stats['warnings'].append(f"Missing description: {mdx_file}")
                
            except Exception as e:
                self.migration_stats['errors'].append(f"Validation error in {mdx_file}: {str(e)}")

    def print_summary(self):
        """Print migration summary"""
        
        print("\n" + "="*60)
        print("📊 MIGRATION SUMMARY")
        print("="*60)
        
        print(f"\n✅ Articles processed: {self.migration_stats['articles_processed']}")
        print(f"🖼️  Media files downloaded: {self.migration_stats['media_processed']}")
        
        if self.migration_stats['warnings']:
            print(f"\n⚠️  Warnings ({len(self.migration_stats['warnings'])}): ")
            for warning in self.migration_stats['warnings'][:5]:
                print(f"   - {warning}")
            if len(self.migration_stats['warnings']) > 5:
                print(f"   ... and {len(self.migration_stats['warnings']) - 5} more")
        
        if self.migration_stats['errors']:
            print(f"\n❌ Errors ({len(self.migration_stats['errors'])}): ")
            for error in self.migration_stats['errors'][:5]:
                print(f"   - {error}")
            if len(self.migration_stats['errors']) > 5:
                print(f"   ... and {len(self.migration_stats['errors']) - 5} more")
        
        print(f"\n📁 Output directory: {self.output_dir}")
        print("\n✨ Migration complete! Next steps:")
        print("   1. Review the generated content")
        print("   2. Run 'mint dev' to preview locally")
        print("   3. Commit and push to GitHub")

    # Helper methods
    def _generate_description(self, metadata: Dict, frontmatter: Dict, content: str) -> str:
        """Generate description from metadata or content"""
        
        # Check frontmatter first
        if 'description' in frontmatter:
            return frontmatter['description']
        
        # Try to extract from content
        if content:
            # Remove markdown formatting
            clean_content = re.sub(r'[#*_`\[\]<!->]', '', content)
            # Get first meaningful paragraph
            paragraphs = clean_content.split('\n\n')
            for p in paragraphs:
                p = p.strip()
                if p and len(p) > 20:
                    # Truncate to 160 chars
                    return p[:157] + '...' if len(p) > 160 else p
        
        # Default description
        return f"Learn about {metadata.get('title', 'this topic')}"

    def _truncate_title(self, title: str) -> str:
        """Create shorter sidebar title"""
        
        if len(title) <= 25:
            return title
        
        # Try to truncate at word boundary
        words = title.split()
        short_title = ""
        for word in words:
            if len(short_title + word) < 22:
                short_title += word + " "
            else:
                break
        
        return short_title.strip() + "..."

    def _generate_filename(self, title: str) -> str:
        """Generate URL-safe filename from title"""
        
        # Remove special characters and convert to lowercase
        filename = re.sub(r'[^\w\s-]', '', title.lower())
        # Replace spaces with hyphens
        filename = re.sub(r'[-\s]+', '-', filename)
        # Remove leading/trailing hyphens
        return filename.strip('-')[:50]  # Limit length

    def _sanitize_path(self, path: str) -> str:
        """Sanitize path component"""
        
        if not path:
            return 'general'
        
        # Convert to lowercase and replace special chars
        sanitized = re.sub(r'[^\w-]', '-', path.lower())
        # Remove multiple hyphens
        sanitized = re.sub(r'-+', '-', sanitized)
        return sanitized.strip('-')

    def _format_group_name(self, name: str) -> str:
        """Format category name for display"""
        
        # Replace hyphens and underscores with spaces
        formatted = name.replace('-', ' ').replace('_', ' ')
        # Title case
        return formatted.title()


def main():
    """Main execution function"""
    
    parser = argparse.ArgumentParser(description='Migrate S3 docs to Mintlify format')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--prefix', default='kb/', help='S3 prefix (default: kb/)')
    parser.add_argument('--output', default='./mintlify-docs', help='Output directory')
    parser.add_argument('--profile', help='AWS profile name')
    
    args = parser.parse_args()
    
    # Run migration
    migrator = MintlifyMigrator(
        bucket=args.bucket,
        prefix=args.prefix,
        output_dir=args.output,
        aws_profile=args.profile
    )
    
    migrator.run_migration()


if __name__ == "__main__":
    main()