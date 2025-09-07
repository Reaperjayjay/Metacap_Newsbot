#!/usr/bin/env python3
"""
Professional News Aggregator
============================

A robust news aggregation script that fetches articles from multiple APIs
(GNews, MediaStack, Currents) and pushes them to a Notion database.

Features:
- Multi-API news fetching with error resilience
- Duplicate prevention based on headlines
- Comprehensive error handling and logging
- UTC timestamp management
- Modular architecture for easy extension

Author: Professional Python Engineer
Version: 2.0.0
Python: 3.11+
"""

import requests
from notion_client import Client
from datetime import datetime, timezone
import time
import logging
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse
import sys

# ===============================================
# CONFIGURATION & CONSTANTS
# ===============================================

# API Configuration - Get from environment variables (secrets)
import os

NOTION_TOKEN = os.getenv('NOTION_TOKEN', "ntn_v50193920684b9M32Pr8lVSz4BH97YIePN01WdXO0TS39A")
DATABASE_ID = os.getenv('DATABASE_ID', "26612b2dd84480c9ae0cd716d42c4944")  
GNEWS_API_KEY = os.getenv('GNEWS_API_KEY', "e6ab131a38240a286eeb29bd2704d7cc")
MEDIASTACK_API_KEY = os.getenv('MEDIASTACK_API_KEY', "611bb902ac4ab791ac8e2e504b5586f2")
CURRENTS_API_KEY = os.getenv('CURRENTS_API_KEY', "c4TwqKy9N7Yx6iMg-orThtFtqPSz6U6pX3ttnsVVVmf4Szg4")

# Request Configuration
REQUEST_TIMEOUT = 30  # seconds
RATE_LIMIT_DELAY = 0.1  # seconds between Notion API calls
MAX_ARTICLES_PER_API = 50  # limit per API to avoid overwhelming

# Notion Property Names
NOTION_PROPERTIES = {
    'headline': 'Headline',
    'source': 'Source',
    'url': 'URL',
    'category': 'Category',
    'published_at': 'Published At',
    'added_at': 'Added At'
}

# ===============================================
# LOGGING SETUP
# ===============================================

def setup_logging() -> logging.Logger:
    """Configure logging with proper formatting."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)
    return logger

# ===============================================
# DATA MODELS
# ===============================================

@dataclass
class NewsArticle:
    """Data class for standardized news article representation."""
    title: str
    source: str
    url: str
    category: str = "General"
    published_at: Optional[str] = None
    
    def __post_init__(self):
        """Validate and normalize article data."""
        self.title = self.title.strip() if self.title else "No Title"
        self.source = self.source.strip() if self.source else "Unknown"
        self.url = self.url.strip() if self.url else ""
        self.category = self.category.strip() if self.category else "General"
        
        # Ensure published_at is in proper ISO format
        if not self.published_at:
            self.published_at = datetime.now(timezone.utc).isoformat()
    
    @property
    def is_valid(self) -> bool:
        """Check if article has minimum required data."""
        return (
            self.title and 
            self.title != "No Title" and
            self.url and
            self._is_valid_url(self.url)
        )
    
    def _is_valid_url(self, url: str) -> bool:
        """Validate URL format."""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False

# ===============================================
# NOTION DATABASE MANAGER
# ===============================================

class NotionManager:
    """Handles all Notion database operations."""
    
    def __init__(self, token: str, database_id: str, logger: logging.Logger):
        self.client = Client(auth=token)
        self.database_id = database_id
        self.logger = logger
        self._existing_titles: Optional[Set[str]] = None
    
    def ensure_database_properties(self) -> bool:
        """Ensure all required properties exist in the Notion database."""
        try:
            # FIXED: Changed Source and Category to select properties with default options
            required_properties = {
                NOTION_PROPERTIES['source']: {
                    "select": {
                        "options": [
                            {"name": "GNews", "color": "blue"},
                            {"name": "MediaStack", "color": "green"}, 
                            {"name": "Currents", "color": "orange"},
                            {"name": "Manual", "color": "gray"},
                            {"name": "Unknown", "color": "default"}
                        ]
                    }
                },
                NOTION_PROPERTIES['url']: {"url": {}},
                NOTION_PROPERTIES['category']: {
                    "select": {
                        "options": [
                            {"name": "General", "color": "default"},
                            {"name": "Sports", "color": "green"},
                            {"name": "Politics", "color": "red"},
                            {"name": "Business", "color": "blue"},
                            {"name": "Technology", "color": "purple"},
                            {"name": "Entertainment", "color": "pink"},
                            {"name": "Health", "color": "yellow"}
                        ]
                    }
                },
                NOTION_PROPERTIES['published_at']: {"date": {}},
                NOTION_PROPERTIES['added_at']: {"date": {}}
            }
            
            database = self.client.databases.retrieve(self.database_id)
            current_properties = database["properties"]
            
            missing_properties = {}
            for prop_name, prop_schema in required_properties.items():
                if prop_name not in current_properties:
                    missing_properties[prop_name] = prop_schema
            
            if missing_properties:
                self.client.databases.update(
                    self.database_id,
                    properties=missing_properties
                )
                self.logger.info(f"✅ Added missing properties: {', '.join(missing_properties.keys())}")
            else:
                self.logger.info("✅ All required properties already exist")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to ensure database properties: {e}")
            return False
    
    def get_existing_headlines(self) -> Set[str]:
        """Fetch all existing headlines from Notion to prevent duplicates."""
        if self._existing_titles is not None:
            return self._existing_titles
        
        existing_titles = set()
        
        try:
            has_more = True
            start_cursor = None
            
            while has_more:
                query_params = {"database_id": self.database_id}
                if start_cursor:
                    query_params["start_cursor"] = start_cursor
                
                response = self.client.databases.query(**query_params)
                
                for page in response.get("results", []):
                    title_data = page.get("properties", {}).get(
                        NOTION_PROPERTIES['headline'], {}
                    ).get("title", [])
                    
                    if title_data:
                        title = title_data[0]["text"]["content"].strip()
                        if title:
                            existing_titles.add(title)
                
                has_more = response.get("has_more", False)
                start_cursor = response.get("next_cursor")
            
            self._existing_titles = existing_titles
            self.logger.info(f"📊 Found {len(existing_titles)} existing headlines")
            return existing_titles
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching existing headlines: {e}")
            return set()  # Return empty set to avoid blocking new articles
    
    def add_articles(self, articles: List[NewsArticle]) -> Tuple[int, int, int]:
        """
        Add articles to Notion database.
        
        Returns:
            Tuple of (added_count, skipped_count, error_count)
        """
        if not articles:
            self.logger.warning("⚠️ No articles to add")
            return 0, 0, 0
        
        existing_headlines = self.get_existing_headlines()
        added_count = skipped_count = error_count = 0
        
        for article in articles:
            try:
                # Skip invalid articles
                if not article.is_valid:
                    self.logger.warning(f"⏩ Skipped invalid article: {article.title}")
                    skipped_count += 1
                    continue
                
                # Skip duplicates
                if article.title in existing_headlines:
                    self.logger.info(f"⏩ Skipped duplicate: {article.title}")
                    skipped_count += 1
                    continue
                
                # Add to Notion
                self._create_notion_page(article)
                self.logger.info(f"📰 Added: {article.title}")
                added_count += 1
                
                # Update local cache to prevent duplicates in same batch
                existing_headlines.add(article.title)
                
                # Rate limiting
                time.sleep(RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.logger.error(f"❌ Error adding article '{article.title}': {e}")
                error_count += 1
                continue
        
        return added_count, skipped_count, error_count
    
    def _create_notion_page(self, article: NewsArticle) -> None:
        """Create a new page in Notion database."""
        current_time = datetime.now(timezone.utc).isoformat()
        
        # SAFETY: Ensure source and category exist in select options, fallback if not
        safe_source = self._validate_select_option(article.source, "source")
        safe_category = self._validate_select_option(article.category, "category")
        
        properties = {
            NOTION_PROPERTIES['headline']: {
                "title": [{"text": {"content": article.title}}]
            },
            NOTION_PROPERTIES['source']: {
                "select": {"name": safe_source}
            },
            NOTION_PROPERTIES['url']: {
                "url": article.url
            },
            NOTION_PROPERTIES['category']: {
                "select": {"name": safe_category}
            },
            NOTION_PROPERTIES['published_at']: {
                "date": {"start": article.published_at}
            },
            NOTION_PROPERTIES['added_at']: {
                "date": {"start": current_time}
            }
        }
        
        self.client.pages.create(
            parent={"database_id": self.database_id},
            properties=properties
        )
    
    def _validate_select_option(self, value: str, field_type: str) -> str:
        """Validate that select option exists, return safe fallback if not."""
        # Define valid options for each field
        valid_sources = {"GNews", "MediaStack", "Currents", "Manual", "Unknown"}
        valid_categories = {"General", "Sports", "Politics", "Business", "Technology", "Entertainment", "Health"}
        
        if field_type == "source":
            return value if value in valid_sources else "Unknown"
        elif field_type == "category":
            return value if value in valid_categories else "General"
        else:
            return value

# ===============================================
# NEWS API CLIENTS
# ===============================================

class NewsAPIClient:
    """Base class for news API clients."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.session = requests.Session()
        self.session.timeout = REQUEST_TIMEOUT
    
    def _make_request(self, url: str, api_name: str) -> Optional[Dict]:
        """Make HTTP request with error handling."""
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            self.logger.error(f"❌ {api_name}: Request timeout")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ {api_name}: Network error - {e}")
        except Exception as e:
            self.logger.error(f"❌ {api_name}: Unexpected error - {e}")
        return None

class GNewsClient(NewsAPIClient):
    """Client for GNews API.""" 
    
    def __init__(self, api_key: str, logger: logging.Logger):
        super().__init__(logger)
        self.api_key = api_key
    
    def fetch_articles(self) -> List[NewsArticle]:
        """Fetch articles from GNews API."""
        # FIXED: Using correct GNews endpoint instead of NewsAPI
        url = f"https://gnews.io/api/v4/top-headlines?country=ng&lang=en&token={self.api_key}&max={MAX_ARTICLES_PER_API}"
        
        self.logger.info("📡 Fetching from GNews...")
        
        data = self._make_request(url, "GNews")
        if not data or "articles" not in data:
            self.logger.warning("⚠️ GNews: No articles in response")
            return []
        
        articles = []
        for item in data.get("articles", []):
            try:
                article = NewsArticle(
                    title=item.get("title", "No Title"),
                    source="GNews",  # FIXED: Use consistent source name that matches select options
                    url=item.get("url", ""),
                    category="General",  # GNews doesn't provide categories
                    published_at=item.get("publishedAt")
                )
                articles.append(article)
            except Exception as e:
                self.logger.warning(f"⚠️ GNews: Error parsing article - {e}")
                continue
        
        self.logger.info(f"✅ GNews: Fetched {len(articles)} articles")
        return articles
    
    def _extract_source(self, source_data) -> str:
        """Extract source name from GNews source data."""
        if isinstance(source_data, dict):
            return source_data.get("name", "Unknown")
        return str(source_data) if source_data else "Unknown"

class MediaStackClient(NewsAPIClient):
    """Client for MediaStack API."""
    
    def __init__(self, api_key: str, logger: logging.Logger):
        super().__init__(logger)
        self.api_key = api_key
    
    def fetch_articles(self) -> List[NewsArticle]:
        """Fetch articles from MediaStack API."""
        url = (f"http://api.mediastack.com/v1/news?"
               f"access_key={self.api_key}&countries=ng&languages=en"
               f"&limit={MAX_ARTICLES_PER_API}")
        
        self.logger.info("📡 Fetching from MediaStack...")
        
        data = self._make_request(url, "MediaStack")
        if not data or "data" not in data:
            self.logger.warning("⚠️ MediaStack: No articles in response")
            return []
        
        articles = []
        for item in data.get("data", []):
            try:
                # FIXED: Map category to valid select options and use consistent source name
                category = self._map_category(item.get("category", "General"))
                
                article = NewsArticle(
                    title=item.get("title", "No Title"),
                    source="MediaStack",  # FIXED: Use consistent source name
                    url=item.get("url", ""),
                    category=category,
                    published_at=item.get("published_at")
                )
                articles.append(article)
            except Exception as e:
                self.logger.warning(f"⚠️ MediaStack: Error parsing article - {e}")
                continue
        
        self.logger.info(f"✅ MediaStack: Fetched {len(articles)} articles")
        return articles
    
    def _map_category(self, category: str) -> str:
        """Map API category to Notion select options."""
        category_mapping = {
            "sports": "Sports",
            "politics": "Politics", 
            "business": "Business",
            "technology": "Technology",
            "tech": "Technology",
            "entertainment": "Entertainment",
            "health": "Health"
        }
        return category_mapping.get(category.lower(), "General")

class CurrentsClient(NewsAPIClient):
    """Client for Currents API."""
    
    def __init__(self, api_key: str, logger: logging.Logger):
        super().__init__(logger)
        self.api_key = api_key
    
    def fetch_articles(self) -> List[NewsArticle]:
        """Fetch articles from Currents API."""
        # IMPROVED: Using region instead of keywords for better Nigerian coverage
        url = (f"https://api.currentsapi.services/v1/latest-news?"
               f"apiKey={self.api_key}&language=en&region=ng")
        
        self.logger.info("📡 Fetching from Currents...")
        
        data = self._make_request(url, "Currents")
        if not data or "news" not in data:
            self.logger.warning("⚠️ Currents: No articles in response")
            return []
        
        articles = []
        for item in data.get("news", []):
            try:
                # FIXED: Map category to valid select options and use consistent source name
                category = self._map_category(item.get("category", "General"))
                
                article = NewsArticle(
                    title=item.get("title", "No Title"),
                    source="Currents",  # FIXED: Use consistent source name
                    url=item.get("url", ""),
                    category=category,
                    published_at=item.get("published")
                )
                articles.append(article)
            except Exception as e:
                self.logger.warning(f"⚠️ Currents: Error parsing article - {e}")
                continue
        
        self.logger.info(f"✅ Currents: Fetched {len(articles)} articles")
        return articles
    
    def _map_category(self, category: str) -> str:
        """Map API category to Notion select options."""
        if not category:
            return "General"
        
        category_mapping = {
            "sports": "Sports",
            "politics": "Politics", 
            "business": "Business",
            "technology": "Technology",
            "tech": "Technology",
            "entertainment": "Entertainment",
            "health": "Health"
        }
        return category_mapping.get(category.lower(), "General")

# ===============================================
# NEWS AGGREGATOR
# ===============================================

class NewsAggregator:
    """Main orchestrator for news aggregation process."""
    
    def __init__(self):
        self.logger = setup_logging()
        self.notion_manager = NotionManager(NOTION_TOKEN, DATABASE_ID, self.logger)
        
        # Initialize API clients
        self.api_clients = [
            GNewsClient(GNEWS_API_KEY, self.logger),
            MediaStackClient(MEDIASTACK_API_KEY, self.logger),
            CurrentsClient(CURRENTS_API_KEY, self.logger)
        ]
    
    def run(self) -> None:
        """Execute the complete news aggregation workflow."""
        self.logger.info("🚀 Starting Professional News Aggregator...")
        
        # Setup Notion database
        if not self.notion_manager.ensure_database_properties():
            self.logger.error("❌ Failed to setup Notion database. Exiting.")
            sys.exit(1)
        
        # Fetch articles from all APIs
        all_articles = self._fetch_all_articles()
        
        if not all_articles:
            self.logger.warning("⚠️ No articles were fetched from any API")
            return
        
        self.logger.info(f"📊 Total articles fetched: {len(all_articles)}")
        
        # Add articles to Notion
        added, skipped, errors = self.notion_manager.add_articles(all_articles)
        
        # Final summary
        self._print_summary(added, skipped, errors, len(all_articles))
        self.logger.info("🏁 News aggregation completed!")
    
    def _fetch_all_articles(self) -> List[NewsArticle]:
        """Fetch articles from all configured APIs."""
        all_articles = []
        
        for client in self.api_clients:
            try:
                articles = client.fetch_articles()
                all_articles.extend(articles)
            except Exception as e:
                client_name = client.__class__.__name__
                self.logger.error(f"❌ {client_name}: Critical error - {e}")
                continue
        
        return all_articles
    
    def _print_summary(self, added: int, skipped: int, errors: int, total: int) -> None:
        """Print final execution summary."""
        print("\n" + "="*60)
        print("📊 EXECUTION SUMMARY")
        print("="*60)
        print(f"📥 Total articles fetched: {total}")
        print(f"✅ Articles added to Notion: {added}")
        print(f"⏩ Articles skipped (duplicates/invalid): {skipped}")
        print(f"❌ Articles with errors: {errors}")
        print(f"📈 Success rate: {(added/(total or 1))*100:.1f}%")
        print("="*60)

# ===============================================
# MAIN EXECUTION
# ===============================================

def main():
    """Main entry point for the news aggregator."""
    try:
        aggregator = NewsAggregator()
        aggregator.run()
    except KeyboardInterrupt:
        print("\n❌ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Critical error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
