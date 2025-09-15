#!/usr/bin/env python3
"""
News Aggregator Tool
====================

A news aggregation script that fetches articles from multiple APIs
and pushes them to a Notion database with automatic cleanup.

Version: 2.1.0
Python: 3.11+
"""

import requests
from notion_client import Client
from datetime import datetime, timezone, timedelta
import time
import logging
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse
import sys
import os

# Configuration
NOTION_TOKEN = os.getenv('NOTION_TOKEN', "ntn_v50193920684b9M32Pr8lVSz4BH97YIePN01WdXO0TS39A")
DATABASE_ID = os.getenv('DATABASE_ID', "26612b2dd84480c9ae0cd716d42c4944")  
GNEWS_API_KEY = os.getenv('GNEWS_API_KEY', "e6ab131a38240a286eeb29bd2704d7cc")
MEDIASTACK_API_KEY = os.getenv('MEDIASTACK_API_KEY', "611bb902ac4ab791ac8e2e504b5586f2")
CURRENTS_API_KEY = os.getenv('CURRENTS_API_KEY', "c4TwqKy9N7Yx6iMg-orThtFtqPSz6U6pX3ttnsVVVmf4Szg4")

# Settings
REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 0.1
MAX_ARTICLES_PER_API = 50
ARTICLE_RETENTION_DAYS = 3
ENABLE_AUTO_DELETE = True

# Database field names
NOTION_PROPERTIES = {
    'headline': 'Headline',
    'source': 'Source',
    'url': 'URL',
    'category': 'Category',
    'published_at': 'Published At',
    'added_at': 'Added At'
}

def setup_logging() -> logging.Logger:
    """Setup basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)
    return logger

@dataclass
class NewsArticle:
    """Represents a news article."""
    title: str
    source: str
    url: str
    category: str = "General"
    published_at: Optional[str] = None
    
    def __post_init__(self):
        """Clean up article data."""
        self.title = self.title.strip() if self.title else "No Title"
        self.source = self.source.strip() if self.source else "Unknown"
        self.url = self.url.strip() if self.url else ""
        self.category = self.category.strip() if self.category else "General"
        
        if not self.published_at:
            self.published_at = datetime.now(timezone.utc).isoformat()
    
    @property
    def is_valid(self) -> bool:
        """Check if article has required data."""
        return (
            self.title and 
            self.title != "No Title" and
            self.url and
            self._is_valid_url(self.url)
        )
    
    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is properly formatted."""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False

class NotionManager:
    """Handles Notion database operations."""
    
    def __init__(self, token: str, database_id: str, logger: logging.Logger):
        self.client = Client(auth=token)
        self.database_id = database_id
        self.logger = logger
        self._existing_titles: Optional[Set[str]] = None
    
    def setup_database(self) -> bool:
        """Make sure database has all needed properties."""
        try:
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
                self.logger.info(f"Added missing properties: {', '.join(missing_properties.keys())}")
            else:
                self.logger.info("All required properties exist")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup database properties: {e}")
            return False
    
    def get_existing_headlines(self) -> Set[str]:
        """Get all existing headlines to avoid duplicates."""
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
            self.logger.info(f"Found {len(existing_titles)} existing headlines")
            return existing_titles
            
        except Exception as e:
            self.logger.error(f"Error fetching existing headlines: {e}")
            return set()
    
    def cleanup_old_articles(self, retention_days: int = ARTICLE_RETENTION_DAYS) -> int:
        """Remove articles older than specified days."""
        if not ENABLE_AUTO_DELETE:
            self.logger.info("Auto-delete is disabled")
            return 0
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        deleted_count = 0
        
        self.logger.info(f"Cleaning up articles older than {retention_days} days...")
        self.logger.info(f"Cutoff date: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        try:
            filter_condition = {
                "property": NOTION_PROPERTIES['added_at'],
                "date": {
                    "before": cutoff_date.isoformat()
                }
            }
            
            has_more = True
            start_cursor = None
            
            while has_more:
                query_params = {
                    "database_id": self.database_id,
                    "filter": filter_condition
                }
                if start_cursor:
                    query_params["start_cursor"] = start_cursor
                
                response = self.client.databases.query(**query_params)
                pages_to_delete = response.get("results", [])
                
                for page in pages_to_delete:
                    try:
                        page_id = page["id"]
                        
                        # Get article info for logging
                        title_data = page.get("properties", {}).get(
                            NOTION_PROPERTIES['headline'], {}
                        ).get("title", [])
                        title = "Unknown Title"
                        if title_data:
                            title = title_data[0]["text"]["content"][:50]
                        
                        added_date_data = page.get("properties", {}).get(
                            NOTION_PROPERTIES['added_at'], {}
                        ).get("date", {})
                        added_date = "Unknown Date"
                        if added_date_data and added_date_data.get("start"):
                            added_date = added_date_data["start"][:10]
                        
                        # Archive the page
                        self.client.pages.update(page_id=page_id, archived=True)
                        deleted_count += 1
                        
                        self.logger.info(f"Deleted: '{title}' (added: {added_date})")
                        time.sleep(RATE_LIMIT_DELAY)
                        
                    except Exception as e:
                        self.logger.error(f"Error deleting page {page.get('id', 'unknown')}: {e}")
                        continue
                
                has_more = response.get("has_more", False)
                start_cursor = response.get("next_cursor")
            
            self.logger.info(f"Cleanup completed. Deleted {deleted_count} old articles")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
            return deleted_count
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get database statistics."""
        try:
            total_articles = 0
            has_more = True
            start_cursor = None
            
            while has_more:
                query_params = {"database_id": self.database_id}
                if start_cursor:
                    query_params["start_cursor"] = start_cursor
                
                response = self.client.databases.query(**query_params)
                total_articles += len(response.get("results", []))
                
                has_more = response.get("has_more", False)
                start_cursor = response.get("next_cursor")
            
            # Count recent articles
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            recent_filter = {
                "property": NOTION_PROPERTIES['added_at'],
                "date": {
                    "after": yesterday.isoformat()
                }
            }
            
            recent_response = self.client.databases.query(
                database_id=self.database_id,
                filter=recent_filter
            )
            recent_articles = len(recent_response.get("results", []))
            
            return {
                "total_articles": total_articles,
                "recent_articles": recent_articles
            }
            
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {"total_articles": 0, "recent_articles": 0}
    
    def add_articles(self, articles: List[NewsArticle]) -> Tuple[int, int, int]:
        """Add articles to database."""
        if not articles:
            self.logger.warning("No articles to add")
            return 0, 0, 0
        
        existing_headlines = self.get_existing_headlines()
        added_count = skipped_count = error_count = 0
        
        for article in articles:
            try:
                # Skip invalid articles
                if not article.is_valid:
                    self.logger.warning(f"Skipped invalid article: {article.title}")
                    skipped_count += 1
                    continue
                
                # Skip duplicates
                if article.title in existing_headlines:
                    self.logger.info(f"Skipped duplicate: {article.title}")
                    skipped_count += 1
                    continue
                
                # Add to Notion
                self._create_page(article)
                self.logger.info(f"Added: {article.title}")
                added_count += 1
                
                existing_headlines.add(article.title)
                time.sleep(RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.logger.error(f"Error adding article '{article.title}': {e}")
                error_count += 1
                continue
        
        return added_count, skipped_count, error_count
    
    def _create_page(self, article: NewsArticle) -> None:
        """Create new page in database."""
        current_time = datetime.now(timezone.utc).isoformat()
        
        safe_source = self._validate_option(article.source, "source")
        safe_category = self._validate_option(article.category, "category")
        
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
    
    def _validate_option(self, value: str, field_type: str) -> str:
        """Make sure option exists in select field."""
        valid_sources = {"GNews", "MediaStack", "Currents", "Manual", "Unknown"}
        valid_categories = {"General", "Sports", "Politics", "Business", "Technology", "Entertainment", "Health"}
        
        if field_type == "source":
            return value if value in valid_sources else "Unknown"
        elif field_type == "category":
            return value if value in valid_categories else "General"
        else:
            return value

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
            self.logger.error(f"{api_name}: Request timeout")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"{api_name}: Network error - {e}")
        except Exception as e:
            self.logger.error(f"{api_name}: Unexpected error - {e}")
        return None

class GNewsClient(NewsAPIClient):
    """GNews API client.""" 
    
    def __init__(self, api_key: str, logger: logging.Logger):
        super().__init__(logger)
        self.api_key = api_key
    
    def fetch_articles(self) -> List[NewsArticle]:
        """Get articles from GNews."""
        url = f"https://gnews.io/api/v4/top-headlines?country=ng&lang=en&token={self.api_key}&max={MAX_ARTICLES_PER_API}"
        
        self.logger.info("Fetching from GNews...")
        
        data = self._make_request(url, "GNews")
        if not data or "articles" not in data:
            self.logger.warning("GNews: No articles in response")
            return []
        
        articles = []
        for item in data.get("articles", []):
            try:
                article = NewsArticle(
                    title=item.get("title", "No Title"),
                    source="GNews",
                    url=item.get("url", ""),
                    category="General",
                    published_at=item.get("publishedAt")
                )
                articles.append(article)
            except Exception as e:
                self.logger.warning(f"GNews: Error parsing article - {e}")
                continue
        
        self.logger.info(f"GNews: Fetched {len(articles)} articles")
        return articles

class MediaStackClient(NewsAPIClient):
    """MediaStack API client."""
    
    def __init__(self, api_key: str, logger: logging.Logger):
        super().__init__(logger)
        self.api_key = api_key
    
    def fetch_articles(self) -> List[NewsArticle]:
        """Get articles from MediaStack."""
        url = (f"http://api.mediastack.com/v1/news?"
               f"access_key={self.api_key}&countries=ng&languages=en"
               f"&limit={MAX_ARTICLES_PER_API}")
        
        self.logger.info("Fetching from MediaStack...")
        
        data = self._make_request(url, "MediaStack")
        if not data or "data" not in data:
            self.logger.warning("MediaStack: No articles in response")
            return []
        
        articles = []
        for item in data.get("data", []):
            try:
                category = self._map_category(item.get("category", "General"))
                
                article = NewsArticle(
                    title=item.get("title", "No Title"),
                    source="MediaStack",
                    url=item.get("url", ""),
                    category=category,
                    published_at=item.get("published_at")
                )
                articles.append(article)
            except Exception as e:
                self.logger.warning(f"MediaStack: Error parsing article - {e}")
                continue
        
        self.logger.info(f"MediaStack: Fetched {len(articles)} articles")
        return articles
    
    def _map_category(self, category: str) -> str:
        """Map category names."""
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
    """Currents API client."""
    
    def __init__(self, api_key: str, logger: logging.Logger):
        super().__init__(logger)
        self.api_key = api_key
    
    def fetch_articles(self) -> List[NewsArticle]:
        """Get articles from Currents."""
        url = (f"https://api.currentsapi.services/v1/latest-news?"
               f"apiKey={self.api_key}&language=en&region=ng")
        
        self.logger.info("Fetching from Currents...")
        
        data = self._make_request(url, "Currents")
        if not data or "news" not in data:
            self.logger.warning("Currents: No articles in response")
            return []
        
        articles = []
        for item in data.get("news", []):
            try:
                category = self._map_category(item.get("category", "General"))
                
                article = NewsArticle(
                    title=item.get("title", "No Title"),
                    source="Currents",
                    url=item.get("url", ""),
                    category=category,
                    published_at=item.get("published")
                )
                articles.append(article)
            except Exception as e:
                self.logger.warning(f"Currents: Error parsing article - {e}")
                continue
        
        self.logger.info(f"Currents: Fetched {len(articles)} articles")
        return articles
    
    def _map_category(self, category: str) -> str:
        """Map category names."""
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

class NewsAggregator:
    """Main news aggregation handler."""
    
    def __init__(self):
        self.logger = setup_logging()
        self.notion_manager = NotionManager(NOTION_TOKEN, DATABASE_ID, self.logger)
        
        self.api_clients = [
            GNewsClient(GNEWS_API_KEY, self.logger),
            MediaStackClient(MEDIASTACK_API_KEY, self.logger),
            CurrentsClient(CURRENTS_API_KEY, self.logger)
        ]
    
    def run(self) -> None:
        """Run the news aggregation process."""
        self.logger.info("Starting news aggregator...")
        
        # Setup database
        if not self.notion_manager.setup_database():
            self.logger.error("Failed to setup database. Exiting.")
            sys.exit(1)
        
        # Get initial stats
        initial_stats = self.notion_manager.get_database_stats()
        self.logger.info(f"Database contains {initial_stats['total_articles']} total articles")
        
        # Clean up old articles
        deleted_count = self.notion_manager.cleanup_old_articles()
        
        # Fetch new articles
        all_articles = self._fetch_articles()
        
        if not all_articles:
            self.logger.warning("No articles were fetched from any API")
            return
        
        self.logger.info(f"Total articles fetched: {len(all_articles)}")
        
        # Add articles to database
        added, skipped, errors = self.notion_manager.add_articles(all_articles)
        
        # Get final stats
        final_stats = self.notion_manager.get_database_stats()
        
        # Show summary
        self._print_summary(added, skipped, errors, len(all_articles), deleted_count, final_stats)
        self.logger.info("News aggregation completed!")
    
    def _fetch_articles(self) -> List[NewsArticle]:
        """Fetch articles from all APIs."""
        all_articles = []
        
        for client in self.api_clients:
            try:
                articles = client.fetch_articles()
                all_articles.extend(articles)
            except Exception as e:
                client_name = client.__class__.__name__
                self.logger.error(f"{client_name}: Critical error - {e}")
                continue
        
        return all_articles
    
    def _print_summary(self, added: int, skipped: int, errors: int, total: int, 
                      deleted: int, final_stats: Dict[str, int]) -> None:
        """Print execution summary."""
        print("\n" + "="*60)
        print("EXECUTION SUMMARY")
        print("="*60)
        print(f"Old articles deleted: {deleted}")
        print(f"Total articles fetched: {total}")
        print(f"Articles added to Notion: {added}")
        print(f"Articles skipped (duplicates/invalid): {skipped}")
        print(f"Articles with errors: {errors}")
        print(f"Success rate: {(added/(total or 1))*100:.1f}%")
        print("-"*60)
        print(f"Total articles in database: {final_stats['total_articles']}")
        print(f"Recent articles (24h): {final_stats['recent_articles']}")
        print(f"Retention period: {ARTICLE_RETENTION_DAYS} days")
        print(f"Auto-delete: {'Enabled' if ENABLE_AUTO_DELETE else 'Disabled'}")
        print("="*60)

def run_cleanup_only():
    """Run cleanup without fetching new articles."""
    logger = setup_logging()
    notion_manager = NotionManager(NOTION_TOKEN, DATABASE_ID, logger)
    
    logger.info("Running cleanup-only mode...")
    
    if not notion_manager.setup_database():
        logger.error("Failed to setup database. Exiting.")
        sys.exit(1)
    
    initial_stats = notion_manager.get_database_stats()
    logger.info(f"Database contains {initial_stats['total_articles']} total articles")
    
    deleted_count = notion_manager.cleanup_old_articles()
    final_stats = notion_manager.get_database_stats()
    
    print(f"\nCleanup completed!")
    print(f"Articles deleted: {deleted_count}")
    print(f"Articles remaining: {final_stats['total_articles']}")

def main():
    """Main entry point."""
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "--cleanup-only":
            run_cleanup_only()
        else:
            aggregator = NewsAggregator()
            aggregator.run()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
