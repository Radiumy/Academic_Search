import asyncio
import json
import logging
import os
from pathlib import Path
from typing import List, Optional, Dict
import re
from urllib.parse import urljoin, urlparse
from collections import defaultdict
import math

from crawl4ai import (
    AsyncWebCrawler, 
    BrowserConfig, 
    CrawlerRunConfig,
    CrawlerMonitor,
    DisplayMode,
    RateLimiter,
    CacheMode
)
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher
from flair.data import Sentence
from flair.models import SequenceTagger
import torch

class DFSCrawler:
    def __init__(
        self,
        cache_dir: str,
        max_concurrent: int = 10,
        base_delay: tuple = (2.0, 4.0),  # From documented RateLimiter
        max_delay: float = 30.0,
        max_retries: int = 3,
        verbose: bool = False,
        whitelist: List[str] = None,
        blacklist: List[str] = None,
        use_gpu: bool = False  # Option to use GPU for NER
    ):
        """Initialize the DFS Crawler with configuration parameters.
        
        Args:
            cache_dir: Directory to store cached results
            max_concurrent: Maximum number of concurrent crawls
            base_delay: Tuple of (min, max) delay between requests
            max_delay: Maximum backoff delay for rate limiting
            max_retries: Number of retry attempts on failure
            verbose: Enable verbose logging
            whitelist: List of regex patterns for allowed URLs. If None, all URLs are allowed
            blacklist: List of regex patterns for blocked URLs. Takes precedence over whitelist
            use_gpu: Option to use GPU for NER processing
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self.logger = logging.getLogger("DFSCrawler")
        self.logger.setLevel(logging.DEBUG if verbose else logging.INFO)
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # Store configuration
        self.config = {
            "max_concurrent": max_concurrent,
            "base_delay": base_delay,
            "max_delay": max_delay,
            "max_retries": max_retries,
            "verbose": verbose
        }
        
        self._setup_crawler_config()
        self.whitelist = [re.compile(pattern) for pattern in (whitelist or [])]
        self.blacklist = [re.compile(pattern) for pattern in (blacklist or [])]

        # Initialize NER model
        self.device = torch.device('cuda' if use_gpu and torch.cuda.is_available() else 'cpu')
        self.logger.info(f"Using device for NER: {self.device}")
        
        # Load NER model
        self.ner_model = SequenceTagger.load('flair/ner-english-fast')
        self.ner_model.to(self.device)

    def _setup_crawler_config(self):
        """Setup crawler configurations based on documented API."""
        self.browser_config = BrowserConfig(
            headless=True,
            viewport_width=1280,
            viewport_height=720,
            text_mode=True,  # Documented optimization for text-only crawls
            verbose=self.config["verbose"]
        )

        # RateLimiter with documented parameters
        self.rate_limiter = RateLimiter(
            base_delay=self.config["base_delay"],
            max_delay=self.config["max_delay"],
            max_retries=self.config["max_retries"],
            rate_limit_codes=[429, 503]
        )

        self.monitor = CrawlerMonitor(
            enable_ui=True,
            max_width=120
        )

        # Use documented dispatcher configuration
        self.dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=70.0,
            check_interval=1.0,
            max_session_permit=self.config["max_concurrent"],
            monitor=self.monitor,
            rate_limiter=self.rate_limiter
        )

        # Use documented CrawlerRunConfig parameters
        self.run_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,  # We handle caching ourselves
            stream=True,  # Enable streaming for arun_many()
            word_count_threshold=200,  # Documented default
            wait_for=None,  # No specific element to wait for
            screenshot=False,
            pdf=False
        )

    def _get_cache_path(self, url: str) -> Path:
        """Generate a cache file path for a given URL."""
        # Create a filename-safe version of the URL
        safe_name = "".join(c if c.isalnum() else "_" for c in url)
        return self.cache_dir / f"{safe_name}.json"

    def _load_cache(self, url: str) -> Optional[Dict]:
        """Load cached result for a URL if it exists."""
        cache_path = self._get_cache_path(url)
        if cache_path.exists():
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data:  # Check if cache is not empty
                        self.logger.debug(f"Cache hit for {url}")
                        return data
            except json.JSONDecodeError:
                self.logger.warning(f"Corrupted cache file for {url}")
            except Exception as e:
                self.logger.error(f"Error reading cache for {url}: {e}")
        return None

    def _save_cache(self, url: str, data: Dict):
        """Save crawl result to cache."""
        cache_path = self._get_cache_path(url)
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.debug(f"Cached result for {url}")
        except Exception as e:
            self.logger.error(f"Error caching result for {url}: {e}")

    def _extract_entities(self, text: str) -> Dict[str, List[Dict]]:
        """Extract named entities from text using Flair."""
        try:
            # Create a Flair sentence
            sentence = Sentence(text)
            
            # Run NER prediction
            self.ner_model.predict(sentence)
            
            # Organize entities by type
            entities = {}
            for entity in sentence.get_spans('ner'):
                entity_type = entity.tag
                entity_text = entity.text
                entity_score = entity.score
                
                if entity_type not in entities:
                    entities[entity_type] = []
                
                entities[entity_type].append({
                    'text': entity_text,
                    'score': float(entity_score),  # Convert to float for JSON serialization
                    'start': entity.start_position,
                    'end': entity.end_position
                })
            
            return entities
            
        except Exception as e:
            self.logger.error(f"Error in NER processing: {e}")
            return {}

    async def _crawl_urls(self, urls: List[str]) -> List[Dict]:
        """Internal async method to crawl URLs using documented API."""
        urls = list(set(urls))
        
        results = []
        urls_to_crawl = []

        
        # Check cache first
        for url in urls:
            cached_result = self._load_cache(url)
            if cached_result:
                results.append(cached_result)
            else:
                urls_to_crawl.append(url)

        if not urls_to_crawl:
            self.logger.info("All URLs found in cache")
            return results

        self.logger.info(f"Crawling {len(urls_to_crawl)} URLs...")
        
        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            async for result in await crawler.arun_many(
                urls=urls_to_crawl,
                config=self.run_config,
                dispatcher=self.dispatcher
            ):
                if result.success:
                    # Extract entities from markdown content
                    markdown_text = result.markdown_v2.raw_markdown if result.markdown_v2 else ""
                    entities = self._extract_entities(markdown_text)
                    
                    # Store results including entities
                    result_dict = {
                        "url": result.url,
                        "success": result.success,
                        "html": result.html,
                        "cleaned_html": result.cleaned_html,
                        "markdown": markdown_text,
                        "links": {
                            "internal": result.links.get("internal", []),
                            "external": result.links.get("external", [])
                        },
                        "status_code": result.status_code,
                        "error_message": result.error_message,
                        "entities": entities  # Add extracted entities
                    }
                    
                    self._save_cache(result.url, result_dict)
                    results.append(result_dict)
                    self.logger.info(f"Successfully crawled and processed entities: {result.url}")
                else:
                    self.logger.error(f"Failed to crawl {result.url}: {result.error_message}")

        return results

    def _is_url_allowed(self, url: str) -> bool:
        """Check if URL is allowed based on whitelist and blacklist patterns."""
        # Convert URL to lowercase for case-insensitive matching
        url_lower = url.lower()
        
        # Check blacklist first (takes precedence)
        for pattern in self.blacklist:
            # Convert string patterns to case-insensitive regex if they're not already regex
            if isinstance(pattern, str):
                if pattern.lower() in url_lower:
                    self.logger.debug(f"URL blocked by blacklist: {url}")
                    return False
            elif pattern.search(url_lower):  # For regex patterns
                self.logger.debug(f"URL blocked by blacklist: {url}")
                return False
        
        # If whitelist is empty, allow all non-blacklisted URLs
        if not self.whitelist:
            return True
        
        # Check whitelist
        for pattern in self.whitelist:
            if pattern.search(url):
                return True
        
        self.logger.debug(f"URL not in whitelist: {url}")
        return False

    def _extract_urls(self, html: str, base_url: str) -> List[str]:
        """Extract and normalize URLs from HTML content."""
        # Simple regex for URL extraction
        url_pattern = re.compile(r'href=[\'"]?([^\'" >]+)')
        urls = []
        
        for match in url_pattern.finditer(html):
            url = match.group(1)
            # Normalize URL
            full_url = urljoin(base_url, url)
            
            # Basic URL cleaning
            parsed = urlparse(full_url)
            if parsed.scheme in ('http', 'https') and self._is_url_allowed(full_url):
                urls.append(full_url)
        
        return list(set(urls))  # Remove duplicates

    async def _crawl_layer(self, urls: List[str], visited: set) -> tuple[List[Dict], List[str]]:
        """Crawl a layer of URLs and return results and next layer URLs."""
        results = []
        next_layer_urls = set()
        
        # Filter out already visited URLs
        urls_to_crawl = [url for url in urls if url not in visited]
        if not urls_to_crawl:
            return results, list(next_layer_urls)

        # Crawl current layer
        layer_results = await self._crawl_urls(urls_to_crawl)
        
        # Process results and extract next layer URLs
        for result in layer_results:
            results.append(result)
            visited.add(result['url'])
            
            if result['success']:
                # Extract URLs from both HTML and markdown content
                extracted_urls = self._extract_urls(result['html'], result['url'])
                next_layer_urls.update(extracted_urls)
        
        return results, list(next_layer_urls)

    def dfs(self, seed_urls: List[str], depth: int) -> List[Dict]:
        """
        Perform a depth-first (breadth-first) crawl starting from seed URLs.
        
        Args:
            seed_urls: Initial URLs to start crawling from
            depth: Maximum depth of crawling
            
        Returns:
            List of all crawled pages' results
        """
        self.logger.info(f"Starting DFS crawl from {len(seed_urls)} seed URLs with depth {depth}")
        
        async def _dfs_crawl():
            all_results = []
            visited = set()
            current_urls = seed_urls
            
            for current_depth in range(depth):
                self.logger.info(f"Crawling depth {current_depth + 1}/{depth}")
                
                # Crawl current layer
                results, next_urls = await self._crawl_layer(current_urls, visited)
                all_results.extend(results)
                
                # Filter and prepare next layer
                next_urls = [url for url in next_urls if url not in visited]
                if not next_urls:
                    self.logger.info(f"No more URLs to crawl at depth {current_depth + 1}")
                    break
                
                self.logger.info(f"Found {len(next_urls)} new URLs to crawl in next layer")
                current_urls = next_urls
            
            return all_results
        
        return asyncio.run(_dfs_crawl())

    def aggregate(self, output_path: str, min_count: int=1, max_freq: float = 0.5) -> Dict[str, Dict]:
        """
        Aggregate and deduplicate person entities across all crawled pages.
        
        Args:
            output_path: Path to save the aggregated entities JSON
            min_count: Minimum number of occurrences required
            max_freq: Maximum frequency threshold (percentage of pages)
            
        Returns:
            Dictionary of deduplicated person entities with absolute paths to their cache files
        """
        self.logger.info("Starting entity aggregation...")
        
        # Create output directory if it doesn't exist
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Common terms to filter out
        filter_terms = {
            'admin', 'dean', 'staff', 'faculty', 'student', 'professor', 'advisor',
            'director', 'chair', 'head', 'coordinator', 'assistant', 'associate',
            'manager', 'president', 'secretary', 'support', 'administrator'
        }
        
        # Regex for valid name characters (only English letters, dots, spaces, and hyphens)
        valid_name_pattern = re.compile(r'^[A-Za-z. -]+$')
        
        # Collect all cached files
        cached_files = list(self.cache_dir.glob('*.json'))
        total_pages = len(cached_files)
        
        if total_pages == 0:
            self.logger.warning("No cached pages found")
            return {}
        
        # First pass: count entity frequencies
        entity_counts = defaultdict(int)
        for cache_file in cached_files:
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data.get('success') and 'entities' in data:
                        persons = data['entities'].get('PER', [])
                        for person in persons:
                            name = person['text'].strip()
                            # Only count names that pass the character filter
                            if valid_name_pattern.match(name):
                                entity_counts[name] += 1
            except Exception as e:
                self.logger.error(f"Error reading cache file {cache_file}: {e}")
        
        # Calculate frequency thresholds
        max_count = math.ceil(total_pages * max_freq)
        
        # Second pass: aggregate filtered entities
        aggregated = {}
        for cache_file in cached_files:
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if not (data.get('success') and 'entities' in data):
                        continue
                    
                    persons = data['entities'].get('PER', [])
                    for person in persons:
                        name = person['text'].strip()
                        score = float(person['score'])
                        
                        # Skip if confidence is too low
                        if score < 0.3:
                            print(f"Skipping {name} because of low confidence {score:.2f}")
                            continue
                            
                        # Skip if name contains invalid characters
                        # if not valid_name_pattern.match(name):
                            # print(f"Skipping {name} because of invalid characters")
                            # continue
                        
                        # Skip if name contains filtered terms
                        if any(term.lower() in name.lower() for term in filter_terms):
                            print(f"Skipping {name} because it contains filtered term")
                            continue
                        
                        # Skip if name appears too rarely or too frequently
                        count = entity_counts[name]
                        if count < min_count or count > max_count:
                            print(f"Skipping {name} because of frequency {count}")
                            continue
                        
                        # Add to aggregated results
                        if name not in aggregated:
                            aggregated[name] = {
                                "cache_files": [],  # Store absolute paths to cache files
                                "frequency": count,
                                "confidence": []
                            }
                        
                        # Add absolute cache file path if not already included
                        cache_path = str(cache_file.absolute())  # Use absolute path
                        if cache_path not in aggregated[name]["cache_files"]:
                            aggregated[name]["cache_files"].append(cache_path)
                            aggregated[name]["confidence"].append(float(person['score']))
                            
            except Exception as e:
                self.logger.error(f"Error processing cache file {cache_file}: {e}")
        
        # Calculate average confidence for each entity
        for name in aggregated:
            scores = aggregated[name]["confidence"]
            aggregated[name]["avg_confidence"] = sum(scores) / len(scores)
            del aggregated[name]["confidence"]  # Remove individual scores
        
        # Sort by frequency and confidence
        sorted_aggregated = dict(sorted(
            aggregated.items(),
            key=lambda x: (-x[1]["frequency"], -x[1]["avg_confidence"])
        ))
        
        # Save aggregated results
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(sorted_aggregated, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Saved aggregated entities to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving aggregated results: {e}")
        
        return sorted_aggregated


def main():
    crawler = DFSCrawler(
        cache_dir="./cache_tmp",
        max_concurrent=32,
        base_delay=(2.0, 4.0),
        max_delay=30.0,
        max_retries=3,
        verbose=True,
        whitelist=None,
        blacklist=[
            # File extensions to skip
            r"\.(jpg|jpeg|png|gif|pdf|zip|ico|svg|webp|mp4|mp3|avi|mov|wmv|flv|webm|doc|docx|xls|xlsx|ppt|pptx|txt|csv|json|xml|js|css|asp|aspx|jsp|do|action|dll|exe|bat|sh|pl|cgi|rss|atom|xsl|wsdl|soap|wsf|wsc|wsm|wsa|wad|wax|wbx|wsc|wsf|wsg|wsh|wsn|wss|wst|wsu|wsx|wsy|wtv|wvx|wwe|wwp|wwz|xap|xpi|xpt|xul|zep|zip|tar|gz|bz2|rar|7z|iso|img|bin|dat|dmg|pkg|deb|rpm|msi|app|appx|appxbundle|appxupload|msix|msixbundle|msixupload|dmg|pkg|deb|rpm|msi|app|appx|appxbundle|appxupload|msix|msixbundle|msixupload)$",
            
            # Social media and external services
            "twitter", "x.com", "facebook", "instagram", "linkedin",
            "youtube", "tiktok", "reddit", "dropbox", "cdn",
            "cloudflare", "cloudfront",
            
            # Non-faculty content
            "alumni", "news", "events", "service", "support",
            "contact", "terms", "privacy", "award", "library",
            "hospital", "lectures", "login", "signup", "register",
            "forgot", "reset", "admin",
            
            # Additional filters for non-faculty content
            "undergraduate",
            "courses", "class", "curriculum", "syllabus",
            "workshop", "seminar", "conference", 
            "administrative", "technical", "it-support", "facilities",
            "department-life", "campus", "building", "room", "office",
            "admission", "apply", "fee", "tuition", "scholarship",
            "fellowship", "grant", "funding", "opportunity",
            "calendar", "schedule", "timetable", "deadline",
            "blog", "news-archive", "press", "media", "download",
            "resource", "tool", "software", "dataset", "repository",
            "gallery", "video", "audio", "podcast",
            
            # File types and technical URLs
            ".js", ".css", "xml", ".json", ".txt",
            "api", "feed", "sitemap", "robots", "wp-",
            "search", "filter", "sort", "tag", "category",
            "page", "index", "list", "archive", "browse",
            
            # Query parameters to skip
            r"[?&]utm_", r"[?&]source=", r"[?&]ref=", r"[?&]medium=",
            r"[?&]campaign=", r"[?&]term=", r"[?&]content=",
            r"[?&]page=", r"[?&]sort=", r"[?&]filter="
        ],
        use_gpu=True
    )
    
    def process_person(name: str):
        """Process search results for a single person."""
        search_cache_file = f"cache/searchxng/{name}.json"
        
        try:
            with open(search_cache_file, 'r', encoding='utf-8') as f:
                search_results = json.load(f)
            
            # Extract URLs from search results
            seed_urls = [result['url'] for result in search_results]
            print(f"Found {len(seed_urls)} URLs for {name}")
            
            # Crawl pages with depth=1 (just visit the search results)
            results = crawler.dfs(seed_urls, depth=1)
            print(f"Crawled {len(results)} pages for {name}")
            
            # Aggregate entities
            output_file = f"./data/people/{name}.json"
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            aggregated = crawler.aggregate(
                output_path=output_file,
                min_count=1,
                max_freq=0.5
            )
            print(f"Found {len(aggregated)} unique person entities for {name}")
            
            return len(results), len(aggregated)
            
        except FileNotFoundError:
            print(f"No search results found for {name}")
            return 0, 0
        except Exception as e:
            print(f"Error processing {name}: {e}")
            return 0, 0

    # Get list of all search result files
    search_files = Path("cache/searchxng").glob("*.json")
    names = [f.stem for f in search_files]
    
    total_pages = 0
    total_entities = 0
    
    # Process each person
    for name in names:
        print(f"\nProcessing {name}...")
        pages, entities = process_person(name)
        total_pages += pages
        total_entities += entities
    
    print(f"\nFinished processing {len(names)} people")
    print(f"Total pages crawled: {total_pages}")
    print(f"Total unique entities found: {total_entities}")

if __name__ == "__main__":
    main()