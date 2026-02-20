import asyncio
import json
import os
from typing import List, Dict, Set, Optional, Tuple, Any
from pydantic import BaseModel, Field
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from openai import AsyncOpenAI
import httpx
from urllib.parse import urljoin, urlparse, quote_plus
import aiohttp
from bs4 import BeautifulSoup
import re
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
import tqdm
from datetime import datetime
import random
import time
from src.config import School, Keywords, URLPatterns, AntiCrawlerConfig
from pathlib import Path
from flair.data import Sentence
from flair.models import SequenceTagger
from src.test_searchxng import SearchXNGTester
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher, RateLimiter
from crawl4ai import CrawlerMonitor, DisplayMode

from src.logging_config import configure_logging
configure_logging()
logger = logging.getLogger(__name__)

class ScholarProfile(BaseModel):
    name: str
    url: str
    citations: int
    h_index: int
    i10_index: int
    interests: List[str]
    papers: List[str]

class Person(BaseModel):
    name: str
    affiliations: List[str]
    current_position: str
    email: str = ""
    personal_page: str = ""
    group_page: str = ""
    google_scholar: str = ""
    citations: int = 0
    research_interests: List[str] = []
    papers: List[str] = []
    related_pages: List[str] = []
    scholar_profile: Optional[ScholarProfile] = None
    last_updated: str = ""

class PageContent(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    url: str
    html: str
    text: str
    title: str = ""
    links: List[Dict[str, Any]]

class CachedPage(BaseModel):
    """Model for cached page data"""
    url: str
    html: str
    cleaned_html: str
    title: str
    links: List[Dict[str, Any]]
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

    def to_page_content(self) -> PageContent:
        """Convert cached page to PageContent"""
        soup = BeautifulSoup(self.cleaned_html, 'html.parser')
        text = soup.get_text(separator=' ', strip=True)
        return PageContent(
            url=self.url,
            html=self.html,
            text=text,
            title=self.title,
            links=self.links
        )

class PersonMention:
    def __init__(self, name: str, context: str = "", affiliation: str = ""):
        self.name = name
        self.context = context
        self.affiliation = affiliation
        self.related_urls = []
        self.verified = False

class AcademicCrawler:
    def __init__(self, openai_api_key: str, openai_api_base: str = "https://api.alibj.aiyu.fun/v1",
                 openai_timeout: int = 60, openai_max_retries: int = 3, openai_proxy: str = None):
        self.clients = [
            AsyncOpenAI(
                api_key=openai_api_key,
                base_url=openai_api_base,
                timeout=openai_timeout,
                max_retries=openai_max_retries,
                http_client=httpx.AsyncClient(proxy=openai_proxy),
            )
        ]
        self.current_client_idx = 0

        # Load URL patterns from config
        self.url_patterns = URLPatterns()

        # Load keywords config (for blacklist filtering)
        self.keywords = Keywords()

        # Load anti-crawler config
        self.anti_crawler = AntiCrawlerConfig()

        # Initialize browser config from anti-crawler config
        # Get anti-crawler settings
        stealth_cfg = self.anti_crawler.stealth
        context_cfg = self.anti_crawler.browser_context
        rate_cfg = self.anti_crawler.rate_limiting

        # Determine User-Agent mode
        ua_mode = stealth_cfg.get('user_agent', {}).get('mode', 'random')
        # User-Agent is handled by stealth mode, use default as fallback
        user_agent = stealth_cfg.get('user_agent', {}).get('custom_pool', [None])[0] if ua_mode == 'custom' else None

        # Build BrowserConfig with anti-crawler settings
        self.browser_config = BrowserConfig(
            viewport_width=context_cfg.get('viewport', {}).get('width', 1920),
            viewport_height=context_cfg.get('viewport', {}).get('height', 1080),
            viewport={'width': context_cfg.get('viewport', {}).get('width', 1920),
                     'height': context_cfg.get('viewport', {}).get('height', 1080)},
            text_mode=False,
            user_agent=user_agent,
            user_agent_mode=ua_mode if ua_mode != 'custom' else None,
            light_mode=False,
            # Enable stealth mode
            enable_stealth=stealth_cfg.get('enabled', True),
            # Add extra browser args
            extra_args=self.anti_crawler.extra_args,
            # Add init scripts for anti-detection
            init_scripts=self.anti_crawler.init_scripts,
        )

        # Store rate limiting config for later use
        self.rate_limiting = rate_cfg
        self.request_count = 0

        # Use patterns from config instead of hardcoded
        self.category_patterns = self.url_patterns.category_patterns
        self.profile_patterns = self.url_patterns.profile_patterns
        self.profile_regex_patterns = self.url_patterns.profile_regex_patterns
        self.exclude_patterns = self.url_patterns.exclude_patterns
        self.max_path_depth = self.url_patterns.max_path_depth
        self.allowed_roots = self.url_patterns.allowed_roots

        # Browser settings - use anti-crawler config
        self.browser_settings = {}

        # Browser retry logic from anti-crawler config
        self.browser_retry_count = rate_cfg.get('max_retries', 3)
        self.browser_retry_delay = 5

        # Crawling parameters
        self.max_depth = 3
        self.max_pages_per_seed = 100
        self.visited_urls: Set[str] = set()
        self.crawled_content: List[PageContent] = []

        # Initialize aiohttp session
        self.session = None

        # Add retry configuration
        self.max_retries = 3
        self.retry_delay = 1

        # Add progress tracking
        self.total_seeds = 0
        self.processed_seeds = 0
        self.total_people = 0
        self.processed_people = 0

        # Add rate limiting (from anti_crawler config)
        delay_range = self.rate_limiting.get('delay_range', [2, 5])
        self.request_delay = sum(delay_range) / 2  # Use average as base delay
        self.last_request_time = 0
        self.request_count = 0  # Track requests for dynamic delay
        
        # Add batch size config
        self.batch_size = 3  # number of concurrent requests
        
        # Add retry delays
        self.retry_delays = [5, 15, 30]  # seconds
        
        # Add cache directories
        self.cache_dir = Path("cache")
        self.queries_cache = self.cache_dir / "queries"
        self.pages_cache = self.cache_dir / "pages"
        
        # Create cache directories
        self.cache_dir.mkdir(exist_ok=True)
        self.queries_cache.mkdir(exist_ok=True)
        self.pages_cache.mkdir(exist_ok=True)

        # NER model - lazy loaded on first use
        self.ner_tagger = None
        
        # Search configuration
        self.search_base_url = "https://opnxng.com/search"
        self.results_per_person = 5

        # Add profiles directory to existing directories
        self.profiles_dir = Path("data")
        self.profiles_dir.mkdir(exist_ok=True)

        # Add SearchXNG integration
        self.searx = SearchXNGTester()

        # Add dispatcher configuration
        self.dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=70.0,
            check_interval=1.0,
            max_session_permit=8,  # Reduced from 5 to be more conservative
            rate_limiter=RateLimiter(
                base_delay=(2.0, 5.0),  # Increased delay between requests
                max_delay=60.0,
                max_retries=5,
                rate_limit_codes=[429, 503, 504, 403]  # Added 403 for forbidden
            ),
            monitor=CrawlerMonitor(
                enable_ui=False,
                max_width=120
            )
        )

    async def setup(self):
        """Initialize resources"""
        # Initialize aiohttp session
        if not self.session:
            self.session = aiohttp.ClientSession()
        # Initialize searx
        await self.searx.setup()

        # Clear crawl4ai cache to ensure fresh crawl with new config
        import os
        cache_file = os.path.expanduser("~/.crawl4ai/crawl4ai.db")
        if os.path.exists(cache_file):
            try:
                os.remove(cache_file)
                logger.info("Cleared crawl4ai cache")
            except Exception as e:
                logger.warning(f"Could not clear crawl4ai cache: {e}")

    async def cleanup(self):
        """Cleanup resources"""
        # Cleanup aiohttp session
        if self.session:
            await self.session.close()
        # Cleanup searx
        await self.searx.cleanup()

    async def save_progress(self):
        """Save current crawling progress to file"""
        progress_data = {
            "visited_urls": list(self.visited_urls),
            "total_visited": len(self.visited_urls),
            "crawled_content_count": len(self.crawled_content),
            "timestamp": datetime.now().isoformat()
        }

        progress_file = self.cache_dir / "progress.json"
        try:
            with open(progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Progress saved: {len(self.visited_urls)} URLs visited")
        except Exception as e:
            logger.error(f"Failed to save progress: {str(e)}")

    def save_profile(self, person: Person, school_code: str) -> None:
        """Save person profile to file (simplified version for compatibility)"""
        # Create school directory
        school_dir = self.profiles_dir / school_code.lower()
        school_dir.mkdir(exist_ok=True)

        # Generate filename from person's name
        safe_name = re.sub(r'[^\w\s-]', '', person.name.lower()).replace(' ', '_')
        profile_path = school_dir / f"{safe_name}.json"

        # Convert person to dict
        data = person.model_dump()

        # Save to file
        try:
            with open(profile_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.debug(f"Saved profile for {person.name}")
        except Exception as e:
            logger.error(f"Failed to save profile for {person.name}: {str(e)}")

    async def get_next_client(self):
        client = self.clients[self.current_client_idx]
        self.current_client_idx = (self.current_client_idx + 1) % len(self.clients)
        return client

    def get_cache_key(self, *args) -> str:
        """Generate a safe cache key from arguments"""
        # Join arguments and convert to lowercase
        key = "_".join(str(arg).lower() for arg in args)
        
        # Remove or replace invalid characters
        key = re.sub(r'[^\w\-_.]', '_', key)
        key = re.sub(r'_+', '_', key)  # Replace multiple underscores with single
        
        # For URLs, extract domain and path
        if any(arg.startswith(('http://', 'https://')) for arg in args):
            try:
                url = next(arg for arg in args if arg.startswith(('http://', 'https://')))
                parsed = urlparse(url)
                domain = parsed.netloc.replace('.', '_')
                path = re.sub(r'[^\w\-_.]', '_', parsed.path)
                key = f"{domain}{path}"
            except Exception:
                pass
                
        return key.strip('_')

    def extract_page_text(self, soup: BeautifulSoup, strict: bool = False) -> str:
        """Extract text from page, preferring table text if it contains meaningful content.

        Args:
            soup: BeautifulSoup object of the page
            strict: If True, only use table_text if it contains '博导', '教授', or '院士'.
                    If False, use table_text if it contains '博导' or '教授'.

        Returns:
            Extracted text string
        """
        table_texts = []
        for table in soup.find_all('table'):
            table_texts.append(table.get_text(separator=' ', strip=True))
        table_text = ' '.join(table_texts)
        general_text = soup.get_text(separator=' ', strip=True)

        if strict:
            # Use table_text only if it has very meaningful content
            if '博导' in table_text or '教授' in table_text or '院士' in table_text:
                return table_text
        else:
            # Use table_text if it has meaningful content
            if '博导' in table_text or '教授' in table_text:
                return table_text

        return general_text

    async def generate_search_queries(self, school: str, department: str) -> List[str]:
        """Generate search queries with caching"""
        cache_key = self.get_cache_key(school, department)
        cache_file = self.queries_cache / f"{cache_key}.json"
        
        # Try to load from cache
        if cache_file.exists():
            logger.info(f"Loading cached queries for {school} {department}")
            with open(cache_file, 'r') as f:
                return json.load(f)
        
        # Generate new queries
        client = await self.get_next_client()
        response = await client.chat.completions.create(
            messages=[{
                "role": "user",
                "content": f"""I am searching for AI/CS/NLP PhD programs and I need you to generate some search queries to find faculty pages. Generate a list of search queries that are relevant but not limited to {school}'s {department} faculty.
                Return ONLY a JSON array of strings of the specified school but maybe some diverse departments (make sure they fit CS/NLP/AI), each being a search query. Example:
                ["MIT EECS faculty list", "MIT EECS professors directory"]"""
            }],
            model="gpt-4o-mini",
            temperature=0.3,
            max_tokens=500,
            response_format={"type": "json_object"}
        )
        
        queries = json.loads(response.choices[0].message.content)
        
        # Save to cache
        with open(cache_file, 'w') as f:
            json.dump(queries, f)
        
        return queries

    async def is_relevant_url(self, url: str) -> bool:
        """Check if URL is likely to contain faculty/researcher information"""
        url_lower = url.lower()

        # Check exclude patterns first - use config patterns
        if any(pattern in url_lower for pattern in self.exclude_patterns):
            return False

        # Check blacklist_regex patterns
        if hasattr(self, 'keywords') and self.keywords:
            if self.keywords.is_blacklisted(url):
                return False

        # Check category patterns from config (convert to lowercase for matching)
        category_patterns_lower = [p.lower() for p in self.category_patterns]
        if any(pattern in url_lower for pattern in category_patterns_lower):
            return True

        return False

    async def get_seed_urls(self, school_name: str, department: str) -> Set[str]:
        """Get seed URLs with caching"""
        urls = set()  # Initialize urls set
        queries = await self.generate_search_queries(school_name, department)  # Get queries first
        
        try:
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                for query in queries:
                    search_url = f"https://www.google.com/search?q={query}"
                    result = await crawler.arun(
                        url=search_url,
                        config=CrawlerRunConfig(
                            wait_for="css:#search",
                            cache_mode=CacheMode.BYPASS  # Bypass cache to avoid schema issues
                        )
                    )
                    
                    if result.success:
                        # Filter and add relevant URLs
                        for link in result.links.get("external", []):
                            url = link["href"]
                            if await self.is_relevant_url(url):
                                urls.add(url)
                                logger.debug(f"Added seed URL: {url}")
                    else:
                        logger.warning(f"Search failed for query: {query}")
                        
        except Exception as e:
            logger.error(f"Browser error: {str(e)}")
            
        logger.info(f"Found {len(urls)} seed URLs after filtering")
        return urls

    async def batch_crawl_urls(self, urls: List[str]) -> List[PageContent]:
        """Crawl multiple URLs in parallel with proper rate limiting and retry"""
        results = []

        # Get browser context config
        context_cfg = self.anti_crawler.browser_context
        rate_cfg = self.anti_crawler.rate_limiting
        max_retries = rate_cfg.get('max_retries', 3)

        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            run_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,  # Bypass cache to avoid schema issues
                session_id="academic_crawler",
                excluded_tags=["script", "style", "img", "video", "audio"],  # 忽略图片、视频、音频资源
                keep_data_attributes=True,
                check_robots_txt=False,  # Respect robots.txt
                stream=True,  # Enable streaming mode to get async iterator
                # Load from anti-crawler config
                wait_for=context_cfg.get('wait_for', 'body'),
                delay_before_return_html=context_cfg.get('delay_before_return_html', 2),
                page_timeout=context_cfg.get('page_timeout', 60000),
                # Anti-crawler: locale and timezone
                locale=context_cfg.get('locale', 'en-US'),
                timezone_id=context_cfg.get('timezone_id', 'America/New_York'),
            )

            # First attempt
            failed_urls = []
            successful_urls = []

            try:
                # Get the async iterator
                async_results = await crawler.arun_many(
                    urls=urls,
                    config=run_config,
                    dispatcher=self.dispatcher
                )

                # Process results as they come in
                async for result in async_results:
                    if result.success:
                        successful_urls.append(result.url)
                        # Process successful result - use raw HTML to preserve table content
                        soup = BeautifulSoup(result.html, 'html.parser')

                        # Extract text using helper method
                        cleaned_text = self.extract_page_text(soup, strict=True)

                        # Extract links
                        all_links = []
                        for link_type in ["internal", "external"]:
                            all_links.extend(result.links.get(link_type, []))

                        # Create page content - ensure title is not None
                        page_title = result.metadata.get('title') or ''
                        page_content = PageContent(
                            url=result.url,
                            html=result.html,
                            text=cleaned_text,
                            title=page_title,
                            links=all_links
                        )

                        # Try to load from cache first
                        if not await self.load_from_cache(result.url):
                            # Cache the result if not already cached
                            await self.cache_page_content(page_content)

                        # Add to crawled_content for entity extraction
                        self.crawled_content.append(page_content)

                        results.append(page_content)
                    else:
                        # Collect failed URLs for retry
                        error_msg = result.error_message or ""
                        if "ERR_ABORTED" in error_msg or "net::" in error_msg or "navigation" in error_msg.lower():
                            failed_urls.append(result.url)
                        logger.warning(f"Failed to crawl {result.url}: {result.error_message}")

            except Exception as e:
                logger.error(f"Error in batch crawling: {str(e)}")

            # Retry failed URLs (e.g., ERR_ABORTED errors)
            if failed_urls and max_retries > 1:
                for attempt in range(1, max_retries):
                    if not failed_urls:
                        break

                    logger.info(f"Retrying {len(failed_urls)} failed URLs (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(retry_delay * attempt)  # Exponential backoff

                    retry_results = []
                    retry_failed = []

                    try:
                        async_retry = await crawler.arun_many(
                            urls=failed_urls,
                            config=run_config,
                            dispatcher=self.dispatcher
                        )

                        async for result in async_retry:
                            if result.success:
                                retry_results.append(result.url)
                                soup = BeautifulSoup(result.html, 'html.parser')

                                # Extract text using helper method
                                cleaned_text = self.extract_page_text(soup)

                                all_links = []
                                for link_type in ["internal", "external"]:
                                    all_links.extend(result.links.get(link_type, []))

                                page_title = result.metadata.get('title') or ''
                                page_content = PageContent(
                                    url=result.url,
                                    html=result.html,
                                    text=cleaned_text,
                                    title=page_title,
                                    links=all_links
                                )

                                if not await self.load_from_cache(result.url):
                                    await self.cache_page_content(page_content)

                                self.crawled_content.append(page_content)
                                results.append(page_content)
                            else:
                                error_msg = result.error_message or ""
                                if "ERR_ABORTED" in error_msg or "net::" in error_msg:
                                    retry_failed.append(result.url)
                                logger.warning(f"Retry failed for {result.url}: {result.error_message}")

                    except Exception as e:
                        logger.error(f"Error in retry batch crawling: {str(e)}")

                    failed_urls = retry_failed
                    logger.info(f"Retry completed: {len(retry_results)} succeeded, {len(retry_failed)} still failed")

        return results

    async def cache_page_content(self, page_content: PageContent):
        """Enhanced caching with error handling"""
        try:
            cache_key = self.get_cache_key(page_content.url)
            cache_path = self.pages_cache / f"{cache_key}.json"
            
            data = {
                'url': page_content.url,
                'html': page_content.html,
                'text': page_content.text,
                'title': page_content.title,
                'links': page_content.links,
                'timestamp': datetime.now().isoformat()
            }
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            logger.debug(f"Cached page: {page_content.url}")
            
        except Exception as e:
            logger.error(f"Failed to cache page {page_content.url}: {str(e)}")
            raise

    async def load_from_cache(self, url: str) -> bool:
        """Load page content from cache if available"""
        try:
            cache_key = self.get_cache_key(url)
            cache_file = self.pages_cache / f"{cache_key}.html"
            cache_meta = self.pages_cache / f"{cache_key}.json"
            
            if cache_file.exists() and cache_meta.exists():
                logger.debug(f"Loading cached content for {url}")
                
                # Load HTML content
                with open(cache_file, 'r', encoding='utf-8') as f:
                    html = f.read()
                    
                # Load metadata
                with open(cache_meta, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                
                # Create PageContent object - ensure title is not None
                page_title = metadata.get('title') or ''
                page_content = PageContent(
                    url=url,
                    html=html,
                    text=BeautifulSoup(html, 'html.parser').get_text(separator=' ', strip=True),
                    title=page_title,
                    links=metadata.get('links', [])
                )
                
                self.crawled_content.append(page_content)
                return True
                
        except Exception as e:
            logger.warning(f"Failed to load cache for {url}: {str(e)}")
            
        return False

    async def crawl_site(self, url: str, depth: int = 0) -> None:
        """Updated crawl_site to use batch crawling"""
        if depth > self.max_depth or url in self.visited_urls:
            return
            
        self.visited_urls.add(url)
        
        # Try cache first
        if await self.load_from_cache(url):
            return
            
        # Collect URLs to crawl in parallel
        urls_to_crawl = {url}

        # Crawl the initial URL and collect more URLs
        results = await self.batch_crawl_urls([url])
        if results:
            for page_content in results:
                for link in page_content.links:
                    next_url = link["href"]
                    if (self.is_same_domain(url, next_url) and
                        await self.is_relevant_url(next_url) and
                        next_url not in self.visited_urls):
                        urls_to_crawl.add(next_url)
                        logger.debug(f"Discovered URL: {next_url}")

        logger.info(f"Crawling {len(urls_to_crawl)} URLs at depth {depth}: {list(urls_to_crawl)[:5]}...")

        # Batch crawl collected URLs
        if depth < self.max_depth and urls_to_crawl:
            await self.batch_crawl_urls(list(urls_to_crawl))

    def is_same_domain(self, url1: str, url2: str) -> bool:
        """Check if two URLs belong to the same domain"""
        try:
            domain1 = urlparse(url1).netloc
            domain2 = urlparse(url2).netloc
            # Allow subdomains of the same main domain
            return domain1.endswith(domain2) or domain2.endswith(domain1)
        except:
            return False

    async def extract_faculty_links(self, html: str, base_url: str, keywords=None) -> List[str]:
        """从页面 HTML 中提取包含关键词的链接，并排除匹配 blacklist 关键词的链接

        Args:
            html: 页面的原始 HTML
            base_url: 用于解析相对链接的基准 URL
            keywords: Keywords 对象、positive 关键词列表、或 None（使用默认配置）

        Returns:
            包含关键词的链接列表
        """
        if isinstance(keywords, Keywords):
            positive = keywords.positive
            blacklist = keywords.blacklist
        elif isinstance(keywords, list):
            positive = keywords
            blacklist = []
        else:
            kw_obj = Keywords()
            positive = kw_obj.positive
            blacklist = kw_obj.blacklist

        soup = BeautifulSoup(html, 'html.parser')
        links = []

        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            # 获取链接文本，保留中文用于匹配
            text = a_tag.get_text(strip=True)

            # Skip empty hrefs or javascript links
            if not href or href.startswith(('javascript:', 'mailto:', 'tel:')):
                continue

            # 修复1：清理 href 中的空白字符
            href = href.strip()

            # 首先检查链接文本或 href 是否包含关键词
            # 对 href 进行小写匹配（英文）
            href_lower = href.lower()

            # 对文本进行小写匹配（英文）和原文匹配（中文）
            text_lower = text.lower()
            text_pure = text  # 保留原文用于中文匹配

            # 检查是否匹配 positive 关键词
            matched = False
            for kw in positive:
                # 对于英文关键词，使用小写匹配
                if kw.isascii():
                    kw_lower = kw.lower()
                    if kw_lower in href_lower:
                        matched = True
                        break
                    if kw_lower in text_lower:
                        matched = True
                        break
                else:
                    # 对于中文关键词，直接在原文文本中匹配
                    if kw in text_pure:
                        matched = True
                        break

            if not matched:
                continue  # 不匹配 positive 关键词，跳过

            # 检查是否匹配 blacklist 关键词（URL 路径或链接文本任一匹配即排除）
            excluded = False
            for nkw in blacklist:
                if nkw.isascii():
                    nkw_lower = nkw.lower()
                    if nkw_lower in href_lower or nkw_lower in text_lower:
                        excluded = True
                        break
                else:
                    if nkw in text_pure or nkw in href:
                        excluded = True
                        break

            if excluded:
                logger.debug(f"Excluded by blacklist keyword: {href} ({text})")
                continue

            # 匹配的链接，进行 URL 验证和转换
            # 修复2：全面 URL 规范化，避免 crawl4ai 异常处理
            absolute_url = urljoin(base_url, href)

            # 验证 URL 格式是否有效
            try:
                parsed = urlparse(absolute_url)
                # 确保 URL 有有效的 scheme 和 netloc
                if not parsed.scheme or not parsed.netloc:
                    logger.warning(f"无效 URL 格式: {absolute_url}")
                    continue
                # 检查 path 中是否有空格（可能导致 crawl4ai 截断）
                if parsed.path and ' ' in parsed.path:
                    logger.warning(f"URL path 包含空格: {absolute_url}")
                    continue
            except Exception as e:
                logger.warning(f"URL 解析错误: {absolute_url}, {e}")
                continue

            links.append(absolute_url)

        return list(set(links))  # 去重

    async def extract_detail_page_links(self, html: str, base_url: str) -> List[str]:
        """从栏目页面提取每个人的详情页链接"""
        soup = BeautifulSoup(html, 'html.parser')
        links = []

        # 合并所有详情页模式（从配置加载）
        all_profile_patterns = self.profile_patterns + self.profile_regex_patterns

        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if not href or href.startswith(('javascript:', 'mailto:', 'tel:', '#')):
                continue

            # 检查是否匹配详情页模式（从配置加载）
            is_detail = any(re.search(p, href, re.IGNORECASE) for p in all_profile_patterns)
            if not is_detail:
                continue

            # 检查是否应排除（从配置加载）
            should_exclude = any(re.search(p, href, re.IGNORECASE) for p in self.exclude_patterns)
            if should_exclude:
                continue

            # 转换为绝对 URL
            absolute_url = urljoin(base_url, href)

            # 使用 is_valid_profile_url 验证
            if not self.is_valid_profile_url(absolute_url):
                continue

            links.append(absolute_url)

        return list(set(links))

    def is_valid_profile_url(self, url: str) -> bool:
        """验证是否为有效的导师个人主页 URL"""
        parsed = urlparse(url)
        path = parsed.path
        path_parts = [p for p in path.split('/') if p]

        # 路径深度检查（从配置加载）
        if len(path_parts) > self.max_path_depth:
            return False

        # 根路径检查（从配置加载）
        if path_parts:
            root = path_parts[0].lower()
            # 允许的根路径 或 ~username 或 c开头的ID (支持 c2639a153642 格式)
            # 或者根路径是纯数字（用于某些大学网站的特殊格式如 /58/xx/xxx/）
            if (root not in self.allowed_roots and
                not root.startswith('~') and
                not re.match(r'^c[a-z0-9]+', root) and
                not root.isdigit()):
                return False

        return True

    async def batch_extract_entities(self, contents: List[PageContent]) -> List[Person]:
        """Extract people information from multiple pages using GPT-4"""
        chunk_size = 5
        chunks = [contents[i:i + chunk_size] for i in range(0, len(contents), chunk_size)]

        # Get extraction fields from config
        llm_cfg = self.url_patterns.llm_extraction
        include_fields = llm_cfg.get('include_fields', [])
        exclude_fields = llm_cfg.get('exclude_fields', [])

        all_people = []

        for chunk in chunks:
            # Debug: show URLs in this chunk
            logger.debug(f"Processing chunk with URLs: {[content.url for content in chunk]}")

            # Debug: show text length for each page
            for content in chunk:
                logger.debug(f"=== DEBUG: Page {content.url}")
                logger.debug(f"=== DEBUG: Title: {content.title}")
                logger.debug(f"=== DEBUG: Text length: {len(content.text)}")
                logger.debug(f"=== DEBUG: Text preview: {content.text[:500]}")

            combined_text = "\n---\n".join([
                f"URL: {content.url}\nTitle: {content.title}\n" +
                f"Links: {', '.join(link['href'] for link in content.links if 'mailto:' in link['href'])}\n" +
                f"Content: {content.text[:2000]}"  # Include more content
                for content in chunk
            ])

            client = await self.get_next_client()
            logger.debug(f"Processing chunk with URLs: {[content.url for content in chunk]}")

            # Build extraction instructions from config
            extraction_instructions = ""
            if include_fields:
                extraction_instructions += f"Required fields: {', '.join(include_fields)}\n"
            if exclude_fields:
                extraction_instructions += f"Do NOT include: {', '.join(exclude_fields)}\n"

            try:
                response = await client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{
                        "role": "user",
                        "content": f"""Extract faculty/researcher information from these pages.
                        Pay special attention to email addresses and links. Do not give examples or simulations or fake data, if there is no information, just return an empty array, this is very important!
                        {extraction_instructions}
                        For each person found, return their details in this exact JSON format:
                        {{
                            "name": "Full Name",
                            "affiliations": ["Department", "University"],
                            "current_position": "Current Role",
                            "email": "email@domain.edu",
                            "personal_page": "https://...",
                            "group_page": "https://...",
                            "google_scholar": "https://...",
                            "citations": 0,
                            "research_interests": ["interest1", "interest2"],
                            "papers": ["paper1", "paper2"],
                            "related_pages": ["url1", "url2"]
                        }}
                        Return as a JSON array of people objects.\n\n{combined_text}"""
                    }],
                    temperature=0.3,
                    response_format={"type": "json_object"}
                )
                
                logger.debug(f"Raw API response: {response.choices[0].message.content}")
                try:
                    response_json = json.loads(response.choices[0].message.content)
                    # Ensure we have a list of people - handle different response formats
                    if isinstance(response_json, dict):
                        if "people" in response_json:
                            people_data = response_json["people"]
                        elif "faculty" in response_json:
                            people_data = response_json["faculty"]
                        elif "members" in response_json:
                            people_data = response_json["members"]
                        else:
                            logger.error(f"Unexpected response format: {response_json}")
                            continue
                    elif isinstance(response_json, list):
                        people_data = response_json
                    else:
                        logger.error(f"Unexpected response format: {response_json}")
                        continue
                        
                    for person_data in people_data:
                        try:
                            # Ensure all required fields exist with defaults
                            person_data.setdefault("email", "")
                            person_data.setdefault("personal_page", "")
                            person_data.setdefault("group_page", "")
                            person_data.setdefault("google_scholar", "")
                            person_data.setdefault("citations", 0)
                            person_data.setdefault("research_interests", [])
                            person_data.setdefault("papers", [])
                            person_data.setdefault("related_pages", [])
                            
                            person = Person(**person_data)
                            all_people.append(person)
                            logger.debug(f"Extracted person: {person.name}")
                        except Exception as e:
                            logger.error(f"Error creating Person object: {str(e)}")
                            logger.debug(f"Problem data: {person_data}")
                            
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing JSON response: {str(e)}")
                    logger.debug(f"Raw response: {response.choices[0].message.content}")
                    
            except Exception as e:
                logger.error(f"Error in API call: {str(e)}")
                
        return all_people

    def deduplicate_people(self, people: List[Person]) -> List[Person]:
        """Deduplicate people based on name and merge information from duplicates"""
        unique_people_dict: Dict[str, Person] = {}

        for person in people:
            normalized_name = re.sub(r'[^\w\s]', '', person.name.lower())
            if normalized_name not in unique_people_dict:
                unique_people_dict[normalized_name] = person
            else:
                # Merge information from duplicate person
                existing = unique_people_dict[normalized_name]
                # Merge lists (deduplicate)
                if person.related_pages:
                    existing.related_pages = list(set(existing.related_pages + person.related_pages))
                if person.research_interests:
                    existing.research_interests = list(set(existing.research_interests + person.research_interests))
                if person.papers:
                    existing.papers = list(set(existing.papers + person.papers))
                # Merge string fields (keep longer non-empty value)
                for field, new_val in [
                    ('email', person.email),
                    ('personal_page', person.personal_page),
                    ('group_page', person.group_page),
                    ('google_scholar', person.google_scholar),
                ]:
                    if new_val and (
                        not getattr(existing, field) or
                        len(new_val) > len(getattr(existing, field))
                    ):
                        setattr(existing, field, new_val)
                # Merge numeric fields (keep larger value)
                if person.citations > existing.citations:
                    existing.citations = person.citations

        return list(unique_people_dict.values())

    async def find_scholar_profile(self, person: Person) -> Optional[ScholarProfile]:
        """Find Scholar profile with caching"""
        cache_key = self.get_cache_key("scholar", person.name)
        cache_file = self.cache_dir / "scholar" / f"{cache_key}.json"
        
        # Create scholar cache directory
        (self.cache_dir / "scholar").mkdir(exist_ok=True)
        
        # Try to load from cache
        if cache_file.exists():
            logger.info(f"Loading cached Scholar profile for {person.name}")
            with open(cache_file, 'r') as f:
                data = json.load(f)
                return ScholarProfile(**data) if data else None
        
        try:
            client = await self.get_next_client()
            response = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{
                    "role": "user",
                    "content": f"Find Google Scholar profile information for {person.name} from {person.affiliations[0] if person.affiliations else 'unknown'}"
                }],
                temperature=0.3,
                response_format={"type": "json_object"}
            )
            profile_data = json.loads(response.choices[0].message.content)
            profile = ScholarProfile(**profile_data)
            
            # Save to cache
            with open(cache_file, 'w') as f:
                json.dump(profile.model_dump(), f)
                
            return profile
            
        except Exception as e:
            logger.error(f"Error finding Scholar profile for {person.name}: {str(e)}")
            return None

    async def wait_for_rate_limit(self):
        """Implement rate limiting with anti-crawler config"""
        # Get dynamic delay from anti_crawler config
        delay = self.anti_crawler.get_random_delay()

        # Add extra delay for long running sessions
        increase_every = self.rate_limiting.get('increase_every', 30)
        if self.request_count > 0 and self.request_count % increase_every == 0:
            long_run_increase = self.rate_limiting.get('long_run_delay_increase', 0.5)
            delay += long_run_increase
            logger.debug(f"Long run detected, adding {long_run_increase}s extra delay")

        now = time.time()
        time_since_last = now - self.last_request_time
        if time_since_last < delay:
            await asyncio.sleep(delay - time_since_last)

        self.last_request_time = time.time()
        self.request_count += 1

    async def extract_people_mentions(self, text: str, context_window: int = 200) -> List[PersonMention]:
        """Extract people mentions using Flair NER"""
        # Lazy load NER model on first use
        if self.ner_tagger is None:
            logger.info("Loading NER model (first use)...")
            self.ner_tagger = SequenceTagger.load('flair/ner-english-large')

        sentence = Sentence(text)
        self.ner_tagger.predict(sentence)
        
        people = []
        for entity in sentence.get_spans('ner'):
            if entity.tag == 'PER':
                # Get context around the mention using start/end position
                start_pos = entity.start_position  # Use start_position instead of start_pos
                end_pos = entity.end_position     # Use end_position instead of end_pos
                
                # Get context around the mention
                start = max(0, start_pos - context_window)
                end = min(len(text), end_pos + context_window)
                context = text[start:end]
                
                # Try to find affiliation in context
                org_mentions = [e for e in sentence.get_spans('ner') 
                              if e.tag == 'ORG' and 
                              abs(e.start_position - start_pos) < context_window]  # Use start_position
                
                affiliation = org_mentions[0].text if org_mentions else ""
                
                people.append(PersonMention(
                    name=entity.text,
                    context=context,
                    affiliation=affiliation
                ))
        
        return people

    async def search_person(self, name: str, affiliation: str = "") -> List[Dict[str, str]]:
        """Search for person using multiple SearXNG instances with retries"""
        query = f"{name} {affiliation}".strip()
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                results = await self.searx.search(query)
                if results:  # If we got any results
                    # Filter and process results
                    filtered_results = []
                    for result in results:
                        # Check if result is relevant
                        if name.lower() in result['title'].lower() or name.lower() in result['snippet'].lower():
                            filtered_results.append({
                                'title': result['title'],
                                'url': result['url'],
                                'snippet': result['snippet']
                            })
                    return filtered_results
                    
            except Exception as e:
                logger.warning(f"Search attempt {attempt + 1} failed for {name}: {str(e)}")
                if attempt < max_retries - 1:  # Don't sleep on last attempt
                    await asyncio.sleep(retry_delay * (attempt + 1))  # Exponential backoff
        
        logger.error(f"All search attempts failed for {name}")
        return []  # Return empty list if all attempts fail

    async def verify_person_mention(self, mention: PersonMention, content: List[PageContent]) -> Optional[Person]:
        """Verify a person mention using both cached content and SearXNG search"""
        # First check cached content
        relevant_pages = []
        email = ""
        personal_page = ""
        group_page = ""
        
        # Check cached content
        for page in content:
            if mention.name.lower() in page.text.lower():
                relevant_pages.append(page.url)
                # Look for email in links
                email_links = [link['href'] for link in page.links 
                             if 'mailto:' in link['href'] and mention.name.split()[-1].lower() in link['href'].lower()]
                if email_links:
                    email = email_links[0].replace('mailto:', '')
                
                # Look for personal/group pages
                for link in page.links:
                    href = link['href'].lower()
                    if mention.name.split()[-1].lower() in href:
                        if any(p in href for p in ['/~', '/users/', '/people/', '/faculty/']):
                            personal_page = link['href']
                        elif any(p in href for p in ['/group/', '/lab/', '/research/']):
                            group_page = link['href']
        
        # Search using SearXNG
        search_results = await self.search_person(mention.name, mention.affiliation)
        for result in search_results:
            relevant_pages.append(result['url'])
            # Look for academic/personal pages
            url = result['url'].lower()
            if any(p in url for p in ['/~', '/users/', '/people/', '/faculty/']):
                personal_page = result['url']
            elif any(p in url for p in ['/group/', '/lab/', '/research/']):
                group_page = result['url']
        
        if relevant_pages:
            return Person(
                name=mention.name,
                affiliations=[mention.affiliation] if mention.affiliation else [],
                current_position="",
                email=email,
                personal_page=personal_page,
                group_page=group_page,
                related_pages=list(set(relevant_pages))  # Deduplicate
                )
        return None

    async def process_cached_content(self) -> List[Person]:
        """Process cached content to find and verify people"""
        all_people_mentions = []
        
        # Phase 1: Extract people mentions from cached content
        for content in tqdm.tqdm(self.crawled_content, desc="Extracting mentions"):
            try:
                mentions = await self.extract_people_mentions(content.text)
                all_people_mentions.extend(mentions)
            except Exception as e:
                logger.error(f"Error extracting mentions from {content.url}: {str(e)}")
                continue
        
        logger.info(f"Found {len(all_people_mentions)} initial mentions")
        
        # Deduplicate mentions
        unique_mentions = {}
        for mention in all_people_mentions:
            if mention.name not in unique_mentions:
                unique_mentions[mention.name] = mention
            else:
                # Merge context and affiliation
                existing = unique_mentions[mention.name]
                if mention.affiliation and not existing.affiliation:
                    existing.affiliation = mention.affiliation
                existing.context += f"\n{mention.context}"
        
        logger.info(f"Found {len(unique_mentions)} unique people mentions")
        
        # Phase 2: Verify and enrich profiles
        verified_people = []
        
        for mention in tqdm.tqdm(unique_mentions.values(), desc="Verifying and enriching profiles"):
            try:
                # First verify the person
                person = await self.verify_person_mention(mention, self.crawled_content)
                if person:
                    # Then search for additional information
                    search_results = await self.search_person(mention.name, mention.affiliation)
                    # Enrich profile with search results
                    enriched_person = await self.enrich_person_profile(person, search_results)
                    verified_people.append(enriched_person)
                    logger.debug(f"Enriched profile for: {person.name}")
            except Exception as e:
                logger.error(f"Error processing mention {mention.name}: {str(e)}")
                continue
        
        logger.info(f"Verified and enriched {len(verified_people)} profiles")
        return verified_people

    def merge_person_data(self, existing: dict, new: dict) -> dict:
        """Merge two person dictionaries, keeping the most complete information"""
        merged = existing.copy()
        
        for key, new_value in new.items():
            # If key doesn't exist in existing data, use new value
            if key not in merged:
                merged[key] = new_value
                continue
                
            existing_value = merged[key]
            
            # Handle different types of values
            if isinstance(new_value, list):
                # For lists (like papers, interests), merge and deduplicate
                if existing_value:
                    merged[key] = list(set(existing_value + new_value))
                else:
                    merged[key] = new_value
            elif isinstance(new_value, str):
                # For strings, use the longer non-empty value
                if len(new_value) > len(existing_value) and new_value.strip():
                    merged[key] = new_value
            elif isinstance(new_value, dict):
                # For nested objects (like scholar_profile), recursively merge
                if existing_value:
                    merged[key] = self.merge_person_data(existing_value, new_value)
                else:
                    merged[key] = new_value
            else:
                # For other types (int, etc.), use new value if existing is empty/zero
                if not existing_value and new_value:
                    merged[key] = new_value
        
        return merged

    def save_person_profile(self, person: Person, school: School) -> None:
        """Save person profile with smart merging"""
        # Create school directory
        school_dir = self.profiles_dir / school.code.lower()
        school_dir.mkdir(exist_ok=True)

        # Generate filename from person's name
        safe_name = re.sub(r'[^\w\s-]', '', person.name.lower()).replace(' ', '_')
        profile_path = school_dir / f"{safe_name}.json"

        # Convert person to dict
        new_data = person.model_dump()

        # If profile exists, merge with existing data
        if profile_path.exists():
            try:
                with open(profile_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                merged_data = self.merge_person_data(existing_data, new_data)
            except Exception as e:
                logger.error(f"Error reading existing profile for {person.name}: {str(e)}")
                merged_data = new_data
        else:
            merged_data = new_data
        
        # Save merged data
        try:
            with open(profile_path, 'w', encoding='utf-8') as f:
                json.dump(merged_data, f, indent=2, ensure_ascii=False)
            logger.debug(f"Saved/updated profile for {person.name}")
        except Exception as e:
            logger.error(f"Error saving profile for {person.name}: {str(e)}")

    async def process_person(self, person: Person, school: School) -> Person:
        """Process a single person with proper caching and crawling"""
        logger.info(f"Processing {person.name} from {school.name}")
        
        # 1. Load or fetch search results
        search_cache_key = self.get_cache_key(person.name, school.name)
        search_cache_file = self.cache_dir / "searchxng" / f"{search_cache_key}.json"
        
        if search_cache_file.exists():
            with open(search_cache_file, 'r') as f:
                search_results = json.load(f)
            logger.debug(f"Loaded {len(search_results)} cached search results for {person.name}")
        else:
            search_results = await self.search_person(person.name, school.name)
            # Cache search results
            search_cache_file.parent.mkdir(exist_ok=True)
            with open(search_cache_file, 'w') as f:
                json.dump(search_results, f)
        
        # 2. Collect all relevant URLs
        urls_to_crawl = set()
        visited_urls = set()
        
        # Add initial URLs
        for result in search_results:
            urls_to_crawl.add(result['url'])
        
        # Add known URLs
        if person.personal_page:
            urls_to_crawl.add(person.personal_page)
        if person.group_page:
            urls_to_crawl.add(person.group_page)
        if person.google_scholar:
            urls_to_crawl.add(person.google_scholar)
        urls_to_crawl.update(person.related_pages)
        
        # 3. BFS crawling with caching
        content_collection = []
        max_depth = 2  # Limit depth for person-specific crawling
        
        async def process_url_batch(urls: List[str]) -> List[str]:
            """Process a batch of URLs and return new URLs to crawl"""
            new_urls = set()
            results = await self.batch_crawl_urls(urls)
            
            for page_content in results:
                if page_content.url in visited_urls:
                    continue
                    
                visited_urls.add(page_content.url)
                
                # Store content
                content_collection.append({
                    'url': page_content.url,
                    'title': page_content.title,
                    'content': page_content.text[:4000],
                    'links': page_content.links
                })
                
                # Collect relevant links
                for link in page_content.links:
                    href = link.get('href', '')
                    if not href or href in visited_urls or href in urls_to_crawl:
                        continue
                    
                    # Check relevance to person
                    if any(pattern in href.lower() for pattern in [
                        person.name.lower().replace(' ', '-'),
                        person.name.lower().replace(' ', '_'),
                        'profile', 'research', 'publication', 'cv',
                        'scholar.google.com', 'dblp.org',
                        'semantic-scholar', 'researchgate'
                    ]):
                        new_urls.add(href)
            
            return list(new_urls)
        
        # BFS crawling
        depth = 0
        while urls_to_crawl and depth < max_depth:
            current_batch = list(urls_to_crawl)
            urls_to_crawl.clear()
            
            # Process current batch and get new URLs
            new_urls = await process_url_batch(current_batch)
            urls_to_crawl.update(new_urls)
            
            depth += 1
        
        # 4. Process collected content with LLM
        if content_collection:
            try:
                # Check for cached LLM results
                llm_cache_key = self.get_cache_key(person.name, school.name, "llm")
                llm_cache_file = self.cache_dir / "llm" / f"{llm_cache_key}.json"
                
                if llm_cache_file.exists():
                    with open(llm_cache_file, 'r') as f:
                        extracted = json.load(f)
                else:
                    # Prepare and run LLM extraction
                    prompt = self.prepare_extraction_prompt(person, content_collection)
                    client = await self.get_next_client()
                    response = await client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[{
                            "role": "system",
                            "content": """Extract comprehensive academic profile information. 
                            Focus on verifiable facts and recent information."""
                        }, {
                            "role": "user",
                            "content": prompt
                        }],
                        temperature=0.3,
                        response_format={"type": "json_object"}
                    )
                    
                    extracted = json.loads(response.choices[0].message.content)
                    
                    # Cache LLM results
                    llm_cache_file.parent.mkdir(exist_ok=True)
                    with open(llm_cache_file, 'w') as f:
                        json.dump(extracted, f)
                
                # Update person profile
                updated_person = self.update_person_profile(person, extracted)
                updated_person.related_pages = list(set(
                    updated_person.related_pages + [content['url'] for content in content_collection]
                ))
                updated_person.last_updated = datetime.now().isoformat()
                
                # Save to data directory
                save_dir = self.profiles_dir / school.code.lower()
                save_dir.mkdir(exist_ok=True)

                save_path = save_dir / f"{person.name.lower().replace(' ', '_')}.json"
                with open(save_path, 'w') as f:
                    json.dump(updated_person.model_dump(), f, indent=2)
                
                return updated_person
                
            except Exception as e:
                logger.error(f"Error processing {person.name}: {str(e)}")
                return person
        
        return person

    async def crawl_school(self, school: School, keywords: Keywords = None) -> List[Person]:
        """Modified to use improved person processing with root page extraction"""
        await self.setup()

        logger.info(f"Starting crawl for {school.name} (Tier {school.tier}) - {school.department}")

        base_url = school.url.rstrip('/')

        # Use per-school keywords override, or the provided Keywords object, or defaults
        if school.keywords is not None:
            kw_for_extract = school.keywords
        elif keywords is not None:
            kw_for_extract = keywords
        else:
            kw_for_extract = Keywords()

        self.visited_urls.clear()
        self.crawled_content.clear()

        # Step 1: Crawl the root page first
        logger.info(f"Crawling root page: {school.url}")
        root_results = await self.batch_crawl_urls([school.url])

        # Step 2: Extract faculty links from root page
        faculty_links = []
        if root_results and root_results[0].html:
            logger.info(f"Extracting faculty links from root page...")
            faculty_links = await self.extract_faculty_links(
                root_results[0].html,
                school.url,
                kw_for_extract
            )
            logger.info(f"Found {len(faculty_links)} candidate links from root page: {faculty_links[:5]}...")

        # Step 3: 如果从根页面没有找到链接，不使用直接拼接方案，而是跳过

        # 如果仍然没有找到链接，记录警告但继续执行
        if not faculty_links:
            logger.warning("No faculty links found from root page!")

        # Step 4: Crawl the discovered faculty links (category pages)
        category_urls = list(set(faculty_links))
        logger.info(f"Crawling {len(category_urls)} faculty/people pages...")
        category_results = await self.batch_crawl_urls(category_urls)

        # Add category results to crawled_content
        self.crawled_content.extend(category_results)

        # Step 5: Extract detail page links from category pages
        all_detail_urls = set()
        for result in category_results:
            if result.html:
                detail_links = await self.extract_detail_page_links(result.html, result.url)
                all_detail_urls.update(detail_links)
                logger.info(f"从 {result.url} 提取到 {len(detail_links)} 个详情页链接")

        # Step 6: Crawl detail pages
        if all_detail_urls:
            detail_results = await self.batch_crawl_urls(list(all_detail_urls))
            self.crawled_content.extend(detail_results)
            logger.info(f"Crawled {len(detail_results)} detail pages")
        else:
            logger.warning("未找到详情页链接，退回到使用栏目页内容")

        # Step 7: Extract people from crawled content
        people = await self.batch_extract_entities(self.crawled_content)
        unique_people = self.deduplicate_people(people)

        # Process each person
        processed_people = []
        for person in tqdm.tqdm(unique_people, desc="Processing people"):
            processed_person = await self.process_person(person, school)
            processed_people.append(processed_person)
            await asyncio.sleep(random.uniform(1, 3))  # Prevent rate limiting

        await self.cleanup()
        return processed_people

    async def enrich_person_profile(self, person: Person, search_results: List[Dict[str, str]], max_depth: int = 3) -> Person:
        """Enrich person profile by batch crawling all related pages and using LLM for extraction"""
        logger.info(f"Enriching profile for: {person.name}")
        
        # Initialize URL sets
        urls_to_crawl = set()
        visited = set()
        
        # Add initial URLs from search results
        for result in search_results:
            urls_to_crawl.add(result['url'])
        
        # Add any existing URLs we have for the person
        if person.personal_page:
            urls_to_crawl.add(person.personal_page)
        if person.group_page:
            urls_to_crawl.add(person.group_page)
        if person.google_scholar:
            urls_to_crawl.add(person.google_scholar)
        urls_to_crawl.update(person.related_pages)
        
        content_collection = []
        depth = 0
        
        while urls_to_crawl and depth < max_depth:
            current_batch = list(urls_to_crawl)
            urls_to_crawl.clear()
            
            # Batch crawl current URLs
            results = await self.batch_crawl_urls(current_batch)
            
            for page_content in results:
                if page_content.url in visited:
                    continue
                    
                visited.add(page_content.url)
                
                # Store content for LLM processing
                content_collection.append({
                    'url': page_content.url,
                    'title': page_content.title,
                    'content': page_content.text[:4000],  # Limit content length
                    'links': page_content.links
                })
                
                # Collect next level of relevant URLs
                for link in page_content.links:
                    href = link.get('href', '')
                    if not href or href in visited:
                        continue
                        
                    # Check if URL is relevant to the person
                    if any(pattern in href.lower() for pattern in [
                        person.name.lower().replace(' ', '-'),
                        person.name.lower().replace(' ', '_'),
                        'profile', 'research', 'publication', 'cv',
                        'scholar.google.com', 'dblp.org',
                        'semantic-scholar', 'researchgate'
                    ]):
                        urls_to_crawl.add(href)
            
            depth += 1
        
        # Process collected content with LLM
        if content_collection:
            try:
                # Prepare content for LLM
                prompt = self.prepare_extraction_prompt(person, content_collection)
                
                client = await self.get_next_client()
                response = await client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{
                        "role": "system",
                        "content": """Extract comprehensive academic profile information. 
                        Focus on verifiable facts and recent information. 
                        Ignore any information that seems outdated or uncertain."""
                    }, {
                        "role": "user",
                        "content": prompt
                    }],
                    temperature=0.3,
                    response_format={"type": "json_object"}
                )
                
                # Parse and update profile
                extracted = json.loads(response.choices[0].message.content)
                updated_person = self.update_person_profile(person, extracted)
                
                # Add source URLs to related_pages
                updated_person.related_pages = list(set(
                    updated_person.related_pages + [content['url'] for content in content_collection]
                ))
                
                # Update timestamp
                updated_person.last_updated = datetime.now().isoformat()
                
                return updated_person
                
            except Exception as e:
                logger.error(f"Error enriching profile for {person.name}: {str(e)}")
                return person
        
        return person

    def prepare_extraction_prompt(self, person: Person, content_collection: List[Dict]) -> str:
        """Prepare structured prompt for LLM extraction"""
        # Sort content by relevance
        sorted_content = sorted(
            content_collection,
            key=lambda x: sum(1 for pattern in [
                person.name.lower(),
                'profile', 'research', 'publication'
            ] if pattern in x['url'].lower()),
            reverse=True
        )
        
        prompt = f"""Extract detailed academic profile information for {person.name}.
        
Current known information:
- Affiliations: {', '.join(person.affiliations)}
- Position: {person.current_position}
- Research interests: {', '.join(person.research_interests)}

Please analyze the following pages and extract updated information in JSON format:

"""
        
        # Add content from each page
        for content in sorted_content:
            prompt += f"\nFrom {content['url']}:\nTitle: {content['title']}\n{content['content'][:2000]}\n---\n"
        
        prompt += """\nProvide a JSON object with these fields:
{
    "current_position": "most recent position",
    "affiliations": ["current and past institutions"],
    "research_interests": ["main research areas"],
    "email": "academic email if public",
    "papers": ["recent significant papers"],
    "citations": number or null,
    "h_index": number or null,
    "collaborators": ["key collaborators found"],
    "awards": ["recent awards or recognition"],
    "projects": ["current research projects"]
}"""
        
        return prompt

    def update_person_profile(self, person: Person, extracted: dict) -> Person:
        """Update person profile with new information, keeping existing data if new data is empty"""
        updated = person.model_dump()
        
        for key, new_value in extracted.items():
            if not new_value:  # Skip empty values
                continue
            
            if isinstance(new_value, list):
                # Merge lists without duplicates
                existing = set(updated.get(key, []))
                existing.update(new_value)
                updated[key] = list(existing)
            elif isinstance(new_value, (int, float)):
                # Update numeric values if new value is larger
                if key not in updated or new_value > updated[key]:
                    updated[key] = new_value
            else:
                # Update string values if new value is longer
                if key not in updated or (
                    isinstance(new_value, str) and 
                    len(new_value) > len(str(updated.get(key, "")))
                ):
                    updated[key] = new_value
        
        return Person(**updated) 