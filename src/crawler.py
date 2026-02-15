import asyncio
import json
import os
from typing import List, Dict, Set, Optional, Tuple
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
from src.config import School  # Add this line near other imports
import pickle
from pathlib import Path
from flair.data import Sentence
from flair.models import SequenceTagger
from src.test_searchxng import SearchXNGTester
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher, RateLimiter
from crawl4ai import CrawlerMonitor, DisplayMode

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)
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
    url: str
    html: str
    text: str
    title: str
    links: List[Dict[str, str]]

    class Config:
        arbitrary_types_allowed = True

class CachedPage(BaseModel):
    """Model for cached page data"""
    url: str
    html: str
    cleaned_html: str
    title: str
    links: List[Dict[str, str]]
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
        # Replace old OpenAI initialization with new client
        self.clients = [
            AsyncOpenAI(
                api_key=openai_api_key,
                base_url=openai_api_base,
                timeout=openai_timeout,
                max_retries=openai_max_retries,
                http_client=httpx.AsyncClient(proxies=openai_proxy),
            )
        ]
        self.current_client_idx = 0
        
        # Use default browser config that works
        self.browser_config = BrowserConfig()  # Use default settings
        
        # Add URL filtering for faculty pages with more diverse patterns
        self.faculty_url_patterns = [
            # Faculty and People
            "/faculty",
            "/people",
            "/members",
            "/staff",
            "/personnel",
            "/team",
            
            # Role-based patterns
            "/role/faculty",
            "/role/faculty-cs",
            "/role/faculty-ee",
            "/role/faculty-aid",
            "/professors",
            "/instructors",
            
            # Research Groups and Labs
            "/research",
            "/groups",
            "/labs",
            "/laboratory",
            "/group",
            "/lab",
            
            # Directory and Organization
            "/directory",
            "/department",
            "/about/people",
            "/about/faculty",
            
            # Common URL patterns
            "faculty-directory",
            "faculty-profiles",
            "research-groups",
            "research-areas",
            "principal-investigators",
            "pi-profiles"
        ]

        # Add domain-specific patterns
        self.domain_patterns = {
            "mit.edu": ["/~", "/users/", "/people/"],
            "stanford.edu": ["/~", "/people/", "/profiles/"],
            "berkeley.edu": ["/~", "/users/", "/directory/"],
            "cmu.edu": ["/~", "/directory/", "/people/"],
            # Add more for other universities
        }
        
        # Add browser retry logic
        self.browser_retry_count = 3
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
        
        # Add rate limiting
        self.request_delay = 2  # seconds between requests
        self.last_request_time = 0
        
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

        # Load NER model
        self.ner_tagger = SequenceTagger.load('flair/ner-english-large')
        
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
                max_visible_rows=15,
                display_mode=DisplayMode.DETAILED
            )
        )

    async def setup(self):
        """Initialize resources"""
        # Initialize aiohttp session
        if not self.session:
            self.session = aiohttp.ClientSession()
        # Initialize searx
        await self.searx.setup()

    async def cleanup(self):
        """Cleanup resources"""
        # Cleanup aiohttp session
        if self.session:
            await self.session.close()
        # Cleanup searx
        await self.searx.cleanup()

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
            except:
                pass
                
        return key.strip('_')

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
        
        # Check general patterns
        if any(pattern in url_lower for pattern in self.faculty_url_patterns):
            return True
            
        # Check domain-specific patterns
        parsed_url = urlparse(url_lower)
        domain = parsed_url.netloc
        for known_domain, patterns in self.domain_patterns.items():
            if known_domain in domain:
                if any(pattern in url_lower for pattern in patterns):
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
                            cache_mode=CacheMode.ENABLED  # Enable caching
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
        """Crawl multiple URLs in parallel with proper rate limiting"""
        results = []
        
        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            run_config = CrawlerRunConfig(
                cache_mode=CacheMode.ENABLED,
                session_id="academic_crawler",
                excluded_tags=["script", "style", "nav", "footer"],
                keep_data_attributes=True,
                check_robots_txt=True,  # Respect robots.txt
                stream=True  # Enable streaming mode to get async iterator
            )
            
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
                        # Process successful result
                        soup = BeautifulSoup(result.cleaned_html or result.html, 'html.parser')
                        cleaned_text = soup.get_text(separator=' ', strip=True)
                        
                        # Extract links
                        all_links = []
                        for link_type in ["internal", "external"]:
                            all_links.extend(result.links.get(link_type, []))
                        
                        # Create page content
                        page_content = PageContent(
                            url=result.url,
                            html=result.html,
                            text=cleaned_text,
                            title=result.metadata.get('title', ''),
                            links=all_links
                        )
                        
                        # Try to load from cache first
                        if not await self.load_from_cache(result.url):
                            # Cache the result if not already cached
                            await self.cache_page_content(page_content)
                        
                        results.append(page_content)
                    else:
                        logger.warning(f"Failed to crawl {result.url}: {result.error_message}")
                        
            except Exception as e:
                logger.error(f"Error in batch crawling: {str(e)}")
                
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
                
                # Create PageContent object
                page_content = PageContent(
                    url=url,
                    html=html,
                    text=BeautifulSoup(html, 'html.parser').get_text(separator=' ', strip=True),
                    title=metadata.get('title', ''),
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

    async def batch_extract_entities(self, contents: List[PageContent]) -> List[Person]:
        """Extract people information from multiple pages using GPT-4"""
        chunk_size = 5
        chunks = [contents[i:i + chunk_size] for i in range(0, len(contents), chunk_size)]
        
        all_people = []
        
        for chunk in chunks:
            combined_text = "\n---\n".join([
                f"URL: {content.url}\nTitle: {content.title}\n" + 
                f"Links: {', '.join(link['href'] for link in content.links if 'mailto:' in link['href'])}\n" +
                f"Content: {content.text[:2000]}"  # Include more content
                for content in chunk
            ])
            
            client = await self.get_next_client()
            logger.debug(f"Processing chunk with URLs: {[content.url for content in chunk]}")
            
            try:
                response = await client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{
                        "role": "user",
                        "content": f"""Extract faculty/researcher information from these pages.
                        Pay special attention to email addresses and links. Do not give examples or simulations or fake data, if there is no information, just return an empty array, this is very important!
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
                    print(response.choices[0].message.content)
                    response_json = json.loads(response.choices[0].message.content)
                    # Ensure we have a list of people
                    if isinstance(response_json, dict) and "people" in response_json:
                        people_data = response_json["people"]
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
        """Deduplicate people based on name and affiliation similarity"""
        unique_people = []
        seen_names = set()
        
        for person in people:
            normalized_name = re.sub(r'[^\w\s]', '', person.name.lower())
            if normalized_name not in seen_names:
                seen_names.add(normalized_name)
                unique_people.append(person)
            else:
                # Merge information if this is the same person
                for existing in unique_people:
                    if re.sub(r'[^\w\s]', '', existing.name.lower()) == normalized_name:
                        existing.related_pages.extend(person.related_pages)
                        # Merge other fields as needed
        
        return unique_people

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
            print(response.choices[0].message.content)
            profile_data = json.loads(response.choices[0].message.content)
            profile = ScholarProfile(**profile_data)
            
            # Save to cache
            with open(cache_file, 'w') as f:
                json.dump(profile.dict(), f)
                
            return profile
            
        except Exception as e:
            logger.error(f"Error finding Scholar profile for {person.name}: {str(e)}")
            return None

    async def wait_for_rate_limit(self):
        """Implement rate limiting"""
        now = time.time()
        time_since_last = now - self.last_request_time
        if time_since_last < self.request_delay:
            await asyncio.sleep(self.request_delay - time_since_last)
        self.last_request_time = time.time()

    async def extract_people_mentions(self, text: str, context_window: int = 200) -> List[PersonMention]:
        """Extract people mentions using Flair NER"""
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
        school_dir = self.profiles_dir / school.name.lower().replace(' ', '_')
        school_dir.mkdir(exist_ok=True)
        
        # Generate filename from person's name
        safe_name = re.sub(r'[^\w\s-]', '', person.name.lower()).replace(' ', '_')
        profile_path = school_dir / f"{safe_name}.json"
        
        # Convert person to dict
        new_data = person.dict()
        
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
                save_dir = self.profiles_dir / school.name.lower().replace(' ', '_')
                save_dir.mkdir(exist_ok=True)
                
                save_path = save_dir / f"{person.name.lower().replace(' ', '_')}.json"
                with open(save_path, 'w') as f:
                    json.dump(updated_person.dict(), f, indent=2)
                
                return updated_person
                
            except Exception as e:
                logger.error(f"Error processing {person.name}: {str(e)}")
                return person
        
        return person

    async def crawl_school(self, school: School) -> List[Person]:
        """Modified to use improved person processing"""
        await self.setup()
        
        logger.info(f"Starting crawl for {school.name} (Tier {school.tier}) - {school.department}")
        
        # Initial crawl to find people
        seed_urls = {school.url}
        base_url = school.url.rstrip('/')
        
        for path in ['/people', '/faculty', '/directory']:
            seed_urls.add(f"{base_url}{path}")
        
        self.visited_urls.clear()
        self.crawled_content.clear()
        
        # Process seeds in batches
        for url in seed_urls:
            await self.crawl_site(url)
        
        # Extract people from crawled content
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
        updated = person.dict()
        
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