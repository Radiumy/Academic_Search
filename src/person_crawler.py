import asyncio
import json
import re
from pathlib import Path
from typing import List, Dict, Set, Optional
from pydantic import BaseModel, Field
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy, JsonCssExtractionStrategy
from openai import AsyncOpenAI
from urllib.parse import urlparse, quote_plus, urljoin
import httpx
from bs4 import BeautifulSoup
import logging
from flair.data import Sentence
from flair.models import SequenceTagger
import tqdm
import yaml
import random
from datetime import datetime
import aiohttp
from dataclasses import dataclass
import aiofiles
import hashlib
import os
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('crawler.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

@dataclass
class School:
    name: str
    code: str
    department: str
    url: str
    rank: int
    tier: int

@dataclass
class Person:
    name: str
    institution: str
    position: str = ""
    research_interests: List[str] = None
    papers: List[str] = None
    google_scholar: str = ""
    personal_page: str = ""
    email: str = ""
    related_pages: List[str] = None

    def __post_init__(self):
        self.research_interests = self.research_interests or []
        self.papers = self.papers or []
        self.related_pages = self.related_pages or []

class SchoolConfig(BaseModel):
    name: str
    department: str
    url: str
    tier: int

class PersonProfile(BaseModel):
    name: str
    institution: str
    position: str = ""
    research_interests: List[str] = []
    papers: List[str] = []
    google_scholar: str = ""
    related_pages: List[str] = []
    last_updated: str = Field(default_factory=lambda: datetime.now().isoformat())

class SearchQueriesResponse(BaseModel):
    """Schema for LLM-generated search queries"""
    queries: List[str] = Field(
        description="List of search queries to find faculty and research groups",
        min_items=1,
        max_items=10
    )

class AcademicSpider:
    def __init__(self, openai_api_key: str, cache_dir: str = "cache"):
        self.openai_client = AsyncOpenAI(api_key=openai_api_key, base_url='https://api.alibj.aiyu.fun/v1')
        self.session = None
        self.ner_model = SequenceTagger.load('flair/ner-english-large')
        self.cache_dir = Path(cache_dir)
        self.html_cache = self.cache_dir / "html"
        self.people_cache = self.cache_dir / "people"
        
        # Create cache directories
        self.html_cache.mkdir(parents=True, exist_ok=True)
        self.people_cache.mkdir(parents=True, exist_ok=True)
        
        # Initialize crawl4ai for person-specific crawling
        self.browser_config = BrowserConfig()
        
        # SearXNG instances (you should replace these with actual instances)
        self.searx_instances = [
            "https://searx1.example.com",
            "https://searx2.example.com",
            # Add more instances
        ]
        self.current_searx_idx = 0

    async def setup(self):
        """Initialize resources"""
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def cleanup(self):
        """Cleanup resources"""
        if self.session:
            await self.session.close()

    def load_schools(self, config_path: str) -> List[School]:
        """Load schools from YAML config"""
        with open(config_path) as f:
            data = yaml.safe_load(f)
            return [School(**school) for school in data['schools']]

    async def generate_queries(self, school: School) -> List[str]:
        """Generate search queries using OpenAI"""
        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4",
                messages=[{
                    "role": "user",
                    "content": f"""Generate search queries to find faculty pages for {school.name}'s {school.department}.
                    Focus on faculty lists, directories, and research group pages.
                    Return exactly 5 specific queries as a JSON array of strings.
                    Example: ["MIT EECS Faculty Directory", "MIT CSAIL Research Groups"]"""
                }],
                temperature=0.3,
                response_format={"type": "json_object"}
            )
            
            queries = json.loads(response.choices[0].message.content)
            return queries.get("queries", [])[:5]  # Limit to 5 queries
            
        except Exception as e:
            logger.error(f"Error generating queries for {school.name}: {e}")
            return []

    def get_cache_path(self, url: str) -> Path:
        """Generate cache file path for URL"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        return self.html_cache / f"{url_hash}.html"

    async def fetch_page(self, url: str) -> str:
        """Fetch page with caching"""
        cache_path = self.get_cache_path(url)
        
        # Check cache first
        if cache_path.exists():
            async with aiofiles.open(cache_path, 'r', encoding='utf-8') as f:
                return await f.read()

        try:
            async with self.session.get(url, timeout=30) as response:
                if response.status == 200:
                    html = await response.text()
                    # Cache the result
                    async with aiofiles.open(cache_path, 'w', encoding='utf-8') as f:
                        await f.write(html)
                    return html
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
        
        return ""

    async def crawl_site(self, url: str, visited: Set[str], max_depth: int = 3) -> Set[str]:
        """DFS crawl of a site"""
        if max_depth <= 0 or url in visited:
            return visited
        
        visited.add(url)
        html = await self.fetch_page(url)
        
        if not html:
            return visited
            
        soup = BeautifulSoup(html, 'html.parser')
        base_domain = urlparse(url).netloc
        
        for link in soup.find_all('a', href=True):
            next_url = urljoin(url, link['href'])
            if urlparse(next_url).netloc == base_domain and next_url not in visited:
                await self.crawl_site(next_url, visited, max_depth - 1)
        
        return visited

    async def extract_people(self, html: str) -> List[str]:
        """Extract people names using Flair NER"""
        sentence = Sentence(html)
        self.ner_model.predict(sentence)
        
        people = []
        for entity in sentence.get_spans('ner'):
            if entity.tag == 'PER':
                people.append(entity.text)
        
        return list(set(people))  # Deduplicate

    async def search_person(self, name: str, institution: str) -> List[str]:
        """Search for person using SearXNG with load balancing"""
        instance = self.searx_instances[self.current_searx_idx]
        self.current_searx_idx = (self.current_searx_idx + 1) % len(self.searx_instances)
        
        query = f"{name} {institution}"
        try:
            async with self.session.get(
                f"{instance}/search",
                params={'q': query, 'format': 'json'},
                timeout=30
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return [result['url'] for result in data.get('results', [])][:10]
        except Exception as e:
            logger.error(f"Error searching for {name}: {e}")
        
        return []

    async def process_person(self, name: str, institution: str, urls: List[str]) -> Person:
        """Process person information using crawl4ai and OpenAI"""
        # Crawl related pages
        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            results = await crawler.run_many(urls=urls)
            
            # Collect all text content
            all_content = []
            scholar_url = ""
            
            for result in results:
                if result.success:
                    if "scholar.google.com" in result.url:
                        scholar_url = result.url
                    soup = BeautifulSoup(result.html, 'html.parser')
                    text = soup.get_text(separator=' ', strip=True)
                    all_content.append({"url": result.url, "content": text[:2000]})

        # Extract information using OpenAI
        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4",
                messages=[{
                    "role": "user",
                    "content": f"""Extract information about {name} from {institution} from these pages:
                    {json.dumps(all_content, indent=2)}
                    
                    Return a JSON object with these fields:
                    {{
                        "name": "full name",
                        "position": "current position",
                        "research_interests": ["list of interests"],
                        "papers": ["list of recent papers"],
                        "email": "email if found",
                        "personal_page": "personal page URL"
                    }}"""
                }],
                temperature=0.3,
                response_format={"type": "json_object"}
            )
            
            data = json.loads(response.choices[0].message.content)
            return Person(
                name=data.get("name", name),
                institution=institution,
                position=data.get("position", ""),
                research_interests=data.get("research_interests", []),
                papers=data.get("papers", []),
                google_scholar=scholar_url,
                personal_page=data.get("personal_page", ""),
                email=data.get("email", ""),
                related_pages=urls
            )
            
        except Exception as e:
            logger.error(f"Error processing {name}: {e}")
            return Person(name=name, institution=institution)

    async def crawl_school(self, school: School):
        """Process a single school"""
        logger.info(f"Processing {school.name}")
        
        # Generate search queries
        queries = await self.generate_queries(school)
        
        # Collect seed URLs
        seed_urls = set()
        for query in queries:
            # Here you would implement Google search
            # For now, we'll just use the school's main URL
            seed_urls.add(school.url)
        
        # Crawl sites
        visited_urls = set()
        for url in seed_urls:
            visited_urls.update(await self.crawl_site(url, set()))
        
        # Extract people from all pages
        people_names = set()
        for url in visited_urls:
            html = await self.fetch_page(url)
            names = await self.extract_people(html)
            people_names.update(names)
        
        # Process each person
        people_data = []
        for name in people_names:
            # Search for person
            related_urls = await self.search_person(name, school.name)
            
            # Process person
            person = await self.process_person(name, school.name, related_urls)
            people_data.append(person)
        
        # Save results
        output_dir = Path("data") / school.code
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / "people.json"
        with open(output_file, 'w') as f:
            json.dump([vars(p) for p in people_data], f, indent=2)

async def main():
    # Initialize spider
    spider = AcademicSpider(
        openai_api_key="sk-TLoMP5smKo84g0rQUg27g5eEb71rY6sVNdWH0FfneTLhyGb5"
    )
    
    await spider.setup()
    
    try:
        # Load schools
        schools = spider.load_schools("config/schools.yaml")
        
        # Process each school
        for school in schools:
            await spider.crawl_school(school)
            
    finally:
        await spider.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 