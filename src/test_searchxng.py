import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import json
from pathlib import Path
import logging
import random
from typing import List, Dict
from dataclasses import dataclass
import time
import urllib.parse
from tqdm import tqdm
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SearchInstance:
    url: str
    response_time: float
    uptime: float
    country: str
    version: str = ""
    
    @property
    def score(self) -> float:
        """Calculate instance score based on response time and uptime"""
        return (self.uptime / 100.0) * (1.0 / (self.response_time + 0.1))

class SearchXNGTester:
    def __init__(self):
        self.session = None
        self.instances: List[SearchInstance] = []
        self.current_instance_idx = 0  # Track current instance
        
        # Setup cache
        self.cache_dir = Path("cache/searchxng")
        self.instances_cache = self.cache_dir / "instances.json"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    async def fetch_instances(self):
        """Load instances from local searxng_instances.json"""
        try:
            # Load from local file
            with open('searxng_instances.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.instances = self.parse_instances(data)
                logger.info(f"Loaded {len(self.instances)} instances from local file")
                return self.instances
        except Exception as e:
            logger.error(f"Error loading local instances file: {str(e)}")
            return []

    def parse_instances(self, data: dict) -> List[SearchInstance]:
        """Parse all working instances from local json"""
        instances = []
        for url, info in data.get("instances", {}).items():
            if 'onion' in url:
                continue
            try:
                # Get timing data
                timing = info.get("timing", {})
                search_timing = timing.get("search", {})
                response_time = search_timing.get("all", {}).get("mean", 999)
                
                # Get uptime data
                uptime_data = info.get("uptime", {})
                uptime = uptime_data.get("uptimeMonth", 0) if isinstance(uptime_data, dict) else 0
                
                # Check if instance is working
                if (info.get("version", "").startswith("2025") and  # Recent version
                    info.get("http", {}).get("status_code") == 200):  # Working
                    
                    instance = SearchInstance(
                        url=url,
                        response_time=response_time,
                        uptime=uptime,
                        country=info.get("network", {}).get("country_code", ""),
                        version=info.get("version", "")
                    )
                    instances.append(instance)
                    
            except Exception as e:
                logger.debug(f"Error parsing instance {url}: {str(e)}")
                continue
        
        # Sort by response time
        instances.sort(key=lambda x: x.response_time)
        logger.info(f"Loaded {len(instances)} working instances")
        return instances

    async def setup(self):
        """Initialize session and load instances"""
        if not self.session:
            self.session = aiohttp.ClientSession()
        if not self.instances:
            self.instances = await self.fetch_instances()
            if not self.instances:
                raise RuntimeError("No working instances found!")

    async def cleanup(self):
        if self.session:
            await self.session.close()

    def get_cache_path(self, query: str) -> Path:
        """Generate cache file path for query"""
        safe_query = quote_plus(query)
        return self.cache_dir / f"{safe_query}.json"

    async def search(self, query: str, max_retries: int = 20) -> list:
        """Improved search with better error handling"""
        try:
            cache_path = self.get_cache_path(query)
            if cache_path.exists():
                with open(cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
                
            # Try different instances with retries
            for attempt in range(max_retries):
                instance = self.get_next_instance()
                
                # Build search URL and parameters
                # base_url = instance.url.rstrip('/')
                base_url = 'https://searxng.alibj.aiyu.fun'
                if not base_url.startswith('http'):
                    base_url = f"https://{base_url}"
                
                search_url = f"{base_url}/search"
                
                # Fix parameters - remove None values
                params = {
                    'q': query,
                    'categories': 'general',
                    'language': 'en',
                    'format': 'html',
                    'safesearch': '2'
                }
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1'
                }
                
                try:
                    # Log full request details
                    logger.info(f"\nTrying instance {instance.url} (attempt {attempt + 1})")
                    logger.info(f"Full URL: {search_url}?{urllib.parse.urlencode(params)}")
                    
                    async with self.session.get(
                        search_url,
                        params=params,
                        headers=headers,
                        timeout=30,
                        ssl=False  # Some instances might have SSL issues
                    ) as response:
                        if response.status != 200:
                            logger.warning(f"Bad response: {response.status}")
                            logger.warning(f"Response headers: {dict(response.headers)}")
                            continue
                        
                        html = await response.text()
                        if not html:
                            logger.warning("Empty response received")
                            continue
                            
                        logger.debug(f"Response length: {len(html)}")
                        
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # First try to find the results container
                        containers = soup.select('.results, #results, .content, #main')
                        if not containers:
                            logger.warning("No results container found")
                            continue
                            
                        # Try different result selectors within the container
                        results = []
                        for container in containers:
                            for result in container.select('.result, .g, .searchresult'):
                                try:
                                    # Try multiple possible selectors for each element
                                    title = None
                                    for title_selector in ['.title a', '.result-title', 'h3 a', '.r a']:
                                        title_elem = result.select_one(title_selector)
                                        if title_elem:
                                            title = title_elem.get_text(strip=True)
                                            break
                                    
                                    url = None
                                    for url_selector in ['a[href]', '.result-url', '.link']:
                                        url_elem = result.select_one(url_selector)
                                        if url_elem and url_elem.get('href'):
                                            url = url_elem['href']
                                            break
                                    
                                    snippet = None
                                    for snippet_selector in ['.content', '.snippet', '.description', '.s']:
                                        snippet_elem = result.select_one(snippet_selector)
                                        if snippet_elem:
                                            snippet = snippet_elem.get_text(strip=True)
                                            break
                                    
                                    if title and url and snippet:
                                        results.append({
                                            'title': title,
                                            'url': url,
                                            'snippet': snippet
                                        })
                                        
                                except Exception as e:
                                    logger.debug(f"Error parsing result: {str(e)}")
                                    continue
                        
                        if results:
                            if len(results) > 10:
                                results = results[:9]
                            logger.info(f"Found {len(results)} results")
                            with open(cache_path, 'w', encoding='utf-8') as f:
                                json.dump(results, f, indent=2, ensure_ascii=False)
                            return results
                        
                except aiohttp.ClientError as e:
                    logger.warning(f"Connection error: {str(e)}")
                except Exception as e:
                    logger.warning(f"Error: {str(e)}")
                
                await asyncio.sleep(2)  # Increased delay between retries
            
            logger.error("All attempts failed")
            return []
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in cache for {query}: {str(e)}")
            cache_path.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"Unexpected error during search: {str(e)}")
        
        return []

    def get_next_instance(self) -> SearchInstance:
        """Get next instance sequentially"""
        if not self.instances:
            raise RuntimeError("No instances available")
        
        instance = self.instances[self.current_instance_idx]
        self.current_instance_idx = (self.current_instance_idx + 1) % len(self.instances)
        return instance

async def main():
    # Test queries
    # test_queries = [
    #     "Danqi Chen"
    # ]
    queries = []
    with open('2709.txt', 'r') as f:
        queries = f.readlines()
    queries = [query.strip() for query in queries]
    tester = SearchXNGTester()
    await tester.setup()
    
    try:
        for query in tqdm(queries):
            logger.info(f"\nSearching for: {query}")
            results = await tester.search(query)
            print(results)
            # logger.info(f"Found {len(results)} results:")
            # for i, result in enumerate(results, 1):
                # logger.info(f"\n{i}. {result['title']}")
                # logger.info(f"   URL: {result['url']}")
                # logger.info(f"   Snippet: {result['snippet'][:200]}...")
            
    finally:
        await tester.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 