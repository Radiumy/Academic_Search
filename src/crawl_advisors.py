import json
import asyncio
from pathlib import Path
from typing import Dict, List
import multiprocessing
import os
import logging
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

def setup_logging(name='AdvisorCrawler'):
    """Setup logging configuration"""
    logger = logging.getLogger(name)
    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(f'crawler_{name}_{os.getpid()}.log')
            ]
        )
    return logger

async def crawl_urls(urls: List[str], cache_dir: str):
    """Simple function to crawl a list of URLs and cache the results"""
    browser_config = BrowserConfig(
        headless=True,
        viewport_width=1280,
        viewport_height=720,
        text_mode=True,
        verbose=True
    )

    rate_limiter = RateLimiter(
        base_delay=(2.0, 4.0),
        max_delay=30.0,
        max_retries=3,
        rate_limit_codes=[429, 503]
    )

    monitor = CrawlerMonitor(
        max_visible_rows=15,
        display_mode=DisplayMode.DETAILED
    )

    dispatcher = MemoryAdaptiveDispatcher(
        memory_threshold_percent=70.0,
        check_interval=1.0,
        max_session_permit=8,
        monitor=monitor,
        rate_limiter=rate_limiter
    )

    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        stream=True,
        word_count_threshold=200,
        wait_for=None,
        screenshot=False,
        pdf=False
    )

    results = []
    async with AsyncWebCrawler(config=browser_config) as crawler:
        async for result in await crawler.arun_many(
            urls=urls,
            config=run_config,
            dispatcher=dispatcher
        ):
            if result.success:
                results.append({
                    "url": result.url,
                    "success": result.success,
                    "html": result.html,
                    "cleaned_html": result.cleaned_html,
                    "markdown": result.markdown_v2.raw_markdown if result.markdown_v2 else "",
                    "links": {
                        "internal": result.links.get("internal", []),
                        "external": result.links.get("external", [])
                    },
                    "status_code": result.status_code,
                    "error_message": result.error_message
                })

    return results

def crawl_advisor(advisor: Dict, cache_dir: str, result_queue: multiprocessing.Queue):
    """Process a single advisor's search results"""
    name = advisor['name']
    logger = setup_logging(f'AdvisorCrawler.{name}')
    logger.info(f"Starting process for {name}")
    
    try:
        # Load search results
        search_cache_file = Path(advisor['search_result_path'])
        if not search_cache_file.exists():
            logger.error(f"No search results found for {name}")
            result_queue.put(None)
            return

        with open(search_cache_file, 'r', encoding='utf-8') as f:
            search_results = json.load(f)
        
        # Extract URLs from search results
        seed_urls = [result['url'] for result in search_results]
        logger.info(f"Found {len(seed_urls)} URLs for {name}")

        # Crawl URLs
        results = asyncio.run(crawl_urls(seed_urls, cache_dir))
        logger.info(f"Crawled {len(results)} pages for {name}")
        
        # Save results to cache
        cache_path = Path(cache_dir) / f"{name}.json"
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        result = {
            "name": name,
            "cache_path": str(cache_path),
            "original_data": advisor
        }
        result_queue.put(result)
        
    except Exception as e:
        logger.error(f"Error processing {name}: {e}")
        result_queue.put(None)

def main():
    logger = setup_logging()
    
    try:
        with open("C:\\advisor-crawler\\filtered_data_accepted.json", 'r') as f:
            advisors = json.load(f)
        advisor_items = [advisors[k] for k in advisors]
        for item in advisor_items:
            search_result_path = os.path.join("C:\\advisor-crawler\\cache\\searchxng", f"{item['name'].replace(' ', '+')}.json")
            if not os.path.exists(search_result_path):
                logger.error(f"No search results found for {item['name']}")
                advisor_items.remove(item)
            item['search_result_path'] = search_result_path
        print(f"Found {len(advisor_items)} advisors with search results")
    except FileNotFoundError:
        logger.error("filtered_data_accepted.json not found")
        return
    except json.JSONDecodeError:
        logger.error("Invalid JSON in filtered_data_accepted.json")
        return

    # Create cache directory
    cache_dir = "./cache_crawl"
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    
    # Create a queue for collecting results
    result_queue = multiprocessing.Queue()
    
    # Process advisors in batches
    all_results = []
    max_concurrent = 32
    
    for i in range(0, len(advisor_items), max_concurrent):
        batch = advisor_items[i:i + max_concurrent]
        processes = []
        
        # Start processes for current batch
        logger.info(f"Starting batch of {len(batch)} advisors (advisors {i+1}-{min(i+max_concurrent, len(advisor_items))})")
        for advisor in batch:
            process = multiprocessing.Process(
                target=crawl_advisor,
                args=(advisor, cache_dir, result_queue),
                name=f"Crawler-{advisor['name']}"
            )
            processes.append(process)
            process.start()
        
        # Collect results from current batch
        completed = 0
        while completed < len(batch):
            result = result_queue.get()
            if result is not None:
                all_results.append(result)
                logger.info(f"Received results from {result['name']}. Total advisors completed: {len(all_results)}")
            completed += 1
        
        # Wait for all processes in batch to finish
        for p in processes:
            p.join()
        
        logger.info(f"Completed batch of {len(batch)} advisors")
    
    # Save index of results
    try:
        with open("./cache_crawl/index.json", 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)
        logger.info("Saved results index to cache_crawl/index.json")
    except Exception as e:
        logger.error(f"Error saving results index: {e}")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main() 