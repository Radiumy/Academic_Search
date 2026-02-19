import yaml
import asyncio
from pathlib import Path
from typing import Dict, List
import json
import argparse
from dfs_worker import DFSCrawler
import logging
import multiprocessing
import os

def setup_logging(name='SchoolCrawler'):
    """Setup logging configuration"""
    from src.logging_config import configure_logging
    configure_logging()
    return logging.getLogger(name)

def crawl_school(school_name: str, urls: List[str], cache_dir: str, output_dir: str, result_queue: multiprocessing.Queue):
    """Crawl a single school's URLs and aggregate results in a separate process."""
    logger = setup_logging(f'SchoolCrawler.{school_name}')
    logger.info(f"Starting dedicated process for {school_name} with {len(urls)} seed URLs")
    
    try:
        crawler = DFSCrawler(
            cache_dir=cache_dir,
            max_concurrent=8,
            base_delay=(2.0, 4.0),
            max_delay=30.0,
            max_retries=3,
            verbose=True,
            whitelist=None,
            blacklist=[
                # File extensions to skip
                r"\.(jpg|jpeg|png|gif|pdf|zip|ico|svg|webp|mp4|mp3|avi|mov|wmv|flv|webm|doc|docx|xls|xlsx|ppt|pptx|txt|csv|json|xml|js|css|aspx|jsp|do|action|dll|exe|bat|sh|pl|cgi|rss|atom|xsl|wsdl|soap|wsf|wsc|wsm|wsa|wad|wax|wbx|wsc|wsf|wsg|wsh|wsn|wss|wst|wsu|wsx|wsy|wtv|wvx|wwe|wwp|wwz|xap|xpi|xpt|xul|zep|zip|tar|gz|bz2|rar|7z|iso|img|bin|dat|dmg|pkg|deb|rpm|msi|app|appx|appxbundle|appxupload|msix|msixbundle|msixupload|dmg|pkg|deb|rpm|msi|app|appx|appxbundle|appxupload|msix|msixbundle|msixupload)$",
                
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
                "nih", "arxiv", "pubmed", "pubmedcentral", "ncbi", "doi.org", "doi", "forms", "bit.ly", "orcid", "dblp", "linkedin"
                # Query parameters to skip
                r"[?&]utm_", r"[?&]source=", r"[?&]ref=", r"[?&]medium=",
                r"[?&]campaign=", r"[?&]term=", r"[?&]content=",
                r"[?&]page=", r"[?&]sort=", r"[?&]filter="
            ],
            use_gpu=True
        )
        
        # Crawl pages
        results = crawler.dfs(urls, depth=1)
        logger.info(f"Crawled {len(results)} pages for {school_name}")
        
        # Aggregate entities
        output_path = Path(output_dir) / f"{school_name}.json"
        aggregated = crawler.aggregate(
            output_path=str(output_path),
            min_count=1,
            max_freq=0.5
        )
        logger.info(f"Found {len(aggregated)} unique person entities for {school_name}")
        
        # Put results in queue
        result_queue.put({school_name: aggregated})
        
    except Exception as e:
        logger.error(f"Error processing {school_name}: {e}")
        result_queue.put(None)

def main():
    logger = setup_logging()
    
    # Create output directory
    output_dir = Path("./data")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load seed URLs
    with open("seed_urls.yaml", 'r') as f:
        schools = yaml.safe_load(f)
    
    # Shared cache directory
    cache_dir = "./cache_shared_v2"
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    
    # Create a queue for collecting results
    result_queue = multiprocessing.Queue()
    
    # Create list of all school tasks
    school_tasks = [(name, urls) for name, urls in schools.items()]
    
    # Process schools in batches of 3
    all_results = {}
    max_concurrent = 8
    
    for i in range(0, len(school_tasks), max_concurrent):
        batch = school_tasks[i:i + max_concurrent]
        processes = []
        
        # Start processes for current batch
        logger.info(f"Starting batch of {len(batch)} schools (schools {i+1}-{min(i+max_concurrent, len(school_tasks))})")
        for school_name, urls in batch:
            process = multiprocessing.Process(
                target=crawl_school,
                args=(school_name, urls, cache_dir, str(output_dir), result_queue),
                name=f"Crawler-{school_name}"
            )
            processes.append(process)
            process.start()
        
        # Collect results from current batch
        completed = 0
        while completed < len(batch):
            result = result_queue.get()
            if result is not None:
                all_results.update(result)
                logger.info(f"Received results from a school. Total schools completed: {len(all_results)}")
            completed += 1
        
        # Wait for all processes in batch to finish
        for p in processes:
            p.join()
        
        logger.info(f"Completed batch of {len(batch)} schools")
    
    # Save combined results
    try:
        with open("./data/schools.json", 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)
        logger.info("Saved combined results to schools.json")
    except Exception as e:
        logger.error(f"Error saving combined results: {e}")

if __name__ == "__main__":
    multiprocessing.freeze_support()  # For Windows compatibility
    parser = argparse.ArgumentParser(description="Crawl school faculty pages")
    parser.add_argument(
        '--keywords', '-k',
        type=str,
        default=None,
        help='Path to keywords YAML file (default: config/keywords.yaml)'
    )
    args = parser.parse_args()

    # Store keywords path in environment so subprocesses can access it
    if args.keywords:
        os.environ['CRAWLER_KEYWORDS_PATH'] = args.keywords

    main() 