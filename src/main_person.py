import asyncio
import argparse
from pathlib import Path
import yaml
import json
import logging
from datetime import datetime
from typing import List, Optional
from person_crawler import AcademicPersonCrawler
from logging_config import configure_logging
from dataclasses import dataclass

@dataclass
class School:
    name: str
    code: str
    rank: int
    department: str
    url: str
    tier: int

class CrawlerConfig:
    def __init__(self, config_path: str):
        with open(config_path) as f:
            config = yaml.safe_load(f)
            
        # Load SearXNG instances
        self.searxng_instances = self.load_searxng_instances()
        
        # Convert school dictionaries to School objects
        self.schools = [School(**school) for school in config.get('schools', [])]
        
    def load_searxng_instances(self) -> List[str]:
        """Load and validate SearXNG instances"""
        instances_file = Path("config/searxng_instances.json")
        if not instances_file.exists():
            raise FileNotFoundError("SearXNG instances file not found")
            
        with open(instances_file) as f:
            data = json.load(f)
            
        # Filter for working instances
        working_instances = []
        for url, info in data.get("instances", {}).items():
            if (info.get("version", "").startswith("2025") and  # Recent version
                info.get("http", {}).get("status_code") == 200):  # Working
                working_instances.append(url)
                
        if not working_instances:
            raise RuntimeError("No working SearXNG instances found")
            
        return working_instances

async def run_crawler(args):
    """Run the person crawler with proper configuration"""
    try:
        # Load configuration
        config = CrawlerConfig(args.config)
        
        # Initialize crawler
        crawler = AcademicPersonCrawler(
            openai_api_key=args.openai_key,
            searxng_instances=config.searxng_instances,
            openai_base=args.openai_base
        )
        
        # Filter schools by tier if specified
        schools = config.schools
        if args.tier is not None:
            schools = [s for s in schools if s.tier == args.tier]
            logging.info(f"Processing {len(schools)} schools in tier {args.tier}")
            
        # Process schools
        try:
            await crawler.crawl_schools(schools)
        finally:
            # Ensure cleanup happens
            await crawler.cleanup()
            
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

def main():
    # Configure argument parser
    parser = argparse.ArgumentParser(description='Academic Person Crawler')
    parser.add_argument('--config', type=str, required=True, help='Path to config file')
    parser.add_argument('--openai-key', type=str, default='sk-TLoMP5smKo84g0rQUg27g5eEb71rY6sVNdWH0FfneTLhyGb5', help='OpenAI API key')
    parser.add_argument('--tier', type=int, help='Only process schools in this tier')
    parser.add_argument('--openai-base', type=str, 
                       default="https://api.alibj.aiyu.fun/v1",
                       help='OpenAI API base URL')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    
    # Setup logging
    configure_logging()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Run crawler
    try:
        asyncio.run(run_crawler(args))
    except KeyboardInterrupt:
        logging.info("Crawler stopped by user")
    except Exception as e:
        logging.error(f"Crawler failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 