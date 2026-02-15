import asyncio
import argparse
from src.config import Config
from src.crawler import AcademicCrawler
import logging
from datetime import datetime

async def main():
    parser = argparse.ArgumentParser(description='Academic POI Crawler')
    parser.add_argument('--config', type=str, required=True, help='Path to config file')
    parser.add_argument('--openai-key', type=str, required=False, default='sk-TLoMP5smKo84g0rQUg27g5eEb71rY6sVNdWH0FfneTLhyGb5', help='OpenAI API key')
    parser.add_argument('--tier', type=int, help='Only process schools in this tier')
    parser.add_argument('--openai-base', type=str, default="https://api.alibj.aiyu.fun/v1", help='OpenAI API base URL')
    parser.add_argument('--openai-timeout', type=int, default=60, help='OpenAI timeout in seconds')
    parser.add_argument('--openai-retries', type=int, default=3, help='OpenAI max retries')
    parser.add_argument('--openai-proxy', type=str, help='OpenAI proxy URL')
    args = parser.parse_args()

    # Load config
    config = Config(args.config)
    
    # Filter schools by tier if specified
    schools = config.schools
    if args.tier is not None:
        schools = [s for s in schools if s.tier == args.tier]
        logging.info(f"Processing {len(schools)} schools in tier {args.tier}")
    
    # Initialize crawler with all parameters
    crawler = AcademicCrawler(
        openai_api_key=args.openai_key,
        openai_api_base=args.openai_base,
        openai_timeout=args.openai_timeout,
        openai_max_retries=args.openai_retries,
        openai_proxy=args.openai_proxy
    )
    
    # Process schools with progress tracking
    total_schools = len(schools)
    for idx, school in enumerate(schools, 1):
        logging.info(f"Processing school {idx}/{total_schools}: {school.name}")
        try:
            people = await crawler.crawl_school(school)
            logging.info(f"Found {len(people)} people at {school.name}")
            
            # Save profiles
            for person in people:
                crawler.save_profile(person, school.code)
            
            # Save progress periodically
            if idx % 3 == 0:
                await crawler.save_progress()
            
        except Exception as e:
            logging.error(f"Failed to process {school.name}: {str(e)}")
            continue

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=f'crawler_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    )
    
    asyncio.run(main()) 