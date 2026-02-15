import asyncio
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def test_browser():
    # Test with default browser settings
    config = BrowserConfig()  # Use default settings
    
    try:
        async with AsyncWebCrawler(config=config) as crawler:
            # Test a simple Google search
            result = await crawler.arun(
                url="https://www.google.com/search?q=MIT+EECS+faculty",
                config=CrawlerRunConfig(
                    wait_for="css:#search"                )
            )
            
            if result.success:
                logger.info("Browser test successful!")
                logger.info(f"Found {len(result.links.get('external', []))} external links")
                logger.info(result.links.get('external', []))
            else:
                logger.error(f"Browser test failed: {result.error_message}")
                
    except Exception as e:
        logger.error(f"Browser setup failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_browser()) 