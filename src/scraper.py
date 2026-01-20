import asyncio
import os
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

async def scrape_page(url):
    print(f"Scraping: {url}")
    
    # Configure browser for better JavaScript handling
    browser_config = BrowserConfig(
        headless=True,
        verbose=True,
        extra_args=["--disable-blink-features=AutomationControlled"]  # Avoid detection
    )
    
    # Configure the crawl
    crawl_config = CrawlerRunConfig(
        wait_until="networkidle",  # Wait until network is idle
        page_timeout=30000,  # 30 second timeout
        delay_before_return_html=5.0,  # Wait 5 seconds for JS to fully render
        js_code=[
            "window.scrollTo(0, document.body.scrollHeight);",  # Scroll to bottom
            "await new Promise(resolve => setTimeout(resolve, 2000));"  # Wait 2 more seconds
        ]
    )
    
    # Initialize crawler
    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(url=url, config=crawl_config)
        
        # We want the "markdown" version because it's easier for AI to read later
        return result.markdown

def save_raw_data(filename, data):
    # Ensure the directory exists
    os.makedirs("data/raw", exist_ok=True)
    
    filepath = os.path.join("data/raw", filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(data)
    print(f"Saved raw data to: {filepath}")

async def main():
    # TEST URL: We'll test with a real YC startup job or a stable page
    # You can change this URL to any specific job posting later
    target_url = "https://www.ziprecruiter.com/c/Td/Job/Mobile-Software-Engineer-Intern-Co-op-%28Summer-2026%29/-in-Toronto,ON?jid=eaf44a4f0eae5b44&utm_campaign=google_jobs_apply&utm_source=google_jobs_apply&utm_medium=organic" 
    
    raw_text = await scrape_page(target_url)
    
    # Save it with a timestamp or ID (using 'test_run' for now)
    save_raw_data("TD_test_posting.md", raw_text)

if __name__ == "__main__":
    asyncio.run(main())