import sys
import asyncio
from scraper import scrape_page
from parser import parse_job_text
from db import save_job_data, init_db

# Hardcoded test URL  
# Testing with another real posting
TEST_URL = "https://td.wd3.myworkdayjobs.com/TD_Bank_Careers/job/Toronto-Ontario/Software-Engineer-Intern-Co-op_R_1467905?utm_source=Simplify&ref=Simplify"

async def main():
    print(f"Starting pipeline for: {TEST_URL}")

    # 1. Scrape
    print("Scraping page...")
    html_content = await scrape_page(TEST_URL)
    if not html_content:
        print("Scraping failed.")
        return
    
    print(f"✓ Scraped {len(html_content)} characters")

    # 2. Parse
    print("Parsing with Gemini...")
    job_data = parse_job_text(html_content)
    if not job_data:
        print("Parsing failed.")
        return
    
    print(f"✓ Extracted: {job_data.get('job_title')} at {job_data.get('company')}")
    
    # Add the URL to the data (Gemini doesn't know the URL)
    job_data['url'] = TEST_URL

    # 3. Save
    print("💾 Saving to PostgreSQL...")
    save_job_data(job_data)
    
    print("Pipeline finished! Check your database.")

if __name__ == "__main__":
    # Ensure DB tables exist before we start
    init_db()
    asyncio.run(main())