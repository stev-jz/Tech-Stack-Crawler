"""
Batch Job Processor

Processes multiple job postings concurrently using asyncio.
Configurable batch size and concurrency limits to balance speed vs rate limiting.
"""
import asyncio
from typing import List, Optional
from dataclasses import dataclass
import time

from github_scraper import JobPosting, get_job_urls
from scraper import scrape_page
from parser import parse_job_text
from db import save_job_data, init_db
from job_tracker import filter_new_jobs, print_stats


@dataclass
class ProcessResult:
    """Result of processing a single job."""
    job: JobPosting
    success: bool
    error: Optional[str] = None
    parsed_data: Optional[dict] = None


class BatchProcessor:
    """
    Processes job postings in batches with configurable concurrency.
    
    Args:
        max_concurrent: Maximum number of concurrent scraping tasks (default: 5)
        delay_between_batches: Seconds to wait between batches (default: 2)
    """
    
    def __init__(self, max_concurrent: int = 5, delay_between_batches: float = 2.0):
        self.max_concurrent = max_concurrent
        self.delay_between_batches = delay_between_batches
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Stats
        self.processed = 0
        self.succeeded = 0
        self.failed = 0
    
    async def process_single_job(self, job: JobPosting) -> ProcessResult:
        """
        Process a single job posting: scrape -> parse -> save.
        Uses semaphore to limit concurrency.
        """
        async with self.semaphore:
            try:
                # 1. Scrape the job page
                html_content = await scrape_page(job.apply_url)
                
                if not html_content or len(html_content) < 500:
                    return ProcessResult(
                        job=job,
                        success=False,
                        error=f"Scraping failed or content too short ({len(html_content) if html_content else 0} chars)"
                    )
                
                # 2. Parse with Gemini
                parsed = parse_job_text(html_content)
                
                if not parsed:
                    return ProcessResult(
                        job=job,
                        success=False,
                        error="Parsing failed"
                    )
                
                # 3. Enrich with data from GitHub (in case Gemini missed it)
                if not parsed.get('job_title') or parsed.get('job_title') == 'null':
                    parsed['job_title'] = job.role
                if not parsed.get('company') or parsed.get('company') == 'null':
                    parsed['company'] = job.company
                
                # Add the URL
                parsed['url'] = job.apply_url
                parsed['location'] = job.location
                
                # 4. Save to database
                save_job_data(parsed)
                
                return ProcessResult(
                    job=job,
                    success=True,
                    parsed_data=parsed
                )
                
            except Exception as e:
                return ProcessResult(
                    job=job,
                    success=False,
                    error=str(e)
                )
    
    async def process_batch(self, jobs: List[JobPosting]) -> List[ProcessResult]:
        """
        Process a batch of jobs concurrently.
        
        Args:
            jobs: List of job postings to process
            
        Returns:
            List of ProcessResult objects
        """
        print(f"\nProcessing batch of {len(jobs)} jobs (max {self.max_concurrent} concurrent)...")
        start_time = time.time()
        
        # Create tasks for all jobs in the batch
        tasks = [self.process_single_job(job) for job in jobs]
        
        # Run all tasks concurrently (semaphore limits actual concurrency)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions that slipped through
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append(ProcessResult(
                    job=jobs[i],
                    success=False,
                    error=str(result)
                ))
            else:
                processed_results.append(result)
        
        # Update stats
        for result in processed_results:
            self.processed += 1
            if result.success:
                self.succeeded += 1
            else:
                self.failed += 1
        
        elapsed = time.time() - start_time
        print(f"Batch complete in {elapsed:.1f}s - {sum(1 for r in processed_results if r.success)}/{len(jobs)} succeeded")
        
        return processed_results
    
    async def process_all(self, jobs: List[JobPosting], batch_size: int = 10) -> List[ProcessResult]:
        """
        Process all jobs in batches.
        
        Args:
            jobs: List of all job postings
            batch_size: Number of jobs per batch
            
        Returns:
            List of all ProcessResult objects
        """
        print(f"\n{'='*60}")
        print(f"Starting batch processing of {len(jobs)} jobs")
        print(f"   Batch size: {batch_size}, Max concurrent: {self.max_concurrent}")
        print(f"{'='*60}")
        
        all_results = []
        total_batches = (len(jobs) + batch_size - 1) // batch_size
        
        for i in range(0, len(jobs), batch_size):
            batch_num = i // batch_size + 1
            batch = jobs[i:i + batch_size]
            
            print(f"\nBatch {batch_num}/{total_batches}")
            results = await self.process_batch(batch)
            all_results.extend(results)
            
            # Delay between batches (except for the last one)
            if i + batch_size < len(jobs) and self.delay_between_batches > 0:
                print(f"   Waiting {self.delay_between_batches}s before next batch...")
                await asyncio.sleep(self.delay_between_batches)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"FINAL RESULTS")
        print(f"{'='*60}")
        print(f"   Total processed: {self.processed}")
        print(f"   Succeeded: {self.succeeded}")
        print(f"   Failed: {self.failed}")
        print(f"   Success rate: {self.succeeded/self.processed*100:.1f}%" if self.processed > 0 else "N/A")
        
        return all_results


async def run_batch_pipeline(
    limit: int = None,
    batch_size: int = 10,
    max_concurrent: int = 5,
    skip_existing: bool = True
):
    """
    Main entry point for batch processing jobs from GitHub.
    
    Args:
        limit: Max number of jobs to process (None = all)
        batch_size: Jobs per batch
        max_concurrent: Max concurrent scraping tasks
        skip_existing: If True, skip jobs already in the database
    """
    # Initialize database
    init_db()
    
    # Fetch job URLs from GitHub
    print("Fetching jobs from GitHub...")
    jobs = get_job_urls(limit=limit)
    
    if not jobs:
        print("No jobs found!")
        return []
    
    # Filter out already-processed jobs
    if skip_existing:
        jobs = filter_new_jobs(jobs)
        
        if not jobs:
            print("All jobs have already been processed!")
            print_stats()
            return []
    
    # Process in batches
    processor = BatchProcessor(
        max_concurrent=max_concurrent,
        delay_between_batches=2.0
    )
    
    results = await processor.process_all(jobs, batch_size=batch_size)
    
    # Print final stats
    print_stats()
    
    return results


# Test the batch processor
if __name__ == "__main__":
    print("=" * 60)
    print("Batch Processor - Test Run")
    print("=" * 60)
    
    # Run with small limits for testing
    results = asyncio.run(run_batch_pipeline(
        limit=5,  # Only process 5 jobs for testing
        batch_size=3,  # 3 jobs per batch
        max_concurrent=2  # 2 concurrent at a time
    ))
    
    print("\nDetailed Results:")
    for r in results:
        status = "SUCCESS" if r.success else "FAILED"
        print(f"{status} {r.job.company} - {r.job.role}")
        if not r.success:
            print(f"   Error: {r.error}")
        elif r.parsed_data:
            skills = r.parsed_data.get('skills', {})
            total_skills = sum(len(v) for v in skills.values() if isinstance(v, list))
            print(f"   Extracted {total_skills} skills")
