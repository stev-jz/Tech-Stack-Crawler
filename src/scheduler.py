"""
Job Scheduler

Automatically runs the job scraping pipeline at configurable intervals.
Can run as:
1. A long-running daemon process
2. A one-time run (for cron jobs)
"""
import asyncio
import signal
import sys
from datetime import datetime, timedelta
from typing import Optional

from batch_processor import run_batch_pipeline
from job_tracker import print_stats
from db import init_db, clear_failed_urls, get_failed_urls


class JobScheduler:
    """
    Scheduler that runs the job scraping pipeline at regular intervals.
    
    Args:
        interval_hours: Hours between runs (default: 24)
        batch_size: Jobs per batch (default: 10)
        max_concurrent: Max concurrent scraping tasks (default: 5)
        max_jobs_per_run: Max jobs to process per run (None = all new jobs)
        skip_failed: If True, skip URLs that previously failed (default: True)
    """
    
    def __init__(
        self,
        interval_hours: float = 24,
        batch_size: int = 10,
        max_concurrent: int = 5,
        max_jobs_per_run: Optional[int] = None,
        skip_failed: bool = True
    ):
        self.interval_hours = interval_hours
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent
        self.max_jobs_per_run = max_jobs_per_run
        self.skip_failed = skip_failed
        
        self._running = False
        self._next_run: Optional[datetime] = None
    
    def _log(self, message: str):
        """Log a message with timestamp."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")
    
    async def run_once(self) -> dict:
        """
        Run the pipeline once.
        
        Returns:
            Dict with run statistics
        """
        self._log("Starting scheduled job run...")
        start_time = datetime.now()
        
        try:
            results = await run_batch_pipeline(
                limit=self.max_jobs_per_run,
                batch_size=self.batch_size,
                max_concurrent=self.max_concurrent,
                skip_existing=True,
                skip_failed=self.skip_failed
            )
            
            elapsed = (datetime.now() - start_time).total_seconds()
            
            stats = {
                'success': True,
                'jobs_processed': len(results) if results else 0,
                'jobs_succeeded': sum(1 for r in results if r.success) if results else 0,
                'elapsed_seconds': elapsed,
                'timestamp': start_time.isoformat()
            }
            
            self._log(f"Run complete: {stats['jobs_succeeded']}/{stats['jobs_processed']} jobs in {elapsed:.1f}s")
            
            return stats
            
        except Exception as e:
            self._log(f"Run failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': start_time.isoformat()
            }
    
    async def run_daemon(self):
        """
        Run as a daemon, executing the pipeline at regular intervals.
        Handles graceful shutdown on SIGINT/SIGTERM.
        """
        self._running = True
        
        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            self._log("Shutdown signal received, stopping after current run...")
            self._running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        self._log("=" * 60)
        self._log("JOB SCHEDULER DAEMON STARTED")
        self._log(f"   Interval: every {self.interval_hours} hours")
        self._log(f"   Batch size: {self.batch_size}")
        self._log(f"   Max concurrent: {self.max_concurrent}")
        self._log(f"   Max jobs/run: {self.max_jobs_per_run or 'unlimited'}")
        self._log("=" * 60)
        
        run_count = 0
        
        while self._running:
            run_count += 1
            self._log(f"\nRun #{run_count}")
            
            # Run the pipeline
            await self.run_once()
            
            if not self._running:
                break
            
            # Calculate next run time
            self._next_run = datetime.now() + timedelta(hours=self.interval_hours)
            self._log(f"Next run scheduled for: {self._next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Sleep until next run (check every minute for shutdown signal)
            sleep_seconds = self.interval_hours * 3600
            while sleep_seconds > 0 and self._running:
                await asyncio.sleep(min(60, sleep_seconds))
                sleep_seconds -= 60
        
        self._log("Scheduler stopped gracefully")


async def run_scheduled_pipeline(
    interval_hours: float = 24,
    batch_size: int = 10,
    max_concurrent: int = 5,
    max_jobs_per_run: Optional[int] = None,
    daemon: bool = False,
    retry_failed: bool = False
):
    """
    Main entry point for scheduled job processing.
    
    Args:
        interval_hours: Hours between runs
        batch_size: Jobs per batch
        max_concurrent: Max concurrent tasks
        max_jobs_per_run: Max jobs per run
        daemon: If True, run continuously; if False, run once
        retry_failed: If True, retry previously failed URLs
    """
    # Initialize database
    init_db()
    
    scheduler = JobScheduler(
        interval_hours=interval_hours,
        batch_size=batch_size,
        max_concurrent=max_concurrent,
        max_jobs_per_run=max_jobs_per_run,
        skip_failed=not retry_failed  # If retry_failed=True, skip_failed=False
    )
    
    if daemon:
        await scheduler.run_daemon()
    else:
        await scheduler.run_once()


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Job Scheduler - Automatically scrape jobs at regular intervals"
    )
    parser.add_argument(
        '--daemon', '-d',
        action='store_true',
        help='Run as a daemon (continuous mode)'
    )
    parser.add_argument(
        '--interval', '-i',
        type=float,
        default=24,
        help='Hours between runs (default: 24)'
    )
    parser.add_argument(
        '--batch-size', '-b',
        type=int,
        default=10,
        help='Jobs per batch (default: 10)'
    )
    parser.add_argument(
        '--max-concurrent', '-c',
        type=int,
        default=5,
        help='Max concurrent scraping tasks (default: 5)'
    )
    parser.add_argument(
        '--max-jobs', '-m',
        type=int,
        default=None,
        help='Max jobs to process per run (default: unlimited)'
    )
    parser.add_argument(
        '--stats', '-s',
        action='store_true',
        help='Just print stats and exit'
    )
    parser.add_argument(
        '--retry-failed',
        action='store_true',
        help='Include previously failed URLs (retry them)'
    )
    parser.add_argument(
        '--clear-failed',
        action='store_true',
        help='Clear all failed URLs from the database and exit'
    )
    parser.add_argument(
        '--show-failed',
        action='store_true',
        help='Show count of failed URLs and exit'
    )
    
    args = parser.parse_args()
    
    if args.clear_failed:
        init_db()
        clear_failed_urls()
        return
    
    if args.show_failed:
        init_db()
        failed = get_failed_urls()
        print(f"Failed URLs: {len(failed)}")
        return
    
    if args.stats:
        init_db()
        print_stats()
        return
    
    asyncio.run(run_scheduled_pipeline(
        interval_hours=args.interval,
        batch_size=args.batch_size,
        max_concurrent=args.max_concurrent,
        max_jobs_per_run=args.max_jobs,
        daemon=args.daemon,
        retry_failed=args.retry_failed
    ))


if __name__ == "__main__":
    main()
