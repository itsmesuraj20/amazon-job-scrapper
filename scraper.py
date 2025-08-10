import os
import json
import time
import sqlite3
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import schedule
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AmazonJobsScraper:
    def __init__(self):
        # Configuration from environment variables
        self.telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not self.telegram_bot_token or not self.telegram_chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables are required")
        
        # Amazon Jobs API configuration
        self.base_api_url = "https://www.amazon.jobs/en/search.json"
        self.search_params = {
            'base_query': 'SDE 1',
            'sort': 'recent',
            'radius': '24km',
            'facets[]': 'normalized_country_code',
            'facets[]': 'normalized_state_name',
            'facets[]': 'normalized_city_name',
            'facets[]': 'location',
            'facets[]': 'business_category',
            'facets[]': 'category',
            'facets[]': 'schedule_type_id',
            'facets[]': 'employee_class',
            'facets[]': 'normalized_location',
            'facets[]': 'job_family_name',
            'offset': 0
        }
        
        # Database setup
        self.db_path = 'jobs.db'
        self.init_database()
        
        # Request session with retry logic
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def init_database(self):
        """Initialize SQLite database to store job IDs"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS seen_jobs (
                        job_id TEXT PRIMARY KEY,
                        title TEXT,
                        location TEXT,
                        posted_date TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                conn.commit()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise

    @contextmanager
    def get_db_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def is_job_seen(self, job_id: str) -> bool:
        """Check if job has been seen before"""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM seen_jobs WHERE job_id = ?", (job_id,))
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking if job seen: {e}")
            return False

    def mark_job_as_seen(self, job_data: Dict):
        """Mark job as seen in database"""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO seen_jobs (job_id, title, location, posted_date)
                    VALUES (?, ?, ?, ?)
                ''', (
                    job_data['job_id'],
                    job_data['title'],
                    job_data['location'],
                    job_data['posted_date']
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Error marking job as seen: {e}")

    def cleanup_old_jobs(self, days_old: int = 30):
        """Remove old job records from database"""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                cutoff_date = datetime.now() - timedelta(days=days_old)
                cursor.execute(
                    "DELETE FROM seen_jobs WHERE created_at < ?",
                    (cutoff_date.isoformat(),)
                )
                deleted_count = cursor.rowcount
                conn.commit()
                if deleted_count > 0:
                    logger.info(f"Cleaned up {deleted_count} old job records")
        except Exception as e:
            logger.error(f"Error cleaning up old jobs: {e}")

    def fetch_jobs(self) -> List[Dict]:
        """Fetch jobs from Amazon Jobs API"""
        try:
            logger.info("Fetching jobs from Amazon API...")
            response = self.session.get(
                self.base_api_url,
                params=self.search_params,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            jobs = data.get('jobs', [])
            logger.info(f"Fetched {len(jobs)} jobs from API")
            return jobs
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching jobs: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching jobs: {e}")
            return []

    def is_recent_job(self, posted_date_str: str) -> bool:
        """Check if job was posted in the last 24 hours"""
        try:
            # Amazon uses various date formats, try to parse
            posted_date = None
            
            # Try different date formats
            date_formats = [
                '%B %d, %Y',  # December 8, 2023
                '%b %d, %Y',   # Dec 8, 2023
                '%Y-%m-%d',    # 2023-12-08
                '%m/%d/%Y',    # 12/08/2023
            ]
            
            for date_format in date_formats:
                try:
                    posted_date = datetime.strptime(posted_date_str, date_format)
                    break
                except ValueError:
                    continue
            
            if not posted_date:
                logger.warning(f"Could not parse date: {posted_date_str}")
                return False
            
            # Check if posted within last 24 hours
            now = datetime.now()
            time_diff = now - posted_date
            return time_diff.total_seconds() <= 24 * 60 * 60
            
        except Exception as e:
            logger.error(f"Error checking job recency: {e}")
            return False

    def filter_sde1_jobs(self, jobs: List[Dict]) -> List[Dict]:
        """Filter jobs for SDE-1 roles posted in last 24 hours"""
        filtered_jobs = []
        
        for job in jobs:
            try:
                title = job.get('title', '').lower()
                job_id = job.get('id_icims', '')
                
                # Check if it's an SDE-1 role
                if not any(keyword in title for keyword in ['sde 1', 'software development engineer i', 'sde i']):
                    continue
                
                # Check if job was posted recently
                posted_date = job.get('posted_date', '')
                if not self.is_recent_job(posted_date):
                    continue
                
                # Check if we've seen this job before
                if self.is_job_seen(job_id):
                    continue
                
                # Extract relevant data
                job_data = {
                    'job_id': job_id,
                    'title': job.get('title', 'N/A'),
                    'location': job.get('location', 'N/A'),
                    'posted_date': posted_date,
                    'url': f"https://www.amazon.jobs/en/jobs/{job_id}/apply"
                }
                
                filtered_jobs.append(job_data)
                
            except Exception as e:
                logger.error(f"Error processing job: {e}")
                continue
        
        return filtered_jobs

    def send_telegram_notification(self, job_data: Dict) -> bool:
        """Send notification via Telegram"""
        try:
            message = f"""🚨 New Amazon SDE-1 Role!

Title: {job_data['title']}
Location: {job_data['location']}
Posted: {job_data['posted_date']}
Apply: {job_data['url']}

#AmazonJobs #SDE1 #SoftwareEngineer"""

            telegram_api_url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            
            payload = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'HTML',
                'disable_web_page_preview': False
            }
            
            response = requests.post(telegram_api_url, json=payload, timeout=30)
            response.raise_for_status()
            
            logger.info(f"Notification sent for job: {job_data['title']}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending Telegram notification: {e}")
            return False

    def run_scraping_cycle(self):
        """Run one complete scraping cycle"""
        try:
            logger.info("Starting scraping cycle...")
            
            # Fetch jobs from API
            jobs = self.fetch_jobs()
            if not jobs:
                logger.warning("No jobs fetched from API")
                return
            
            # Filter for SDE-1 jobs
            new_jobs = self.filter_sde1_jobs(jobs)
            
            if not new_jobs:
                logger.info("No new SDE-1 jobs found")
                return
            
            logger.info(f"Found {len(new_jobs)} new SDE-1 jobs")
            
            # Send notifications and mark jobs as seen
            for job in new_jobs:
                if self.send_telegram_notification(job):
                    self.mark_job_as_seen(job)
                    time.sleep(1)  # Rate limiting for Telegram
            
            # Cleanup old job records weekly
            if datetime.now().hour == 0 and datetime.now().minute < 15:
                self.cleanup_old_jobs()
            
            logger.info("Scraping cycle completed successfully")
            
        except Exception as e:
            logger.error(f"Error in scraping cycle: {e}")

    def test_setup(self) -> bool:
        """Test the setup by sending a test message"""
        try:
            test_message = "🧪 Amazon Jobs Scraper is now active!\n\nYou'll receive notifications for new SDE-1 roles."
            
            telegram_api_url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            payload = {
                'chat_id': self.telegram_chat_id,
                'text': test_message
            }
            
            response = requests.post(telegram_api_url, json=payload, timeout=30)
            response.raise_for_status()
            
            logger.info("Test message sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"Test setup failed: {e}")
            return False

def main():
    """Main function to run the scraper"""
    try:
        scraper = AmazonJobsScraper()
        
        # Test setup on startup
        if scraper.test_setup():
            logger.info("Setup test passed - starting scheduler")
        else:
            logger.error("Setup test failed - check configuration")
            return
        
        # Schedule the scraper to run every 15 minutes
        schedule.every(15).minutes.do(scraper.run_scraping_cycle)
        
        # Run initial scrape
        scraper.run_scraping_cycle()
        
        logger.info("Scheduler started - running every 15 minutes")
        
        # Keep the script running
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        logger.info("Scraper stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()