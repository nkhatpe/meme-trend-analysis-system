import time
from pyfaktory import Client, Job, Producer
from pymongo import MongoClient
from datetime import datetime, timedelta
from config import FAKTORY_URL, MONGODB_URI, MONGODB_DB, MONGODB_COLLECTION, SUBREDDITS
from utils import setup_logger

# Configure logging
logger = setup_logger("reddit_time_window_enqueuer")


class RedditTimeWindowEnqueuer:
    def __init__(
        self,
        faktory_url,
        mongodb_uri,
        subreddits,
        start_date=datetime(2024, 11, 1),
        end_date=datetime(2024, 12, 5),
        chunk_size=10800,  # 3 hours in seconds
        refresh_interval=1800,  # 30 minutes
    ):
        self.faktory_url = faktory_url
        self.mongodb_uri = mongodb_uri
        self.subreddits = subreddits
        self.start_date = start_date
        self.end_date = min(end_date, datetime.utcnow())  # Never exceed current time
        self.chunk_size = chunk_size
        self.refresh_interval = refresh_interval
        self.total_jobs_enqueued = 0

        # Initialize MongoDB
        self._init_mongodb()

    def _init_mongodb(self):
        """Initialize MongoDB connection"""
        try:
            self.mongo_client = MongoClient(self.mongodb_uri)
            self.db = self.mongo_client[MONGODB_DB]
            self.collection = self.db[MONGODB_COLLECTION]
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"MongoDB connection failed: {str(e)}")
            raise

    def generate_time_chunks(self, start_time, end_time):
        """Generate time chunks based on chunk_size"""
        chunks = []
        current = start_time

        while current < end_time:
            chunk_end = min(current + timedelta(seconds=self.chunk_size), end_time)
            chunks.append((current, chunk_end))
            current = chunk_end

        return chunks

    def get_posts_needing_refresh(self, subreddit):
        """Get existing posts that need refreshing"""
        try:
            current_time = time.time()
            cutoff_time = current_time - self.refresh_interval

            # Find posts within date range that need refresh
            query = {
                "subreddit": subreddit,
                "created": {
                    "$gte": self.start_date.timestamp(),
                    "$lte": self.end_date.timestamp(),
                },
                "$or": [
                    {"last_updated": {"$lt": cutoff_time}},
                    {"last_updated": {"$exists": False}},
                ],
            }

            posts = self.collection.find(query, {"id": 1, "created": 1})
            return list(posts)

        except Exception as e:
            logger.error(f"Error getting posts for refresh: {str(e)}")
            return []

    def create_chunk_job(self, subreddit, start_time, end_time):
        """Create a job for a specific time chunk"""
        try:
            # Convert datetime to timestamp for the worker
            job_data = {
                "subreddit": subreddit,
                "start_date": start_time.timestamp(),
                "end_date": end_time.timestamp(),
            }

            return Job(
                jobtype="refresh_reddit_data",
                args=[job_data],
                queue="reddit_refresh_queue2",
                retry=3,
                reserve_for=1800,
                custom={
                    "enqueued_at": time.time(),
                    "subreddit": subreddit,
                    "chunk_start": start_time.isoformat(),
                    "chunk_end": end_time.isoformat(),
                },
            )
        except Exception as e:
            logger.error(f"Error creating chunk job: {str(e)}")
            return None

    def create_refresh_job(self, subreddit, post_ids):
        """Create a job for refreshing specific posts"""
        try:
            job_data = {
                "subreddit": subreddit,
                "post_ids": post_ids,
                "start_date": self.start_date.timestamp(),
                "end_date": self.end_date.timestamp(),
            }

            return Job(
                jobtype="refresh_reddit_data",
                args=[job_data],
                queue="reddit_refresh_queue2",
                retry=3,
                reserve_for=1800,
                custom={
                    "enqueued_at": time.time(),
                    "subreddit": subreddit,
                    "job_type": "post_refresh",
                    "post_count": len(post_ids),
                },
            )
        except Exception as e:
            logger.error(f"Error creating refresh job: {str(e)}")
            return None

    def enqueue_jobs(self, producer):
        """Enqueue chunk jobs & refresh jobs"""
        enqueued_count = 0

        try:
            for subreddit in self.subreddits:
                time_chunks = self.generate_time_chunks(self.start_date, self.end_date)

                for chunk_start, chunk_end in time_chunks:
                    job = self.create_chunk_job(subreddit, chunk_start, chunk_end)
                    if job:
                        producer.push(job)
                        enqueued_count += 1
                        logger.info(
                            f"Enqueued chunk job for r/{subreddit}: "
                            f"{chunk_start.strftime('%Y-%m-%d %H:%M')} to "
                            f"{chunk_end.strftime('%Y-%m-%d %H:%M')}"
                        )

                posts = self.get_posts_needing_refresh(subreddit)
                if posts:
                    batch_size = 100
                    for i in range(0, len(posts), batch_size):
                        batch = posts[i : i + batch_size]
                        post_ids = [post["id"] for post in batch]

                        job = self.create_refresh_job(subreddit, post_ids)
                        if job:
                            producer.push(job)
                            enqueued_count += 1
                            logger.info(
                                f"Enqueued refresh job for r/{subreddit}: "
                                f"{len(post_ids)} posts"
                            )

            return enqueued_count

        except Exception as e:
            logger.error(f"Error enqueueing jobs: {str(e)}")
            return enqueued_count

    def run(self):
        """Main enqueuing loop"""
        logger.info(f"Starting Reddit time-window enqueuer...")
        logger.info(f"Collection window: {self.start_date} to {self.end_date}")
        logger.info(f"Chunk size: {self.chunk_size} seconds")
        logger.info(f"Refresh interval: {self.refresh_interval} seconds")
        logger.info(
            f"Monitoring subreddits: {', '.join(['r/' + s for s in self.subreddits])}"
        )

        while True:
            cycle_start = time.time()

            try:
                with Client(faktory_url=self.faktory_url, role="producer") as client:
                    producer = Producer(client=client)

                    # Enqueue all jobs
                    enqueued = self.enqueue_jobs(producer)
                    self.total_jobs_enqueued += enqueued

                    logger.info(
                        f"Enqueue cycle completed. "
                        f"Jobs enqueued this cycle: {enqueued}, "
                        f"Total jobs enqueued: {self.total_jobs_enqueued}"
                    )

            except Exception as e:
                logger.error(f"Error in enqueue cycle: {str(e)}")
                time.sleep(30)
                continue

            elapsed = time.time() - cycle_start
            sleep_time = max(0, self.refresh_interval - elapsed)

            if sleep_time > 0:
                logger.debug(f"Sleeping for {sleep_time:.2f} seconds until next cycle")
                time.sleep(sleep_time)


def main():
    """Initialize and run the enqueuer"""
    try:
        current_time = datetime.utcnow().replace(microsecond=0)

        end_date = min(datetime(2024, 12, 5, 23, 59, 59), current_time)

        enqueuer = RedditTimeWindowEnqueuer(
            faktory_url=FAKTORY_URL,
            mongodb_uri=MONGODB_URI,
            subreddits=SUBREDDITS,
            start_date=datetime(2024, 11, 1),
            end_date=end_date,
            chunk_size=10800,  # 3 hours in seconds
            refresh_interval=1800,  # 30 minutes
        )
        enqueuer.run()
    except KeyboardInterrupt:
        logger.info("Enqueuer stopped by user")
    except Exception as e:
        logger.critical(f"Critical error in enqueuer: {str(e)}")
        raise


if __name__ == "__main__":
    main()
