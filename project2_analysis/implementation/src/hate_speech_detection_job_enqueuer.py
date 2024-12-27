from pyfaktory import Client, Job, Producer
import logging
import time
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateMany
from config import (
    FAKTORY_URL,
    MONGODB_URI,
    MONGODB_DB,
    MONGODB_COLLECTION,
    COMMENTS_COLLECTION,
)
from utils import setup_logger

logger = setup_logger("hatespeech_detection_enqueuer")


class HateSpeechDetectionEnqueuer:
    def __init__(
        self, faktory_url, mongodb_uri, batch_size=100, interval=5  # 5 seconds
    ):
        self.faktory_url = faktory_url
        self.mongodb_uri = mongodb_uri
        self.batch_size = batch_size
        self.interval = interval
        self.total_jobs_enqueued = 0
        self.failed_jobs = 0

        self._init_mongodb()

    def _init_mongodb(self):
        """Initialize MongoDB connection"""
        try:
            self.mongo_client = MongoClient(self.mongodb_uri)
            self.db = self.mongo_client[MONGODB_DB]
            self.posts_collection = self.db[MONGODB_COLLECTION]
            self.comments_collection = self.db[COMMENTS_COLLECTION]
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"MongoDB connection failed: {str(e)}")
            raise

    def create_detection_job(self, content_type, content_id):
        """Create a job for hate speech detection"""
        try:
            return Job(
                jobtype="detect_hate_speech",
                args=[{"content_type": content_type, "content_id": content_id}],
                queue="hate_speech_detection_queue",
                retry=3,
                reserve_for=600,  # 10 minutes timeout
                custom={"enqueued_at": time.time(), "content_type": content_type},
            )
        except Exception as e:
            logger.error(
                f"Error creating job for {content_type} {content_id}: {str(e)}"
            )
            return None

    def get_and_mark_unanalyzed_content(self):
        """Get content that hasn't been analyzed and mark it as in progress"""
        try:
            current_time = time.time()

            # Query for unanalyzed content
            content_query = {
                "$and": [
                    {"hate_speech_analyzed": {"$exists": False}},
                    # Also check if there's no enqueued_at or if it's old (stuck jobs)
                    {
                        "$or": [
                            {"hate_speech_enqueued_at": {"$exists": False}},
                            # Consider jobs older than 1 hour as stuck
                            {"hate_speech_enqueued_at": {"$lt": current_time - 3600}},
                        ]
                    },
                ]
            }

            # Find posts and comments in one go
            posts = list(
                self.posts_collection.find(content_query, {"id": 1, "_id": 0}).limit(
                    self.batch_size
                )
            )

            comments = list(
                self.comments_collection.find(content_query, {"id": 1, "_id": 0}).limit(
                    self.batch_size
                )
            )

            # Mark posts as enqueued
            if posts:
                post_ids = [post["id"] for post in posts]
                self.posts_collection.update_many(
                    {"id": {"$in": post_ids}},
                    {
                        "$set": {
                            "hate_speech_enqueued_at": current_time,
                            "hate_speech_enqueued": True,
                        }
                    },
                )

            # Mark comments as enqueued
            if comments:
                comment_ids = [comment["id"] for comment in comments]
                self.comments_collection.update_many(
                    {"id": {"$in": comment_ids}},
                    {
                        "$set": {
                            "hate_speech_enqueued_at": current_time,
                            "hate_speech_enqueued": True,
                        }
                    },
                )

            logger.info(
                f"Found {len(posts)} unanalyzed posts and {len(comments)} unanalyzed comments"
            )
            return posts, comments

        except Exception as e:
            logger.error(f"Error getting unanalyzed content: {str(e)}")
            return [], []

    def enqueue_batch(self, producer):
        """Enqueue a batch of hate speech detection jobs"""
        try:
            posts, comments = self.get_and_mark_unanalyzed_content()
            jobs = []
            enqueued_count = 0

            # Create jobs for posts
            for post in posts:
                job = self.create_detection_job("post", post["id"])
                if job:
                    jobs.append(job)

            # Create jobs for comments
            for comment in comments:
                job = self.create_detection_job("comment", comment["id"])
                if job:
                    jobs.append(job)

            # Enqueue jobs in batches
            if jobs:
                producer.push_bulk(jobs)
                enqueued_count = len(jobs)
                logger.info(
                    f"Enqueued batch of {enqueued_count} hate speech detection jobs"
                )

            return enqueued_count

        except Exception as e:
            logger.error(f"Error enqueueing batch: {str(e)}")
            self.failed_jobs += 1
            return 0

    def run(self):
        """Main enqueuing loop"""
        logger.info("Starting hate speech detection enqueuer...")

        consecutive_errors = 0
        ERROR_THRESHOLD = 3

        while True:
            cycle_start = time.time()

            try:
                with Client(faktory_url=self.faktory_url, role="producer") as client:
                    producer = Producer(client=client)

                    enqueued = self.enqueue_batch(producer)
                    self.total_jobs_enqueued += enqueued

                    logger.info(
                        f"Enqueue cycle completed. "
                        f"Jobs enqueued this cycle: {enqueued}, "
                        f"Total jobs enqueued: {self.total_jobs_enqueued}, "
                        f"Failed jobs: {self.failed_jobs}"
                    )

                    consecutive_errors = 0

            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Error in enqueue cycle: {str(e)}")

                if consecutive_errors >= ERROR_THRESHOLD:
                    backoff_time = min(
                        300, 30 * (2 ** (consecutive_errors - ERROR_THRESHOLD))
                    )
                    logger.warning(
                        f"Multiple errors detected, backing off for {backoff_time} seconds"
                    )
                    time.sleep(backoff_time)
                    continue

            elapsed = time.time() - cycle_start
            sleep_time = max(0, self.interval - elapsed)

            if sleep_time > 0:
                logger.debug(f"Sleeping for {sleep_time:.2f} seconds until next cycle")
                time.sleep(sleep_time)


def main():
    try:
        enqueuer = HateSpeechDetectionEnqueuer(
            faktory_url=FAKTORY_URL,
            mongodb_uri=MONGODB_URI,
            batch_size=100,
            interval=5,  # 5 seconds
        )
        enqueuer.run()
    except KeyboardInterrupt:
        logger.info("Enqueuer stopped by user")
    except Exception as e:
        logger.critical(f"Critical error in enqueuer: {str(e)}")
        raise


if __name__ == "__main__":
    main()
