from pyfaktory import Client, Job, Producer
import logging
import requests
import time
from datetime import datetime, timedelta
from pymongo import MongoClient, DESCENDING
from config import (
    FAKTORY_URL,
    MONGODB_URI,
    FOURCHAN_BOARDS_DB,
    FOURCHAN_BOARDS_COLLECTION,
    BOARDS,
)
from utils import setup_logger, handle_api_response


logger = setup_logger("fourchan_boards_enqueuer")


class FourChanBoardsEnqueuer:
    def __init__(self, faktory_url, mongodb_uri, boards, batch_size=50, interval=1800):
        """Initialize the 4chan boards enqueuer"""
        self.faktory_url = faktory_url
        self.mongodb_uri = mongodb_uri
        self.boards = boards
        self.batch_size = batch_size
        self.interval = interval
        self.total_jobs_enqueued = 0
        self.failed_jobs = 0

        self._init_mongodb()

        self.known_threads = set()
        self.last_catalog_fetch = {}

    def _init_mongodb(self, max_retries=3):
        """Initialize MongoDB connection"""
        for attempt in range(max_retries):
            try:
                self.mongo_client = MongoClient(
                    self.mongodb_uri, serverSelectionTimeoutMS=5000
                )
                self.db = self.mongo_client[FOURCHAN_BOARDS_DB]
                self.collection = self.db[FOURCHAN_BOARDS_COLLECTION]

                self.mongo_client.server_info()
                logger.info("Successfully connected to MongoDB")
                return

            except Exception as e:
                logger.error(
                    f"MongoDB connection attempt {attempt + 1} failed: {str(e)}"
                )
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                else:
                    raise Exception(
                        "Failed to connect to MongoDB after maximum retries"
                    )

    def fetch_catalog(self, board, max_retries=3):
        """Fetch board catalog"""
        url = f"https://a.4cdn.org/{board}/catalog.json"

        last_fetch = self.last_catalog_fetch.get(board, 0)
        time_since_last = time.time() - last_fetch
        if time_since_last < 1:
            time.sleep(1 - time_since_last)

        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=30)
                data = handle_api_response(
                    response, logger, f"Fetching catalog for /{board}/"
                )

                if data:
                    self.last_catalog_fetch[board] = time.time()
                    return self._extract_thread_info(data, board)

            except requests.Timeout:
                logger.error(
                    f"Timeout fetching catalog for /{board}/, attempt {attempt + 1}"
                )
            except requests.RequestException as e:
                logger.error(
                    f"Error fetching catalog for /{board}/, attempt {attempt + 1}: {str(e)}"
                )

            if attempt < max_retries - 1:
                time.sleep(2**attempt)

        return []

    def _extract_thread_info(self, catalog_data, board):
        """Extract and validate thread information from catalog data"""
        thread_info = []

        try:
            for page in catalog_data:
                if "threads" not in page:
                    continue

                for thread in page["threads"]:
                    if not isinstance(thread, dict) or "no" not in thread:
                        continue

                    thread_data = {
                        "no": thread["no"],
                        "last_modified": thread.get("last_modified", int(time.time())),
                        "replies": thread.get("replies", 0),
                        "images": thread.get("images", 0),
                        "sticky": thread.get("sticky", 0),
                        "closed": thread.get("closed", 0),
                        "archived": thread.get("archived", 0),
                        "board": board,
                    }
                    thread_info.append(thread_data)

            return thread_info

        except Exception as e:
            logger.error(f"Error extracting thread info from catalog: {str(e)}")
            return []

    def should_update_thread(self, thread):
        """Determine if a thread needs updating based on last modified time"""
        try:
            existing_thread = self.collection.find_one(
                {"board": thread["board"], "thread_id": thread["no"]}
            )
            if not existing_thread:
                return True

            if thread["last_modified"] > existing_thread.get("last_modified", 0):
                return True

            thread_age = time.time() - thread["last_modified"]

            if thread_age < 3600:  # Less than 1 hour old
                update_interval = 60  # 1 minutes
            elif thread_age < 86400:  # Less than 1 day old
                update_interval = 180  # 3 minutes
            else:
                update_interval = 300  # 5 minutes

            last_update = existing_thread.get("updated_at", 0)
            return (time.time() - last_update) >= update_interval

        except Exception as e:
            logger.error(f"Error checking thread update status: {str(e)}")
            return True  # Update on error to be safe

    def create_job(self, thread):
        """Create a job for fetching a specific thread"""
        try:
            return Job(
                jobtype="fetch_4chan_threads",
                args=[thread["board"], thread["no"]],
                queue="4chan_threads_queue",
                retry=3,
                reserve_for=900,  # 15 minutes timeout
                custom={
                    "enqueued_at": time.time(),
                    "board": thread["board"],
                    "last_modified": thread["last_modified"],
                    "replies": thread["replies"],
                    "images": thread["images"],
                },
            )
        except Exception as e:
            logger.error(f"Error creating job for thread {thread['no']}: {str(e)}")
            return None

    def enqueue_batch(self, producer):
        """Enqueue a batch of thread fetching jobs"""
        enqueued_count = 0

        for board in self.boards:
            try:
                threads = self.fetch_catalog(board)
                jobs = []

                for thread in threads:
                    if self.should_update_thread(thread):
                        job = self.create_job(thread)
                        if job:
                            jobs.append(job)

                            if len(jobs) >= self.batch_size:
                                producer.push_bulk(jobs)
                                enqueued_count += len(jobs)
                                logger.info(
                                    f"Enqueued batch of {len(jobs)} jobs for /{board}/"
                                )
                                jobs = []

                if jobs:
                    producer.push_bulk(jobs)
                    enqueued_count += len(jobs)
                    logger.info(
                        f"Enqueued final batch of {len(jobs)} jobs for /{board}/"
                    )

            except Exception as e:
                logger.error(f"Error processing board /{board}/: {str(e)}")
                self.failed_jobs += 1
                continue

        return enqueued_count

    def run(self):
        """Main enqueuing loop for monitoring 4chan boards"""
        logger.info("Starting 4chan boards enqueuer...")
        logger.info(
            f"Monitoring boards: {', '.join(['/' + b + '/' for b in self.boards])}"
        )

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
        enqueuer = FourChanBoardsEnqueuer(
            faktory_url=FAKTORY_URL,
            mongodb_uri=MONGODB_URI,
            boards=BOARDS,
            batch_size=50,
            interval=120,  # 3 minutes
        )
        enqueuer.run()
    except KeyboardInterrupt:
        logger.info("Enqueuer stopped by user")
    except Exception as e:
        logger.critical(f"Critical error in enqueuer: {str(e)}")
        raise


if __name__ == "__main__":
    main()
