from pymongo import MongoClient
import asyncio
import aiohttp
import time
from datetime import datetime
import logging
import os
from logging.handlers import RotatingFileHandler
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGODB_URI, MODERATE_API_KEY

# Configuration
DB = "crawler_4chan_v2"
POSTS_COLLECTION = "posts"
BATCH_SIZE = 20
SKIP_SIZE = 1000
LOG_DIR = "logs"
EMPTY_MARKERS = ["", None, "[deleted]", "[removed]"]

# Time range configuration
START_TIME = 1731128400  # 9 Nov 2024
END_TIME = 1731214800  # 10 Nov 2024

# Setup logging
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logger = logging.getLogger("4chan_hate_speech_detector")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(console_handler)

file_handler = RotatingFileHandler(
    f"{LOG_DIR}/4chan_hate_speech_{datetime.now().strftime('%Y%m%d')}.log",
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)


class AsyncHateSpeechDetector:
    def __init__(self, api_key):
        self.api_key = api_key
        self.api_url = "https://api.moderatehatespeech.com/api/v1/moderate/"
        self.max_retries = 5
        self.base_delay = 1
        self.session = None

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()

    def clean_text(self, text):
        """Clean and prepare text for API request"""
        if not text:
            return ""
        text = text.replace("&#039;", "'").replace("&quot;", '"')
        text = " ".join(text.split())
        return text

    async def detect(self, text):
        """Detect hate speech in text with retry logic"""
        if not text:
            return None

        cleaned_text = self.clean_text(text)
        request_data = {"token": self.api_key, "text": cleaned_text}
        headers = {"Content-Type": "application/json"}

        for attempt in range(self.max_retries):
            try:
                async with self.session.post(
                    self.api_url, json=request_data, headers=headers, timeout=30
                ) as response:
                    if response.status == 429:
                        delay = (2**attempt) * self.base_delay
                        logger.warning(
                            f"Rate limit hit, backing off for {delay} seconds"
                        )
                        await asyncio.sleep(delay)
                        continue

                    response_data = await response.json()

                    if response_data.get("response") == "Success":
                        return {
                            "class": response_data.get("class"),
                            "confidence": float(response_data.get("confidence", 0)),
                            "analyzed_at": datetime.utcnow().timestamp(),
                        }

            except Exception as e:
                logger.error(f"API error on attempt {attempt + 1}: {str(e)}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.base_delay)

        return None


async def process_post(detector, post, posts_collection, stats):
    """Process a single post"""
    try:
        current_time = time.time()
        update_data = {
            "hate_speech_analyzed": True,
            "hate_speech_updated_at": current_time,
        }

        # Check if post has valid content to analyze
        com = post.get("com", "")
        if not com or com in EMPTY_MARKERS:
            update_data.update(
                {
                    "analysis_skipped": True,
                    "analysis_skipped_reason": "no_content",
                    "hate_speech_result": None,
                }
            )
            stats["skipped_empty"] += 1
        else:
            result = await detector.detect(com)
            if result:
                update_data.update(
                    {"hate_speech_result": result, "analysis_skipped": False}
                )
                if result["class"] == "hate":
                    stats["total_hate_speech"] += 1
            else:
                update_data.update(
                    {
                        "analysis_skipped": True,
                        "analysis_skipped_reason": "api_error",
                        "hate_speech_result": None,
                    }
                )
                stats["api_errors"] += 1

        await posts_collection.update_one({"_id": post["_id"]}, {"$set": update_data})

        stats["total_processed"] += 1

    except Exception as e:
        logger.error(f"Error processing post {post.get('no')}: {str(e)}")


async def process_batch(detector, posts, posts_collection, stats):
    """Process a batch of posts concurrently"""
    tasks = [process_post(detector, post, posts_collection, stats) for post in posts]
    await asyncio.gather(*tasks)


async def process_posts_async():
    """Main async function to process posts"""
    try:
        client = AsyncIOMotorClient(MONGODB_URI)
        db = client[DB]
        posts_collection = db[POSTS_COLLECTION]

        # Get total count of posts in time range
        query = {
            "time": {"$gte": START_TIME, "$lte": END_TIME},
            "hate_speech_analyzed": None,
            "com": {"$ne": ""},
        }
        total_posts = await posts_collection.count_documents(query)

        logger.info(
            f"Found {total_posts:,d} posts to process in time range "
            f"{datetime.fromtimestamp(START_TIME)} to {datetime.fromtimestamp(END_TIME)}"
        )

        detector = AsyncHateSpeechDetector(MODERATE_API_KEY)
        await detector.init_session()

        stats = {
            "total_processed": 0,
            "total_hate_speech": 0,
            "skipped_empty": 0,
            "api_errors": 0,
            "start_time": time.time(),
        }

        logger.info("Starting hate speech detection process...")

        skip = 0
        while skip < total_posts:
            # Get a batch of posts using skip/limit
            batch = (
                await posts_collection.find(query)
                .sort([("time", 1), ("_id", 1)])
                .skip(skip)
                .limit(SKIP_SIZE)
                .to_list(length=SKIP_SIZE)
            )

            if not batch:
                break

            # Process the batch in smaller chunks
            for i in range(0, len(batch), BATCH_SIZE):
                chunk = batch[i : i + BATCH_SIZE]
                await process_batch(detector, chunk, posts_collection, stats)

                # Log progress
                elapsed = time.time() - stats["start_time"]
                rate = stats["total_processed"] / elapsed
                progress = (skip + i + len(chunk)) / total_posts * 100

                if stats["total_processed"] % 100 == 0:
                    logger.info(
                        f"Progress: {progress:.1f}% "
                        f"({stats['total_processed']:,d}/{total_posts:,d} posts) | "
                        f"Rate: {rate:.2f} posts/sec | "
                        f"Hate speech: {stats['total_hate_speech']:,d} | "
                        f"Skipped: {stats['skipped_empty']:,d} | "
                        f"Errors: {stats['api_errors']:,d}"
                    )

            skip += len(batch)  # Update skip based on actual batch size
            await asyncio.sleep(0.1)  # Small delay between major batches

        elapsed = time.time() - stats["start_time"]
        logger.info(
            f"\nProcessing completed in {elapsed:.2f} seconds.\n"
            f"Total processed: {stats['total_processed']:,d}/{total_posts:,d}\n"
            f"Empty/skipped: {stats['skipped_empty']:,d}\n"
            f"API errors: {stats['api_errors']:,d}\n"
            f"Hate speech detected: {stats['total_hate_speech']:,d}\n"
            f"Average rate: {stats['total_processed']/elapsed:.2f} posts/second"
        )

    except Exception as e:
        logger.error(f"Critical error: {str(e)}")
        raise
    finally:
        await detector.close()
        client.close()


def main():
    try:
        asyncio.run(process_posts_async())
    except KeyboardInterrupt:
        logger.info("Processing stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")


if __name__ == "__main__":
    main()
