from pyfaktory import Client, Consumer
import requests
import time
import logging
from pymongo import MongoClient, UpdateOne
from datetime import datetime
from config import (
    FAKTORY_URL,
    MONGODB_URI,
    MONGODB_DB,
    MONGODB_COLLECTION,
    COMMENTS_COLLECTION,
    MODERATE_API_KEY,  # Add this to config.py
)
from utils import setup_logger, handle_api_response


logger = setup_logger("hatespeech_detection_worker")


# Initialize MongoDB connection
def init_mongodb():
    """Initialize MongoDB connection with collections"""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DB]
        posts_collection = db[MONGODB_COLLECTION]
        comments_collection = db[COMMENTS_COLLECTION]

        # Create new indexes for hate speech detection
        posts_collection.create_index("hate_speech_analyzed")
        comments_collection.create_index("hate_speech_analyzed")

        return client, posts_collection, comments_collection
    except Exception as e:
        logger.error(f"MongoDB connection failed: {str(e)}")
        raise


class HateSpeechDetector:
    def __init__(self, api_key):
        self.api_key = api_key
        self.api_url = "https://api.moderatehatespeech.com/api/v1/moderate/"
        self.max_retries = 3
        self.base_delay = 1

    def clean_text(self, text):
        """Clean and prepare text for API request"""
        if not text:
            return ""
        # Remove newlines and excessive spaces
        text = " ".join(text.split())
        # Escape special characters
        text = text.replace('"', '\\"').replace("\n", " ").replace("\r", "")
        return text

    def detect(self, text):
        """
        Detect hate speech in text with retry logic and exponential backoff
        """
        if not text or text in ["[deleted]", "[removed]"]:
            return None

        # Clean the text
        cleaned_text = self.clean_text(text)

        # Prepare request data
        request_data = {"token": self.api_key, "text": cleaned_text}

        headers = {"Content-Type": "application/json"}

        for attempt in range(self.max_retries):
            try:
                # Make request using json parameter instead of data
                response = requests.post(
                    self.api_url,
                    json=request_data,  # Use json parameter to handle serialization
                    headers=headers,
                    timeout=30,
                )

                # Log response for debugging
                logger.info(f"API response: {response.text}")

                if response.status_code == 429:
                    delay = (2**attempt) * self.base_delay
                    logger.warning(f"Rate limit hit, backing off for {delay} seconds")
                    time.sleep(delay)
                    continue

                try:
                    response_data = response.json()
                    logger.info(f"Parsed response data: {response_data}")
                except Exception as e:
                    logger.error(f"Failed to parse JSON response: {str(e)}")
                    logger.error(f"Raw response: {response.text}")
                    continue

                if response_data.get("response") == "Success":
                    return {
                        "class": response_data.get("class"),
                        "confidence": float(response_data.get("confidence", 0)),
                        "analyzed_at": datetime.utcnow().timestamp(),
                    }
                else:
                    logger.error(f"API Error: {response_data}")

            except requests.Timeout:
                logger.error(f"Request timeout on attempt {attempt + 1}")
            except requests.RequestException as e:
                logger.error(f"Request failed on attempt {attempt + 1}: {str(e)}")
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
                logger.error(f"Full error: {str(e)}")

            if attempt < self.max_retries - 1:
                time.sleep((2**attempt) * self.base_delay)

        return None


def process_content(content_type, content_data, detector, collections):
    """
    Process content (post or comment) for hate speech detection
    """
    posts_collection, comments_collection = collections
    collection = posts_collection if content_type == "post" else comments_collection

    try:
        # Extract text based on content type
        if content_type == "post":
            title = content_data.get("title", "")
            selftext = content_data.get("selftext", "")
            # Combine title and selftext with proper spacing
            text = f"{title} {selftext}".strip()
            content_id = content_data["id"]
        else:
            text = content_data.get("body", "")
            content_id = content_data["id"]

        # Skip already analyzed or deleted/removed content
        if not text or text in ["[deleted]", "[removed]"]:
            result = {
                "hate_speech_analyzed": True,
                "hate_speech_result": None,
                "analysis_skipped": True,
                "analysis_skipped_reason": "deleted_or_removed",
            }
        else:
            # Perform hate speech detection
            detection_result = detector.detect(text)
            result = {
                "hate_speech_analyzed": True,
                "hate_speech_result": detection_result,
                "analysis_skipped": False if detection_result else True,
                "analysis_skipped_reason": None if detection_result else "api_error",
            }

        # Update the document
        collection.update_one({"id": content_id}, {"$set": result})

        logger.info(f"Processed {content_type} {content_id} for hate speech detection")
        return True

    except Exception as e:
        logger.error(
            f"Error processing {content_type} {content_data.get('id')}: {str(e)}"
        )
        return False


def process_hate_speech_job(job_data):
    """
    Process a hate speech detection job
    """
    try:
        mongo_client, posts_collection, comments_collection = init_mongodb()
        detector = HateSpeechDetector(MODERATE_API_KEY)
        collections = (posts_collection, comments_collection)

        content_type = job_data.get("content_type")
        content_id = job_data.get("content_id")

        if not content_type or not content_id:
            logger.error("Invalid job data: missing content_type or content_id")
            return False

        # Fetch content from appropriate collection
        collection = posts_collection if content_type == "post" else comments_collection
        content_data = collection.find_one({"id": content_id})

        if not content_data:
            logger.error(f"{content_type} {content_id} not found")
            return False

        return process_content(content_type, content_data, detector, collections)

    except Exception as e:
        logger.error(f"Error in hate speech detection job: {str(e)}")
        raise


def main():
    """
    Initialize and run the hate speech detection worker
    """
    logger.info("Starting hate speech detection worker...")

    consecutive_errors = 0
    max_consecutive_errors = 3
    base_sleep_time = 30

    while True:
        try:
            with Client(faktory_url=FAKTORY_URL, role="consumer") as client:
                consumer = Consumer(
                    client=client,
                    queues=["hate_speech_detection_queue"],
                    concurrency=15,
                )
                consumer.register("detect_hate_speech", process_hate_speech_job)
                logger.info("Worker started and listening for jobs...")
                consumer.run()
                consecutive_errors = 0

        except Exception as e:
            consecutive_errors += 1
            sleep_time = base_sleep_time * (2 ** (consecutive_errors - 1))

            logger.error(f"Worker error (attempt {consecutive_errors}): {str(e)}")
            logger.info(f"Restarting worker in {sleep_time} seconds...")

            if consecutive_errors >= max_consecutive_errors:
                logger.critical("Too many consecutive errors. Exiting...")
                raise

            time.sleep(sleep_time)


if __name__ == "__main__":
    main()
