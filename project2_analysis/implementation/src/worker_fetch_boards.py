from pyfaktory import Client, Consumer
from config import (
    FAKTORY_URL,
    MONGODB_URI,
    MEDIA_DIR,
)
import requests
import os
import hashlib
import time
from datetime import datetime
import logging
from pymongo import MongoClient, errors, UpdateOne
from utils import setup_logger, handle_api_response

# New database and collection names
FOURCHAN_DB = "crawler_4chan_v2"
THREADS_COLLECTION = "threads"
POSTS_COLLECTION = "posts"

logger = setup_logger("fourchan_boards_worker")


def init_mongodb(max_retries=3):
    """Initialize MongoDB connection with retry logic"""
    for attempt in range(max_retries):
        try:
            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            client.server_info()
            db = client[FOURCHAN_DB]
            threads_collection = db[THREADS_COLLECTION]
            posts_collection = db[POSTS_COLLECTION]

            # Create indexes if they don't exist
            threads_collection.create_index(
                [("board", 1), ("thread_id", 1)], unique=True
            )
            threads_collection.create_index("last_modified")
            threads_collection.create_index("archived")
            threads_collection.create_index([("board", 1)])

            posts_collection.create_index(
                [("board", 1), ("thread_id", 1), ("no", 1)], unique=True
            )
            posts_collection.create_index([("thread_id", 1)])
            posts_collection.create_index([("board", 1)])
            posts_collection.create_index([("no", 1)])
            posts_collection.create_index([("hate_speech_analyzed", 1)])
            posts_collection.create_index([("hate_speech_enqueued_at", 1)])

            logger.info("Successfully connected to MongoDB")
            return client, threads_collection, posts_collection
        except errors.ServerSelectionTimeoutError:
            logger.error(
                f"MongoDB connection attempt {attempt + 1} failed. Retrying..."
            )
            time.sleep(5)

    logger.critical("Failed to connect to MongoDB after maximum retries")
    raise Exception("MongoDB connection failed")


mongo_client, threads_collection, posts_collection = init_mongodb()


def fetch_thread(board, thread_id, max_retries=3):
    """Fetch a specific thread with retry logic"""
    url = f"https://a.4cdn.org/{board}/thread/{thread_id}.json"

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            data = handle_api_response(response, logger, f"Fetching thread {thread_id}")

            if data and "posts" in data:
                return data

        except requests.Timeout:
            logger.error(f"Timeout fetching thread {thread_id}, attempt {attempt + 1}")
        except requests.RequestException as e:
            logger.error(
                f"Error fetching thread {thread_id}, attempt {attempt + 1}: {str(e)}"
            )

        if attempt < max_retries - 1:
            time.sleep(2**attempt)

    return None


def ensure_media_path(board, thread_id):
    """Create media directory structure if it doesn't exist"""
    try:
        media_path = os.path.join(MEDIA_DIR, board, str(thread_id))
        os.makedirs(media_path, exist_ok=True)
        return media_path
    except Exception as e:
        logger.error(f"Error creating media directory: {str(e)}")
        return None


def download_media(board, thread_id, filename, ext, max_retries=3):
    """Download media file"""
    if ext.lower() not in [".jpg", ".jpeg", ".png"]:
        logger.info(f"Skipping non-JPG/PNG file: {filename}")
        return None

    media_url = f"https://i.4cdn.org/{board}/{filename}"
    media_path = ensure_media_path(board, thread_id)

    if not media_path:
        return None

    file_path = os.path.join(media_path, filename)

    if os.path.exists(file_path):
        return file_path

    for attempt in range(max_retries):
        try:
            response = requests.get(media_url, timeout=30, stream=True)
            if response.status_code == 200:
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"Downloaded media: {filename}")
                return file_path

        except Exception as e:
            logger.error(
                f"Error downloading media {filename}, attempt {attempt + 1}: {str(e)}"
            )

        if attempt < max_retries - 1:
            time.sleep(2**attempt)

    return None


def calculate_file_hash(file_path):
    """Calculate MD5 hash of downloaded file"""
    try:
        md5_hash = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
    except Exception as e:
        logger.error(f"Error calculating file hash: {str(e)}")
        return None


def process_post(post, board, thread_id):
    """Process a single post with media handling"""
    processed_post = {
        "board": board,
        "thread_id": thread_id,
        "no": post.get("no"),
        "time": post.get("time"),
        "name": post.get("name", "Anonymous"),
        "com": post.get("com", ""),
        "filename": post.get("filename", ""),
        "ext": post.get("ext", ""),
        "w": post.get("w"),
        "h": post.get("h"),
        "tn_w": post.get("tn_w"),
        "tn_h": post.get("tn_h"),
        "tim": post.get("tim"),
        "md5": post.get("md5"),
        "fsize": post.get("fsize"),
        "resto": post.get("resto", 0),
        "capcode": post.get("capcode", ""),
        "semantic_url": post.get("semantic_url", ""),
        "replies": post.get("replies", 0),
        "images": post.get("images", 0),
        "unique_ips": post.get("unique_ips", 0),
        "last_modified": post.get("last_modified", int(time.time())),
    }

    if post.get("tim") and post.get("ext"):
        filename = f"{post['tim']}{post['ext']}"
        media_path = download_media(board, thread_id, filename, post["ext"])

        if media_path:
            processed_post["media_path"] = media_path
            processed_post["local_md5"] = calculate_file_hash(media_path)

    # Keep existing hate speech analysis fields if they exist
    existing_post = posts_collection.find_one(
        {"board": board, "thread_id": thread_id, "no": post.get("no")}
    )

    if existing_post:
        for field in [
            "hate_speech_analyzed",
            "hate_speech_result",
            "hate_speech_updated_at",
        ]:
            if field in existing_post:
                processed_post[field] = existing_post[field]

    return processed_post


def is_thread_archived(last_modified):
    """Check if thread should be considered archived"""
    ARCHIVE_THRESHOLD = 48 * 60 * 60  # 48 hours
    return (time.time() - last_modified) > ARCHIVE_THRESHOLD


def process_thread(board, thread_id):
    """Process a single thread with improved error handling"""
    logger.info(f"Processing thread {thread_id} from /{board}/")

    try:
        thread_data = fetch_thread(board, thread_id)
        if not thread_data:
            logger.error(f"Failed to fetch thread {thread_id}")
            return

        posts = thread_data.get("posts", [])
        if not posts:
            logger.warning(f"No posts in thread {thread_id}")
            return

        processed_posts = []
        posts_operations = []

        for post in posts:
            try:
                processed_post = process_post(post, board, thread_id)
                if processed_post:
                    processed_posts.append(processed_post)
                    posts_operations.append(
                        UpdateOne(
                            {
                                "board": board,
                                "thread_id": thread_id,
                                "no": processed_post["no"],
                            },
                            {"$set": processed_post},
                            upsert=True,
                        )
                    )
            except Exception as e:
                logger.error(f"Error processing post in thread {thread_id}: {str(e)}")

        if not processed_posts:
            logger.error(f"No valid posts processed for thread {thread_id}")
            return

        # Get OP post
        op_post = processed_posts[0]

        # Update thread document
        thread_document = {
            "board": board,
            "thread_id": thread_id,
            "subject": op_post.get("sub", ""),
            "created_time": op_post.get("time"),
            "last_modified": max(post.get("time", 0) for post in processed_posts),
            "reply_count": len(processed_posts) - 1,  # Excluding OP
            "image_count": sum(1 for post in processed_posts if "media_path" in post),
            "archived": is_thread_archived(op_post.get("time", 0)),
            "sticky": bool(op_post.get("sticky")),
            "closed": bool(op_post.get("closed")),
            "updated_at": int(time.time()),
        }

        try:
            # Update thread document
            threads_collection.update_one(
                {"board": board, "thread_id": thread_id},
                {"$set": thread_document},
                upsert=True,
            )

            # Bulk update posts
            if posts_operations:
                posts_collection.bulk_write(posts_operations, ordered=False)

            logger.info(
                f"Successfully updated thread {thread_id} with {len(processed_posts)} posts"
            )

        except Exception as e:
            logger.error(f"Database error for thread {thread_id}: {str(e)}")

    except Exception as e:
        logger.error(f"Error processing thread {thread_id}: {str(e)}")


def main():
    """Main worker function"""
    logger.info("Starting 4chan boards worker...")

    os.makedirs(MEDIA_DIR, exist_ok=True)

    while True:
        try:
            with Client(faktory_url=FAKTORY_URL, role="consumer") as client:
                consumer = Consumer(
                    client=client, queues=["4chan_threads_queue"], concurrency=10
                )
                consumer.register("fetch_4chan_threads", process_thread)
                logger.info("Worker started and listening for jobs...")
                consumer.run()
        except Exception as e:
            logger.error(f"Worker error: {str(e)}")
            logger.info("Restarting worker in 30 seconds...")
            time.sleep(30)


if __name__ == "__main__":
    main()
