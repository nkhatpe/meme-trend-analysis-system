import requests
import time
from pymongo import MongoClient, UpdateOne, ASCENDING
from pyfaktory import Client, Consumer
from ratelimit import limits, sleep_and_retry
from datetime import datetime
from config import (
    FAKTORY_URL,
    MONGODB_URI,
    MONGODB_DB,
    MONGODB_COLLECTION,
    COMMENTS_COLLECTION,
    REDDIT_USER_AGENT,
    REQUESTS_PER_MINUTE,
    COMMENT_BATCH_SIZE,
)
from utils import get_access_token, setup_logger, handle_api_response

logger = setup_logger("reddit_time_window_worker")


def init_mongodb():
    """Initialize MongoDB connection with separate collections for posts and comments"""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DB]
        posts_collection = db[MONGODB_COLLECTION]
        comments_collection = db[COMMENTS_COLLECTION]

        # Create indexes for posts
        posts_collection.create_index("id", unique=True)
        posts_collection.create_index("created")
        posts_collection.create_index("subreddit")
        posts_collection.create_index("last_updated")
        posts_collection.create_index("removed")
        posts_collection.create_index("deleted")

        # Create indexes for comments
        comments_collection.create_index("id", unique=True)
        comments_collection.create_index(
            [("post_id", ASCENDING), ("created_utc", ASCENDING)]
        )
        comments_collection.create_index("parent_id")
        comments_collection.create_index("path")
        comments_collection.create_index("last_updated")
        comments_collection.create_index("is_root")
        comments_collection.create_index("depth")

        return client, posts_collection, comments_collection
    except Exception as e:
        logger.error(f"MongoDB connection failed: {str(e)}")
        raise


ONE_MINUTE = 60


@sleep_and_retry
@limits(calls=REQUESTS_PER_MINUTE, period=ONE_MINUTE)
def limited_request(url, headers, params=None):
    """Make rate-limited requests"""
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        if response.status_code == 429:
            logger.warning("Rate limit exceeded. Sleeping for 60 seconds.")
            time.sleep(60)
            raise Exception("Rate limit exceeded")
        return response
    except requests.exceptions.Timeout:
        logger.error("Request timed out")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in request: {str(e)}")
        raise


class RedditAPI:
    def __init__(self):
        self.access_token = None
        self.token_expires = 0
        self.max_retries = 3
        self.retry_delay = 5

    def get_headers(self):
        """Get authorization headers"""
        current_time = time.time()
        if not self.access_token or current_time >= self.token_expires:
            self.access_token = get_access_token()
            if not self.access_token:
                raise Exception("Failed to obtain access token")
            self.token_expires = current_time + 3600

        return {
            "Authorization": f"bearer {self.access_token}",
            "User-Agent": REDDIT_USER_AGENT,
        }

    def fetch_posts(self, subreddit, start_timestamp, end_timestamp):
        """Fetch posts within time window"""
        url = f"https://oauth.reddit.com/r/{subreddit}/new"
        params = {"limit": 100, "raw_json": 1}
        posts = []
        after = None

        while True:
            try:
                if after:
                    params["after"] = after

                response = limited_request(url, self.get_headers(), params)
                data = handle_api_response(response, logger)

                if not data or "data" not in data or "children" not in data["data"]:
                    break

                for post in data["data"]["children"]:
                    posts.append(post["data"])

                if not data["data"]["children"] or not data["data"].get("after"):
                    break

                after = data["data"].get("after")
                time.sleep(0.01)

            except Exception as e:
                logger.error(
                    f"Error fetching posts for timeframe {start_timestamp}-{end_timestamp}: {str(e)}"
                )
                break

        return posts

    def fetch_post_by_id(self, post_id):
        """Fetch a specific post by ID"""
        url = f"https://oauth.reddit.com/by_id/t3_{post_id}"

        for attempt in range(self.max_retries):
            try:
                response = limited_request(url, self.get_headers())
                data = handle_api_response(response, logger)

                if data and "data" in data and "children" in data["data"]:
                    return (
                        data["data"]["children"][0]["data"]
                        if data["data"]["children"]
                        else None
                    )

            except Exception as e:
                logger.error(
                    f"Error fetching post {post_id} (attempt {attempt + 1}): {str(e)}"
                )
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))

        return None

    def fetch_comments(self, post_id):
        """Fetch complete comment tree"""
        url = f"https://oauth.reddit.com/comments/{post_id}"
        params = {
            "limit": 500,
            "raw_json": 1,
            "depth": 10,
            "sort": "confidence",  # Default sort method
        }

        for attempt in range(self.max_retries):
            try:
                response = limited_request(url, self.get_headers(), params)
                data = handle_api_response(response, logger)

                if data and len(data) > 1:
                    comments_data = data[1]["data"]["children"]

                    additional_comments = self.stream_more_comments(
                        post_id, comments_data
                    )
                    if additional_comments:
                        comments_data.extend(additional_comments)

                    return comments_data

            except Exception as e:
                logger.error(
                    f"Error fetching comments for {post_id} (attempt {attempt + 1}): {str(e)}"
                )
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))

        return []

    def stream_more_comments(self, post_id, comments, depth=0, max_depth=10):
        """Stream process additional comments"""
        if depth >= max_depth:
            return []

        more_comments = []
        children_ids = set()

        for comment in comments:
            if comment["kind"] == "more":
                children_ids.update(comment["data"].get("children", []))

        if not children_ids:
            return []

        batch_size = 100
        children_ids_list = list(children_ids)

        for i in range(0, len(children_ids_list), batch_size):
            batch = children_ids_list[i : i + batch_size]
            if not batch:
                continue

            try:
                url = "https://oauth.reddit.com/api/morechildren"
                params = {
                    "link_id": f"t3_{post_id}",
                    "children": ",".join(batch),
                    "api_type": "json",
                    "sort": "confidence",
                }

                response = limited_request(url, self.get_headers(), params)
                data = handle_api_response(response, logger)

                if data and "json" in data and "data" in data["json"]:
                    new_comments = data["json"]["data"]["things"]
                    more_comments.extend(new_comments)

                    additional_comments = self.stream_more_comments(
                        post_id, new_comments, depth + 1, max_depth
                    )
                    more_comments.extend(additional_comments)

            except Exception as e:
                logger.error(f"Error fetching more comments batch: {str(e)}")

            time.sleep(0.01)

        return more_comments


def process_posts_batch(posts, api, posts_collection, comments_collection):
    """Process and store posts with their comments"""
    for post_data in posts:
        try:
            post_id = post_data["id"]
            existing_post = posts_collection.find_one({"id": post_id})

            processed_post = process_post(post_data, existing_post)
            if not processed_post:
                continue

            comments = api.fetch_comments(post_id)
            comment_stats = store_comments_batch(
                post_id, comments, comments_collection, COMMENT_BATCH_SIZE
            )

            processed_post["comment_stats"] = comment_stats
            processed_post["last_updated"] = time.time()

            posts_collection.update_one(
                {"id": post_id}, {"$set": processed_post}, upsert=True
            )

            logger.info(
                f"Stored post {post_id} with {comment_stats['total_comments']} comments"
            )

        except Exception as e:
            logger.error(f"Error processing post {post_data.get('id')}: {str(e)}")


def process_post(post_data, existing_post=None):
    """Process post data"""
    try:
        current_time = time.time()

        processed_post = {
            "id": post_data["id"],
            "created": post_data.get("created", 0),
            "subreddit": post_data.get("subreddit", ""),
            "title": post_data.get("title", ""),
            "selftext": post_data.get("selftext", ""),
            "url": post_data.get("url", ""),
            "score": post_data.get("score", 0),
            "num_comments": post_data.get("num_comments", 0),
            "author": post_data.get("author", "[deleted]"),
            "permalink": post_data.get("permalink", ""),
            "upvote_ratio": post_data.get("upvote_ratio", 0.0),
            "removed": post_data.get("removed_by_category") is not None,
            "deleted": post_data.get("author") == "[deleted]",
            "last_updated": current_time,
            "domain": post_data.get("domain", ""),
            "is_self": post_data.get("is_self", False),
            "is_video": post_data.get("is_video", False),
            "over_18": post_data.get("over_18", False),
            "spoiler": post_data.get("spoiler", False),
            "stickied": post_data.get("stickied", False),
        }

        if existing_post:
            history_entry = {
                "timestamp": current_time,
                "score": processed_post["score"],
                "num_comments": processed_post["num_comments"],
                "upvote_ratio": processed_post["upvote_ratio"],
                "removed": processed_post["removed"],
                "deleted": processed_post["deleted"],
            }

            processed_post["history"] = existing_post.get("history", [])[
                -9:
            ]  # Keep last 10 entries
            processed_post["history"].append(history_entry)

            if processed_post["removed"] or processed_post["deleted"]:
                processed_post["original_selftext"] = existing_post.get(
                    "original_selftext", processed_post["selftext"]
                )
                processed_post["original_author"] = existing_post.get(
                    "original_author", processed_post["author"]
                )
        else:
            processed_post["history"] = [
                {
                    "timestamp": current_time,
                    "score": processed_post["score"],
                    "num_comments": processed_post["num_comments"],
                    "upvote_ratio": processed_post["upvote_ratio"],
                    "removed": processed_post["removed"],
                    "deleted": processed_post["deleted"],
                }
            ]
            processed_post["original_selftext"] = processed_post["selftext"]
            processed_post["original_author"] = processed_post["author"]

        return processed_post

    except Exception as e:
        logger.error(f"Error processing post data: {str(e)}")
        return None


def store_comments_batch(post_id, comments, comments_collection, batch_size):
    """Store comments in batches and calculate statistics"""
    operations = []
    stats = {
        "total_comments": 0,
        "root_comments": 0,
        "max_depth": 0,
        "deleted_comments": 0,
        "removed_comments": 0,
        "total_score": 0,
        "controversial_comments": 0,
        "last_updated": time.time(),
    }

    def process_comment_batch(batch):
        if batch:
            try:
                result = comments_collection.bulk_write(batch, ordered=False)
                logger.debug(f"Bulk write completed: {result.bulk_api_result}")
            except Exception as e:
                logger.error(f"Error in comment bulk write: {str(e)}")

    for comment in comments:
        if comment["kind"] != "t1":
            continue

        try:
            processed_comment = process_single_comment(comment["data"], post_id)
            if not processed_comment:
                continue

            stats["total_comments"] += 1
            stats["total_score"] += processed_comment["score"]

            if processed_comment["is_root"]:
                stats["root_comments"] += 1
            stats["max_depth"] = max(stats["max_depth"], processed_comment["depth"])
            if processed_comment["deleted"]:
                stats["deleted_comments"] += 1
            if processed_comment["removed"]:
                stats["removed_comments"] += 1
            if processed_comment.get("controversial", False):
                stats["controversial_comments"] += 1

            operations.append(
                UpdateOne(
                    {"id": processed_comment["id"]},
                    {"$set": processed_comment},
                    upsert=True,
                )
            )

            if len(operations) >= batch_size:
                process_comment_batch(operations)
                operations = []

        except Exception as e:
            logger.error(f"Error processing comment: {str(e)}")

    if operations:
        process_comment_batch(operations)

    if stats["total_comments"] > 0:
        stats["average_score"] = stats["total_score"] / stats["total_comments"]
        stats["deletion_rate"] = (
            stats["deleted_comments"] + stats["removed_comments"]
        ) / stats["total_comments"]

    return stats


def process_single_comment(comment_data, post_id):
    """Process a single comment"""
    try:
        current_time = time.time()

        processed_comment = {
            "id": comment_data["id"],
            "post_id": post_id,
            "parent_id": comment_data.get("parent_id"),
            "author": comment_data.get("author", "[deleted]"),
            "body": comment_data.get("body", "[deleted]"),
            "created_utc": comment_data.get("created_utc", 0),
            "score": comment_data.get("score", 0),
            "edited": bool(comment_data.get("edited", False)),
            "removed": comment_data.get("body") == "[removed]",
            "deleted": comment_data.get("author") == "[deleted]",
            "is_root": comment_data.get("parent_id", "").startswith("t3_"),
            "depth": int(comment_data.get("depth", 0)),
            "distinguished": comment_data.get("distinguished"),
            "controversiality": int(comment_data.get("controversiality", 0)),
            "last_updated": current_time,
        }

        processed_comment["controversial"] = processed_comment[
            "controversiality"
        ] > 0 or (
            processed_comment["score"] > 10
            and processed_comment.get("ups", 0) / processed_comment["score"] < 0.6
        )

        return processed_comment

    except Exception as e:
        logger.error(f"Error processing comment data: {str(e)}")
        return None


def refresh_reddit_data(job_data):
    """Process a refresh job"""
    try:
        api = RedditAPI()
        mongo_client, posts_collection, comments_collection = init_mongodb()

        subreddit = job_data["subreddit"]
        start_timestamp = job_data["start_date"]
        end_timestamp = job_data["end_date"]

        if "post_ids" in job_data:
            for post_id in job_data["post_ids"]:
                try:
                    post_data = api.fetch_post_by_id(post_id)
                    if post_data:
                        process_posts_batch(
                            [post_data], api, posts_collection, comments_collection
                        )
                except Exception as e:
                    logger.error(f"Error refreshing post {post_id}: {str(e)}")
        else:
            posts = api.fetch_posts(subreddit, start_timestamp, end_timestamp)
            if posts:
                process_posts_batch(posts, api, posts_collection, comments_collection)
                logger.info(f"Processed {len(posts)} posts for r/{subreddit}")

    except Exception as e:
        logger.error(f"Error in refresh job: {str(e)}")
        raise


def main():
    """Initialize and run the worker"""
    logger.info("Starting Reddit time-window worker...")

    consecutive_errors = 0
    max_consecutive_errors = 3
    base_sleep_time = 30

    while True:
        try:
            with Client(faktory_url=FAKTORY_URL, role="consumer") as client:
                consumer = Consumer(
                    client=client, queues=["reddit_refresh_queue2"], concurrency=1
                )
                consumer.register("refresh_reddit_data", refresh_reddit_data)
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
