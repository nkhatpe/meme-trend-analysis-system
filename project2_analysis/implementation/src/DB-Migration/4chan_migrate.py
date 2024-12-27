from pymongo import MongoClient, IndexModel, ASCENDING, UpdateOne
import time
import logging
from datetime import datetime
from config import (
    MONGODB_URI,
    FOURCHAN_BOARDS_DB,
    FOURCHAN_BOARDS_COLLECTION,
)
from utils import setup_logger

logger = setup_logger("fourchan_migration")

# Add new collection name to config.py
FOURCHAN_POSTS_COLLECTION = "posts"

def init_mongodb():
    """Initialize MongoDB connection and create new indexes"""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[FOURCHAN_BOARDS_DB]
        threads_collection = db[FOURCHAN_BOARDS_COLLECTION]
        posts_collection = db[FOURCHAN_POSTS_COLLECTION]

        # Create indexes for the new posts collection
        post_indexes = [
            IndexModel([("board", ASCENDING), ("thread_id", ASCENDING), ("no", ASCENDING)], 
                      unique=True, name="board_thread_post"),
            IndexModel([("thread_id", ASCENDING)], name="thread_lookup"),
            IndexModel([("board", ASCENDING)], name="board_lookup"),
            IndexModel([("hate_speech_analyzed", ASCENDING)], name="hate_speech_status"),
            IndexModel([("hate_speech_enqueued_at", ASCENDING)], 
                      name="hate_speech_enqueue_time"),
            IndexModel([("created_time", ASCENDING)], name="creation_time")
        ]
        
        posts_collection.create_indexes(post_indexes)
        logger.info("Created indexes for posts collection")

        return client, db, threads_collection, posts_collection
    except Exception as e:
        logger.error(f"MongoDB initialization failed: {str(e)}")
        raise

def migrate_thread(thread, posts_collection):
    """Migrate a single thread's posts to the new collection"""
    posts_bulk = []
    thread_id = thread["thread_id"]
    board = thread["board"]
    
    for post in thread.get("posts", []):
        # Create new post document with thread reference
        post_doc = {
            "board": board,
            "thread_id": thread_id,
            "no": post["no"],
            "time": post.get("time"),
            "name": post.get("name"),
            "com": post.get("com", ""),
            "filename": post.get("filename"),
            "ext": post.get("ext"),
            "w": post.get("w"),
            "h": post.get("h"),
            "tn_w": post.get("tn_w"),
            "tn_h": post.get("tn_h"),
            "tim": post.get("tim"),
            "md5": post.get("md5"),
            "fsize": post.get("fsize"),
            "resto": post.get("resto"),
            "capcode": post.get("capcode"),
            "semantic_url": post.get("semantic_url"),
            "replies": post.get("replies"),
            "images": post.get("images"),
            "unique_ips": post.get("unique_ips"),
            "last_modified": post.get("last_modified"),
            "media_path": post.get("media_path"),
            "local_md5": post.get("local_md5"),
            "created_time": post.get("time", 0),
            "is_op": post["no"] == thread_id,
            # Add any existing hate speech analysis data
            "hate_speech_analyzed": post.get("hate_speech_analyzed"),
            "hate_speech_result": post.get("hate_speech_result"),
            "hate_speech_enqueued_at": post.get("hate_speech_enqueued_at"),
            "analysis_skipped": post.get("analysis_skipped"),
            "analysis_skipped_reason": post.get("analysis_skipped_reason"),
        }
        
        posts_bulk.append(
            UpdateOne(
                {
                    "board": board,
                    "thread_id": thread_id,
                    "no": post["no"]
                },
                {"$set": post_doc},
                upsert=True
            )
        )
    
    return posts_bulk

def migrate_threads_to_posts():
    """Main migration function"""
    client, db, threads_collection, posts_collection = init_mongodb()
    
    try:
        # Get total count for progress tracking
        total_threads = threads_collection.count_documents({})
        logger.info(f"Starting migration of {total_threads} threads")
        
        processed_threads = 0
        processed_posts = 0
        batch_size = 100
        bulk_ops = []
        
        # Process threads in batches
        for thread in threads_collection.find({}, no_cursor_timeout=True):
            try:
                # Get bulk operations for thread's posts
                thread_ops = migrate_thread(thread, posts_collection)
                bulk_ops.extend(thread_ops)
                processed_posts += len(thread_ops)
                
                # Execute bulk operations when batch size is reached
                if len(bulk_ops) >= batch_size:
                    result = posts_collection.bulk_write(bulk_ops, ordered=False)
                    logger.info(f"Inserted/updated {len(bulk_ops)} posts")
                    bulk_ops = []
                
                processed_threads += 1
                if processed_threads % 100 == 0:
                    logger.info(
                        f"Progress: {processed_threads}/{total_threads} threads, "
                        f"{processed_posts} posts processed"
                    )
                
            except Exception as e:
                logger.error(f"Error processing thread {thread.get('thread_id')}: {str(e)}")
                continue
        
        # Process any remaining bulk operations
        if bulk_ops:
            result = posts_collection.bulk_write(bulk_ops, ordered=False)
            logger.info(f"Inserted/updated final batch of {len(bulk_ops)} posts")
        
        logger.info(
            f"Migration completed: {processed_threads} threads and {processed_posts} "
            f"posts processed"
        )
        
        # Verify migration
        total_posts = posts_collection.count_documents({})
        logger.info(f"Total posts in new collection: {total_posts}")
        
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        return False
    finally:
        client.close()

def verify_migration():
    """Verify the migration was successful"""
    client, db, threads_collection, posts_collection = init_mongodb()
    
    try:
        # Count posts in original threads
        total_posts_in_threads = sum(
            len(thread.get("posts", [])) 
            for thread in threads_collection.find({})
        )
        
        # Count posts in new collection
        total_posts_migrated = posts_collection.count_documents({})
        
        logger.info(f"Posts in threads: {total_posts_in_threads}")
        logger.info(f"Posts in new collection: {total_posts_migrated}")
        
        if total_posts_in_threads == total_posts_migrated:
            logger.info("Migration verification successful!")
            return True
        else:
            logger.error("Migration verification failed: post counts don't match")
            return False
            
    except Exception as e:
        logger.error(f"Verification failed: {str(e)}")
        return False
    finally:
        client.close()

def main():
    """Run the migration"""
    try:
        logger.info("Starting 4chan database migration...")
        
        # Run migration
        if migrate_threads_to_posts():
            # Verify migration
            if verify_migration():
                logger.info("Migration and verification completed successfully")
            else:
                logger.error("Migration verification failed")
        else:
            logger.error("Migration failed")
            
    except KeyboardInterrupt:
        logger.info("Migration stopped by user")
    except Exception as e:
        logger.critical(f"Critical error during migration: {str(e)}")
        raise

if __name__ == "__main__":
    main()