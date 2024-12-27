import os
from dotenv import load_dotenv

load_dotenv()

# Reddit API credentials
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')
REDDIT_REDIRECT_URI = os.getenv('REDDIT_REDIRECT_URI')

# Subreddits to monitor
SUBREDDITS = ['politics', 'memes', 'dankmemes']

# Faktory configuration
FAKTORY_URL = os.getenv('FAKTORY_URL')

# MongoDB configuration
MONGODB_URI = os.getenv('MONGODB_URI')
MONGODB_DB = 'crawler4Reddit'
MONGODB_COLLECTION = 'posts'
COMMENTS_COLLECTION = 'comments'
COMMENT_BATCH_SIZE = 100

# Rate limiting
REQUESTS_PER_MINUTE = 99
REQUESTS_PER_MINUTE_COMMENTS = 90

# 4chan configuration
BOARDS = ['pol', 'b']
MEDIA_DIR = '4chan_media'

FOURCHAN_BOARDS_DB = 'crawler_4chan'
FOURCHAN_BOARDS_COLLECTION = 'threads'

# Logging
LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)