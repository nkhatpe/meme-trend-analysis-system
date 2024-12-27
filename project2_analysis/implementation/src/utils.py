import requests
import base64
import logging
import logging.handlers
from datetime import datetime
from config import REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT, LOG_DIR


def setup_logger(name):
    """Setup logger with both file and console handlers"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Create formatters
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File Handler with rotation
    log_file = f"{LOG_DIR}/{name}_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5  # 10MB
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def get_access_token():
    """Obtain OAuth2 access token for Reddit API with proper error handling"""
    logger = logging.getLogger("reddit_auth")

    try:
        auth_url = "https://www.reddit.com/api/v1/access_token"
        auth_header = f"{REDDIT_CLIENT_ID}:{REDDIT_CLIENT_SECRET}"
        auth_header_encoded = base64.b64encode(auth_header.encode()).decode()

        headers = {
            "Authorization": f"Basic {auth_header_encoded}",
            "User-Agent": REDDIT_USER_AGENT,
        }

        data = {"grant_type": "client_credentials"}

        response = requests.post(auth_url, headers=headers, data=data, timeout=10)
        response.raise_for_status()

        response_data = response.json()

        if "access_token" in response_data:
            logger.info("Successfully obtained access token")
            return response_data["access_token"]
        else:
            logger.error(f"No access token in response: {response_data}")
            return None

    except requests.exceptions.Timeout:
        logger.error("Timeout while obtaining access token")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error obtaining access token: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while obtaining access token: {str(e)}")
        return None


# Helper function to handle API responses
def handle_api_response(response, logger, context="API"):
    """Generic function to handle API responses with proper logging"""
    try:
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f"{context} HTTP Error: {str(e)}")
        if response.status_code == 429:
            logger.warning(f"{context} Rate limit exceeded")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"{context} Request Exception: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"{context} Unexpected error: {str(e)}")
        return None
