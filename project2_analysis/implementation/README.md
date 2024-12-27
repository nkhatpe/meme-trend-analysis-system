# Meme Trend Analysis System

## Overview

This project aims to analyze trending memes across platforms like Reddit and 4chan. We will collect meme data, analyze trends, and provide insights on popular memes.

## Features

- **Continuous Data Collection**: Memes will be collected regularly from Reddit and 4chan.
- **Meme Popularity Tracking**: Analyze the popularity of specific memes based on post volume, upvotes, and comments.
- **Cross-Platform Analysis**: Compare meme dissemination across platforms.
- **Sentiment Analysis**: Identify the sentiment and context associated with meme posts.

## How to Run

1. Clone the repository
2. Add .env file to src/ folder with following content:

   ```bash
   REDDIT_CLIENT_ID=<REDDIT_CLIENT_ID>
   REDDIT_CLIENT_SECRET=<REDDIT_CLIENT_SECRET>
   REDDIT_USER_AGENT=<REDDIT_USER_AGENT>
   MODERATE_API_KEY=<MODERATE_API_KEY>
   MONGO_URI=mongodb://localhost:27017
   FAKTORY_URL=faktory://localhost:7419
   REDDIT_CLIENT_ID=<REDDIT_CLIENT_ID>
   ```

3. Set up the MongoDB database and faktory docker containers:

   ```bash
   docker-compose up -d
   ```

4. Set up the virtual environment:

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

5. Install the required packages:

   ```bash
   pip install -r requirements.txt
   pip install motor aiohttp
   pip install --upgrade motor pymongo
   ```

6. Run each script script individually in separate terminal: (May use Screen or tmux)
   ```bash
   python3 src/enqueue_posts_jobs.py
   python3 src/enqueue_board_jobs.py
   python3 src/hate_speech_detection_job_enqueuer.py
   ```
7. Run the worker script individually in separate terminal: (May use Screen or tmux)
   ```bash
   python3 src/worker_fetch_posts.py
   python3 src/worker_fetch_boards.py
   python3 src/hate_speech_detector_worker.py
   python3 src/4chan_hate_speech.py
   ```

## Data Sources

1. **Reddit**: Using the Reddit API, we will collect memes from targeted subreddits such as /r/memes, /r/dankmemes, /r/AdviceAnimals, and more.
2. **4chan**: A custom scraper will be used to gather memes from meme-heavy boards such as /b/ and /pol/.

## Libraries and Tools

- **HTTP Requests**: Custom Python requests to make API calls to Reddit and scrape 4chan.
- **Requests/BeautifulSoup**: For scraping meme images and threads from 4chan.
- **OAuth 2.0**: For authenticating Reddit API requests manually.
- **OpenCV**: For image processing and meme recognition (e.g., detecting recurring meme templates).
- **Transformers (Hugging Face)**: For analyzing text posts related to memes, identifying context, and sentiment.
- **MongoDB**: To store and manage the collected meme data (images, text, metadata).
- **Pandas/NumPy**: For data processing and analysis.
- **Matplotlib/Plotly**: For visualizing meme trends and metrics.
- **Docker**: For containerization and deployment.
- **NewRelic**: For system monitoring and visualization.

## Team Members

- Priyanka Nandwani
- Amruta Chaudhari
- Narendra Khatpe
