# Meme Trend Analysis System

## Overview
This project aims to analyze trending memes across platforms like Reddit and 4chan. We will collect meme data, analyze trends, and provide insights on popular memes.

## Features
- **Continuous Data Collection**: Memes will be collected regularly from Reddit and 4chan.
- **Meme Popularity Tracking**: Analyze the popularity of specific memes based on post volume, upvotes, and comments.
- **Cross-Platform Analysis**: Compare meme dissemination across platforms.
- **Sentiment Analysis**: Identify the sentiment and context associated with meme posts.

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
- **Jenkins**: For continuous integration and deployment (CI/CD).
- **Grafana**: For system monitoring and visualization.


## Team Members
- Priyanka Nandwani
- Amruta Chaudhari
- Narendra Khatpe