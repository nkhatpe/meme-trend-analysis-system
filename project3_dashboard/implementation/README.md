# Meme Trend Analysis System: Interactive Dashboard for Toxicity and Engagement Insights

## Overview
We have designed the **Meme Trend Analysis System** as an interactive dashboard to explore meme toxicity, sentiment, and engagement trends across Reddit and 4chan. This system aims to provide insights into the cultural and harmful dynamics of memes through customizable queries and visualizations. Our project supports researchers and users in investigating how memes evolve, spread, and engage audiences, with a focus on toxic content.


## Features
- **Toxicity Analysis**:
  - Track and analyze trends in meme toxicity over time on Reddit and 4chan.
  - Compare toxicity levels across platforms.
- **Engagement Analysis**:
  - Study the correlation between meme toxicity and user engagement (e.g., upvotes, comments, discussions).
- **Interactive Dashboard**:
  - Filter data by date range and platform to create dynamic visualizations.
- **Temporal Trends**:
  - Detect toxicity spikes in memes during major cultural or political events.
- **Cross-Platform Comparisons**:
  - Examine how memes propagate and how trends differ between Reddit and 4chan.


## How to Run

1. Clone the repository

2. Start the Redis server:
   ```bash
   redis-server
   ```

3. Set up a virtual environment and activate it:
   ```bash
   cd research_analysis
   python3 -m venv venv
   source venv/bin/activate
   ```

4. Install the necessary dependencies:
   ```bash
   pip install -r requirements.txt
   ```

5. Start the Flask server:
   ```bash
   python app.py
   ```

6. Open the dashboard in your browser at `http://localhost:5000`.


## Usage Instructions
- Choose a date range and platform from the dashboard to view relevant data.
- Visualize trends in meme toxicity and user engagement.
- Adjust filters to refine insights and focus on specific time periods or platforms.
- Dedicated visualizations are provided for Hate Speech Analysis, Image Usage Analysis, and Platform Activity Insights.
- Additional analysis from Project 2 is included in the dashboard.


## Team Members

- Priyanka Nandwani
- Amruta Chaudhari
- Narendra Khatpe

