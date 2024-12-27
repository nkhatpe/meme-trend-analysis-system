from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://10.144.1.3:27017/")
db = client["crawler4Reddit"]

# Date range
start_date = datetime(2024, 11, 1)
end_date = datetime(2024, 11, 14, 23, 59, 59)

# Query posts from r/politics within date range
posts = list(
    db.posts.find(
        {
            "subreddit": "politics",
            "created": {"$gte": start_date.timestamp(), "$lte": end_date.timestamp()},
        },
        {"created": 1, "_id": 0},
    )
)

# Convert to DataFrame and process data
df = pd.DataFrame(posts)
df["date"] = pd.to_datetime(df["created"], unit="s").dt.strftime("%Y-%m-%d")
daily_counts = df.groupby("date").size().reset_index(name="count")

# Ensure all dates are present in the DataFrame
date_range = pd.date_range(start_date, end_date, freq="D").strftime("%Y-%m-%d")
daily_counts = (
    daily_counts.set_index("date").reindex(date_range, fill_value=0).reset_index()
)

# Create and save the bar chart
plt.figure(figsize=(12, 6))
plt.bar(daily_counts["index"], daily_counts["count"])
plt.title("Daily r/politics Submission Counts\n(Nov 1-14, 2024)")
plt.xlabel("Date")
plt.ylabel("Submission Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("7_daily_politics_submissions.png")
