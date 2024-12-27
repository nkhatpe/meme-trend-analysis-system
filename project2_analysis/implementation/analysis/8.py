from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["crawler4Reddit"]

# Date range
start_date = datetime(2024, 11, 1)
end_date = datetime(2024, 11, 14, 23, 59, 59)

# Query comments from r/politics posts within date range
comments = list(
    db.comments.aggregate(
        [
            {
                "$lookup": {
                    "from": "posts",
                    "localField": "post_id",
                    "foreignField": "id",
                    "as": "post",
                }
            },
            {
                "$match": {
                    "post.subreddit": "politics",
                    "created_utc": {
                        "$gte": start_date.timestamp(),
                        "$lte": end_date.timestamp(),
                    },
                }
            },
            {"$project": {"created_utc": 1, "_id": 0}},
        ]
    )
)

# Convert to DataFrame and process data
df = pd.DataFrame(comments)
df["datetime"] = pd.to_datetime(df["created_utc"], unit="s")
df["hour"] = df["datetime"].dt.strftime("%Y-%m-%d %H:00")
hourly_counts = df.groupby("hour").size().reset_index(name="count")

# Create and save the line plot
plt.figure(figsize=(15, 6))
plt.plot(hourly_counts["hour"], hourly_counts["count"])
plt.title("Hourly r/politics Comment Counts\n(Nov 1-14, 2024)")
plt.xlabel("Date/Hour (UTC)")
plt.ylabel("Comment Count")
plt.xticks(hourly_counts.index[::12], hourly_counts["hour"][::12], rotation=45)
plt.tight_layout()
plt.savefig("8_hourly_politics_comments.png")
