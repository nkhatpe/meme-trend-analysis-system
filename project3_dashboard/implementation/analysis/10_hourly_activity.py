from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://10.144.1.3:27017/")
db_reddit = client["crawler4Reddit"]
db_4chan = client["crawler_4chan_v2"]

# Date range
start_date = datetime(2024, 11, 1).timestamp()
end_date = datetime(2024, 11, 14).timestamp()

# Get Reddit data
reddit_data = list(
    db_reddit.comments.find(
        {"created_utc": {"$gte": start_date, "$lte": end_date}},
        {"created_utc": 1, "_id": 0},
    )
)

# Get 4chan data
chan_data = list(
    db_4chan.posts.find(
        {
            "time": {"$gte": start_date, "$lte": end_date},
            "board": "pol",
            "resto": {"$ne": 0},  # Only get replies
        },
        {"time": 1, "_id": 0},
    )
)

# Process Reddit data
reddit_df = pd.DataFrame(reddit_data)
reddit_df["datetime"] = pd.to_datetime(reddit_df["created_utc"], unit="s")
reddit_df["hour"] = reddit_df["datetime"].dt.hour
reddit_hourly = reddit_df.groupby("hour").size().reset_index(name="count")

# Process 4chan data
chan_df = pd.DataFrame(chan_data)
chan_df["datetime"] = pd.to_datetime(chan_df["time"], unit="s")
chan_df["hour"] = chan_df["datetime"].dt.hour
chan_hourly = chan_df.groupby("hour").size().reset_index(name="count")

# Create a single plot with both datasets
fig, ax = plt.subplots(figsize=(15, 8))

# Reddit hourly activity
ax.plot(
    reddit_hourly["hour"],
    reddit_hourly["count"],
    "b-",
    marker="o",
    label="Reddit r/politics",
)

# 4chan hourly activity
ax.plot(
    chan_hourly["hour"], chan_hourly["count"], "r-", marker="o", label="4chan /pol/"
)

ax.set_title("Hourly Activity Patterns (Nov 1-14, 2024)")
ax.set_xlabel("Hour (UTC)")
ax.set_ylabel("Activity Count")
ax.grid(True, alpha=0.3)
ax.set_xticks(range(24))
ax.legend()

plt.tight_layout()
plt.savefig("10_hourly_activity.png", dpi=300, bbox_inches="tight")
plt.close()

# Print summary statistics
print("\nHourly Activity Summary:")
print("\nReddit r/politics:")
print(f"Peak hour: {reddit_hourly.loc[reddit_hourly['count'].idxmax(), 'hour']} UTC")
print(f"Peak hour activity: {int(reddit_hourly['count'].max()):,} comments")
print(f"Average hourly activity: {int(reddit_hourly['count'].mean()):,} comments")

print("\n4chan /pol/:")
print(f"Peak hour: {chan_hourly.loc[chan_hourly['count'].idxmax(), 'hour']} UTC")
print(f"Peak hour activity: {int(chan_hourly['count'].max()):,} posts")
print(f"Average hourly activity: {int(chan_hourly['count'].mean()):,} posts")
