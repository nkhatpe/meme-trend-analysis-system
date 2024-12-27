from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
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
reddit_df["day_type"] = reddit_df["datetime"].dt.dayofweek.apply(
    lambda x: "Weekend" if x >= 5 else "Weekday"
)
reddit_df["date"] = reddit_df["datetime"].dt.strftime("%Y-%m-%d")

# Process 4chan data
chan_df = pd.DataFrame(chan_data)
chan_df["datetime"] = pd.to_datetime(chan_df["time"], unit="s")
chan_df["day_type"] = chan_df["datetime"].dt.dayofweek.apply(
    lambda x: "Weekend" if x >= 5 else "Weekday"
)
chan_df["date"] = chan_df["datetime"].dt.strftime("%Y-%m-%d")

# Calculate daily averages
reddit_daily = reddit_df.groupby(["date", "day_type"]).size().reset_index(name="count")
chan_daily = chan_df.groupby(["date", "day_type"]).size().reset_index(name="count")

# Create figure with two subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

# Reddit weekday vs weekend
reddit_avg = reddit_daily.groupby("day_type")["count"].mean()
ax1.bar(reddit_avg.index, reddit_avg.values, color=["lightblue", "lightgreen"])
ax1.set_title("Reddit r/politics\nAverage Daily Activity")
ax1.set_ylabel("Number of Comments")
for i, v in enumerate(reddit_avg.values):
    ax1.text(i, v + 100, f"{int(v):,}", ha="center")

# 4chan weekday vs weekend
chan_avg = chan_daily.groupby("day_type")["count"].mean()
ax2.bar(chan_avg.index, chan_avg.values, color=["lightblue", "lightgreen"])
ax2.set_title("4chan /pol/\nAverage Daily Activity")
ax2.set_ylabel("Number of Posts")
for i, v in enumerate(chan_avg.values):
    ax2.text(i, v + 100, f"{int(v):,}", ha="center")

plt.suptitle("Weekend vs Weekday Activity Patterns (Nov 1-14, 2024)")
plt.tight_layout()
plt.savefig("11_weekend_weekday_comparison.png", dpi=300, bbox_inches="tight")
plt.close()

# Print summary statistics
print("\nActivity Pattern Summary:")
print("\nReddit r/politics:")
print(f"Weekday average: {int(reddit_avg['Weekday']):,} comments")
print(f"Weekend average: {int(reddit_avg['Weekend']):,} comments")
print(f"Weekend/Weekday ratio: {reddit_avg['Weekend']/reddit_avg['Weekday']:.2f}")

print("\n4chan /pol/:")
print(f"Weekday average: {int(chan_avg['Weekday']):,} posts")
print(f"Weekend average: {int(chan_avg['Weekend']):,} posts")
print(f"Weekend/Weekday ratio: {chan_avg['Weekend']/chan_avg['Weekday']:.2f}")
