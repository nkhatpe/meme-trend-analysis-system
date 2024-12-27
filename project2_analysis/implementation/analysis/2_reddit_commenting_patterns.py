from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["crawler4Reddit"]

# Date range
start_date = datetime(2024, 11, 1).timestamp()
end_date = datetime(2024, 11, 14).timestamp()

# Get comments with specific fields only
comments = list(
    db.comments.find(
        {"created_utc": {"$gte": start_date, "$lte": end_date}},
        {"created_utc": 1, "_id": 0},
    )
)

# Convert to DataFrame and process data
df = pd.DataFrame(comments)
df["datetime"] = pd.to_datetime(df["created_utc"], unit="s")
df["hour"] = df["datetime"].dt.hour
df["day"] = df["datetime"].dt.strftime("%Y-%m-%d")
hourly_counts = df.groupby(["day", "hour"]).size().reset_index(name="count")
pivot_data = hourly_counts.pivot(index="day", columns="hour", values="count").fillna(0)

# Create and save the heatmap
plt.figure(figsize=(15, 8))
sns.heatmap(
    pivot_data, cmap="YlOrRd", cbar_kws={"label": "Number of Comments"}, fmt=".0f"
)
plt.title("Reddit r/politics Commenting Patterns by Hour/Day\n(Nov 1-14, 2024)")
plt.xlabel("Hour of Day (UTC)")
plt.ylabel("Date")
plt.tight_layout()
plt.savefig("2_reddit_commenting_patterns.png")

# Print statistics
print(f"\nTotal comments: {len(df)}")
print("\nBusiest hours:")
print(df.groupby("hour")["created_utc"].count().sort_values(ascending=False).head())
print("\nBusiest days:")
print(df.groupby("day")["created_utc"].count().sort_values(ascending=False).head())
print("\nAverage comments per hour by day:")
print(hourly_counts.groupby("day")["count"].mean())
