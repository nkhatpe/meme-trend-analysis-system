from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://10.144.1.3:27017/")
db = client["crawler4Reddit"]

# Date range
start_date = datetime(2024, 11, 1).timestamp()
end_date = datetime(2024, 11, 14).timestamp()

# Get data
comments = list(
    db.comments.find(
        {"created_utc": {"$gte": start_date, "$lte": end_date}},
        {"created_utc": 1, "author": 1, "_id": 0},
    )
)

# Convert to DataFrame
df = pd.DataFrame(comments)
df["datetime"] = pd.to_datetime(df["created_utc"], unit="s")
df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")

# Remove [deleted] and AutoModerator
df = df[~df["author"].isin(["[deleted]", "AutoModerator"])]

# 1. Daily Active Users Plot
daily_users = df.groupby("date")["author"].nunique().reset_index()

plt.figure(figsize=(12, 6))
plt.plot(daily_users["date"], daily_users["author"], marker="o")
plt.title("Daily Unique Users in r/politics (Nov 1-14, 2024)")
plt.xlabel("Date")
plt.ylabel("Number of Unique Users")
plt.xticks(rotation=45)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("6_daily_users_r_politics.png", dpi=300, bbox_inches="tight")
plt.close()

# 2. User Activity Distribution
user_activity = df.groupby("author").size().reset_index(name="activity_count")

plt.figure(figsize=(12, 6))
plt.hist(user_activity["activity_count"], bins=50, edgecolor="black")
plt.title("User Activity Distribution in r/politics (Nov 1-14, 2024)")
plt.xlabel("Number of Comments per User")
plt.ylabel("Number of Users")
plt.yscale("log")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("6_user_activity_dist_r_politics.png", dpi=300, bbox_inches="tight")
plt.close()

# Print basic summary
print("\nUser Activity Summary:")
print(f"Total unique users: {len(user_activity):,}")
print(f"Average comments per user: {user_activity['activity_count'].mean():.2f}")
print(f"Median comments per user: {user_activity['activity_count'].median():.2f}")
print(f"Most active user comment count: {user_activity['activity_count'].max():,}")
print(f"\nDaily average unique users: {daily_users['author'].mean():.2f}")
