from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

client = MongoClient("mongodb://10.144.1.3:27017/")
db_4chan = client["crawler_4chan_v2"]

start_date = datetime(2024, 11, 1).timestamp()
end_date = datetime(2024, 12, 15).timestamp()

posts = list(
    db_4chan.posts.find(
        {
            "time": {"$gte": start_date, "$lte": end_date},
            "board": "pol",
            "resto": {"$ne": 0},
        },
        {"time": 1, "_id": 0},
    )
)

df = pd.DataFrame(posts)
df["datetime"] = pd.to_datetime(df["time"], unit="s")
df["hour"] = df["datetime"].dt.hour
df["day"] = df["datetime"].dt.strftime("%Y-%m-%d")

hourly_counts = df.groupby(["day", "hour"]).size().reset_index(name="count")
pivot_data = hourly_counts.pivot(index="day", columns="hour", values="count").fillna(0)

plt.figure(figsize=(15, 8))
sns.heatmap(
    pivot_data, cmap="YlOrRd", cbar_kws={"label": "Number of Comments"}, fmt=".0f"
)
plt.title("4chan /pol/ Commenting Patterns by Hour/Day\n(Nov 1-14, 2024)")
plt.xlabel("Hour of Day (UTC)")
plt.ylabel("Date")
plt.tight_layout()
plt.savefig("3_4chan_commenting_patterns.png")

print(f"Total comments: {len(df)}")
print("Busiest hours:")
print(df.groupby("hour")["time"].count().sort_values(ascending=False).head())
print("Busiest days:")
print(df.groupby("day")["time"].count().sort_values(ascending=False).head())
print("Average comments per hour by day:")
print(hourly_counts.groupby("day")["count"].mean())
print(f"Median comments per hour: {hourly_counts['count'].median()}")
print(f"Max comments in one hour: {hourly_counts['count'].max()}")
print(f"Hour with max comments:\n{hourly_counts.loc[hourly_counts['count'].idxmax()]}")
