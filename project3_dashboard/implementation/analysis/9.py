from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Connect to MongoDB
try:
    client = MongoClient("mongodb://10.144.1.3:27017/")  # Updated connection string
    db = client["crawler_4chan_v2"]

    # Convert dates to timestamps like in the example
    start_date = datetime(2024, 11, 1).timestamp()
    end_date = datetime(2024, 11, 14, 23, 59, 59).timestamp()

    # Query matching the example format
    posts = list(
        db.posts.find(
            {
                "time": {"$gte": start_date, "$lte": end_date},
                "board": "pol",
                "resto": {"$ne": 0},
            },
            {"time": 1, "_id": 0},
        )
    )

    if not posts:
        raise ValueError("No posts found in the specified date range")

    # Convert to DataFrame and process data
    df = pd.DataFrame(posts)
    df["datetime"] = pd.to_datetime(df["time"], unit="s")
    df["hour"] = df["datetime"].dt.strftime("%Y-%m-%d %H:00")
    hourly_counts = df.groupby("hour").size().reset_index(name="count")

    # Create and save the line plot
    plt.figure(figsize=(15, 6))
    plt.plot(hourly_counts["hour"], hourly_counts["count"])
    plt.title("Hourly 4chan /pol/ Comment Counts\n(Nov 1-14, 2024)")
    plt.xlabel("Date/Hour (UTC)")
    plt.ylabel("Comment Count")
    plt.xticks(hourly_counts.index[::12], hourly_counts["hour"][::12], rotation=45)
    plt.tight_layout()
    plt.savefig("9_hourly_pol_comments.png")

except Exception as e:
    print(f"Error: {str(e)}")
    print("Debug info:")
    print(f"Number of posts retrieved: {len(posts) if 'posts' in locals() else 0}")
    if "df" in locals():
        print("DataFrame columns:", df.columns.tolist())
