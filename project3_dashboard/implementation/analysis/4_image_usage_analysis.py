from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://10.144.1.3:27017/")
db_4chan = client["crawler_4chan_v2"]

# Date range
start_date = datetime(2024, 11, 1).timestamp()
end_date = datetime(2024, 12, 14).timestamp()

# Get posts with image information for both boards
posts = list(
    db_4chan.posts.find(
        {
            "time": {"$gte": start_date, "$lte": end_date},
            "board": {"$in": ["pol", "b"]},
        },
        {"time": 1, "board": 1, "ext": 1, "_id": 0},
    )
)

# Convert to DataFrame
df = pd.DataFrame(posts)
df["datetime"] = pd.to_datetime(df["time"], unit="s")
df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")
df["has_image"] = df["ext"].notna()
df["ext"] = df["ext"].str.lower().replace({".jpeg": ".jpg"})
df["hour"] = df["datetime"].dt.hour

# Daily Image Usage Analysis
daily_image_stats = (
    df[df["has_image"]]
    .groupby(["date", "board"])
    .size()
    .reset_index(name="posts_with_images")
)
daily_image_stats.pivot(index="date", columns="board", values="posts_with_images").plot(
    figsize=(12, 6), marker="o"
)
plt.title("Daily Number of Posts with Images by Board (Nov 1-14, 2024)")
plt.ylabel("Number of Posts with Images")
plt.savefig("4_daily_image_usage.png", dpi=300, bbox_inches="tight")
plt.close()

# Extension Distribution Analysis
ext_dist = (
    df[df["ext"].isin([".jpg", ".png"])]
    .groupby(["board", "ext"])
    .size()
    .unstack(fill_value=0)
)
ext_dist.plot(kind="bar", figsize=(10, 5), width=0.8)
plt.title("Image Extension Distribution by Board (JPG/PNG)")
plt.ylabel("Number of Images")
plt.savefig("4_extension_distribution.png", dpi=300, bbox_inches="tight")
plt.close()

# Hourly Image Usage Patterns
hourly_image_stats = (
    df[df["has_image"]]
    .groupby(["board", "hour"])
    .size()
    .reset_index(name="posts_with_images")
)
hourly_image_stats.pivot(
    index="hour", columns="board", values="posts_with_images"
).plot(figsize=(12, 6), marker="o")
plt.title("Hourly Number of Posts with Images by Board")
plt.ylabel("Number of Posts with Images")
plt.xticks(range(24))
plt.savefig("4_hourly_image_usage.png", dpi=300, bbox_inches="tight")
plt.close()

# Print and save summary statistics
with open("image_analysis_summary.txt", "w") as f:
    for board in ["pol", "b"]:
        board_data = df[df["board"] == board]
        total_posts = len(board_data)
        posts_with_images = board_data["has_image"].sum()
        ext_counts = board_data[board_data["ext"].isin([".jpg", ".png"])][
            "ext"
        ].value_counts()

        print(f"\n/{board}/ Statistics:")
        print(f"Total Posts: {total_posts:,}")
        print(f"Posts with Images: {int(posts_with_images):,}")
        print("Extension Distribution (JPG/PNG only):")
        print(ext_counts)

        f.write(f"/{board}/ Statistics:\n")
        f.write(f"Total Posts: {total_posts:,}\n")
        f.write(f"Posts with Images: {int(posts_with_images):,}\n")
        f.write("Extension Distribution (JPG/PNG only):\n")
        f.write(f"{ext_counts}\n")
