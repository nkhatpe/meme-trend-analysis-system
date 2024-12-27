from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from collections import defaultdict

# Connect to MongoDB
client = MongoClient("mongodb://10.144.1.3:27017/")
db_4chan = client["crawler_4chan_v2"]

# Date range
start_date = datetime(2024, 11, 1).timestamp()
end_date = datetime(2024, 11, 14).timestamp()

# Get posts with MD5 hashes
posts = list(
    db_4chan.posts.find(
        {
            "time": {"$gte": start_date, "$lte": end_date},
            "board": {"$in": ["pol", "b"]},
            "md5": {"$exists": True},
        },
        {"time": 1, "board": 1, "md5": 1, "thread_id": 1, "_id": 0},
    )
)

# Convert to DataFrame
df = pd.DataFrame(posts)
df["datetime"] = pd.to_datetime(df["time"], unit="s")
df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")

# Analyze image reuse
image_reuse = defaultdict(lambda: {"count": 0, "boards": set(), "threads": set()})

for _, row in df.iterrows():
    if row["md5"]:
        image_reuse[row["md5"]]["count"] += 1
        image_reuse[row["md5"]]["boards"].add(row["board"])
        image_reuse[row["md5"]]["threads"].add(f"{row['board']}_{row['thread_id']}")

# Create reuse statistics
reuse_stats = {"times_used": [], "board_count": [], "thread_count": []}

for md5, stats in image_reuse.items():
    reuse_stats["times_used"].append(stats["count"])
    reuse_stats["board_count"].append(len(stats["boards"]))
    reuse_stats["thread_count"].append(len(stats["threads"]))

reuse_df = pd.DataFrame(reuse_stats)

# 1. Create distribution of image reuse
plt.figure(figsize=(12, 6))
plt.hist(reuse_df["times_used"], bins=50, edgecolor="black")
plt.title("Distribution of Image Reuse (Nov 1-14, 2024)")
plt.xlabel("Number of Times Image Was Used")
plt.ylabel("Number of Images")
plt.yscale("log")  # Log scale for better visualization
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("5_image_reuse_distribution.png", dpi=300, bbox_inches="tight")
plt.close()


# 2. Daily image reuse patterns
df["date_only"] = pd.to_datetime(df["date"])
daily_reuse = df.groupby(["date_only", "md5"]).size()
daily_reuse = daily_reuse.reset_index()
daily_reuse_stats = (
    daily_reuse.groupby("date_only")
    .agg({"md5": "count", 0: "sum"})  # Total unique images  # Total image uses
    .reset_index()
)
daily_reuse_stats["reuse_ratio"] = daily_reuse_stats[0] / daily_reuse_stats["md5"]

plt.figure(figsize=(15, 8))
plt.plot(daily_reuse_stats["date_only"], daily_reuse_stats["reuse_ratio"], marker="o")
plt.title("Daily Image Reuse Ratio (Nov 1-14, 2024)")
plt.xlabel("Date")
plt.ylabel("Average Uses per Unique Image")
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("5_daily_reuse_ratio.png", dpi=300, bbox_inches="tight")
plt.close()

# Save detailed statistics
with open("image_reuse_analysis.txt", "w") as f:
    f.write("Image Reuse Analysis (Nov 1-14, 2024)\n")
    f.write("-" * 50 + "\n\n")

    # Overall statistics
    total_images = len(image_reuse)
    total_uses = sum(reuse_stats["times_used"])
    reused_images = sum(1 for x in reuse_stats["times_used"] if x > 1)

    f.write(f"Total unique images: {total_images:,}\n")
    f.write(f"Total image uses: {total_uses:,}\n")
    f.write(
        f"Images used more than once: {reused_images:,} ({reused_images/total_images*100:.2f}%)\n\n"
    )

    # Most reused images
    f.write("Top 10 Most Reused Images:\n")
    for md5, stats in sorted(
        image_reuse.items(), key=lambda x: x[1]["count"], reverse=True
    )[:10]:
        f.write(f"MD5: {md5}\n")
        f.write(f"Times used: {stats['count']:,}\n")
        f.write(f"Boards: {', '.join(stats['boards'])}\n")
        f.write(f"Number of threads: {len(stats['threads'])}\n")
        f.write("-" * 30 + "\n")

# Print summary to console
print("\nImage Reuse Analysis Summary:")
print("-" * 50)
print(f"Total unique images: {total_images:,}")
print(f"Total image uses: {total_uses:,}")
print(
    f"Images used more than once: {reused_images:,} ({reused_images/total_images*100:.2f}%)"
)
print(f"Maximum reuse of any single image: {max(reuse_stats['times_used']):,}")
print(f"Average uses per image: {total_uses/total_images:.2f}")
