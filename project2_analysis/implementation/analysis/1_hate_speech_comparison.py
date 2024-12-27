from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["crawler4Reddit"]
db_4chan = client["crawler_4chan_v2"]

# Date range
start_date = datetime(2024, 11, 1).timestamp()
end_date = datetime(2024, 11, 14).timestamp()

# Get Reddit data
reddit_posts = list(
    db.posts.find(
        {
            "created": {"$gte": start_date, "$lte": end_date},
            "subreddit": "politics",
            "hate_speech_analyzed": True,
        },
        {"hate_speech_result.class": 1, "_id": 0},
    )
)

reddit_comments = list(
    db.comments.find(
        {
            "created_utc": {"$gte": start_date, "$lte": end_date},
            "hate_speech_analyzed": True,
        },
        {"hate_speech_result.class": 1, "_id": 0},
    )
)

# Get 4chan data
pol_posts = list(
    db_4chan.posts.find(
        {
            "time": {"$gte": start_date, "$lte": end_date},
            "board": "pol",
            "hate_speech_analyzed": True,
        },
        {"hate_speech_result.class": 1, "_id": 0},
    )
)

# Process Reddit posts
reddit_hate_speech = {"normal": 0, "flagged": 0}

for post in reddit_posts + reddit_comments:
    if "hate_speech_result" in post and post["hate_speech_result"]:
        result = post["hate_speech_result"]["class"]
        reddit_hate_speech["flagged" if result != "normal" else "normal"] += 1

# Process 4chan posts
pol_hate_speech = {"normal": 0, "flagged": 0}

for post in pol_posts:
    if "hate_speech_result" in post and post["hate_speech_result"]:
        result = post["hate_speech_result"]["class"]
        pol_hate_speech["flagged" if result != "normal" else "normal"] += 1


# Calculate percentages
def calculate_percentages(data):
    total = sum(data.values())
    return {k: (v / total) * 100 for k, v in data.items()} if total > 0 else data


reddit_percentages = calculate_percentages(reddit_hate_speech)
pol_percentages = calculate_percentages(pol_hate_speech)

# Create DataFrame for plotting
df = pd.DataFrame(
    {"Reddit r/politics": reddit_percentages, "4chan /pol/": pol_percentages}
).reset_index()
df.columns = ["Category", "Reddit r/politics", "4chan /pol/"]

# Plotting
plt.figure(figsize=(12, 6))
x = range(len(df["Category"]))
width = 0.35

plt.bar(
    [i - width / 2 for i in x],
    df["Reddit r/politics"],
    width,
    label="Reddit r/politics",
)
plt.bar([i + width / 2 for i in x], df["4chan /pol/"], width, label="4chan /pol/")

plt.xlabel("Content Category")
plt.ylabel("Percentage of Posts (%)")
plt.title("Hate Speech Detection Comparison\n(Nov 1-14, 2024)")
plt.xticks(x, df["Category"])
plt.legend()
plt.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig("1_hate_speech_comparison.png")

# Print the actual numbers
print("\nReddit r/politics distribution:")
for k, v in reddit_hate_speech.items():
    print(f"{k}: {v} posts ({reddit_percentages[k]:.2f}%)")

print("\n4chan /pol/ distribution:")
for k, v in pol_hate_speech.items():
    print(f"{k}: {v} posts ({pol_percentages[k]:.2f}%)")
