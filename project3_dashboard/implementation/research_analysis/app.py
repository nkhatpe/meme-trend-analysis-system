from flask import Flask, render_template, request
import pandas as pd
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime
import io
import base64
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import redis
import json
from datetime import timedelta
from bson import ObjectId
import json
from json import JSONEncoder
import os
import seaborn as sns


class MongoJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return JSONEncoder.default(self, obj)

app = Flask(__name__)

app.static_folder = 'static'

MONGO_URI = 'mongodb://localhost:27017'
# Redis configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
CACHE_EXPIRATION = 3600 * 24 * 7  # 7 days in seconds 

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

redis_binary = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=False
)

def create_indexes():
    try:
        dbs = get_db_connection()
        
        reddit_comments = dbs['reddit'].comments
        reddit_posts = dbs['reddit'].posts
        
        reddit_comments.create_index([
            ('created_utc', ASCENDING),
            ('post_id', ASCENDING)
        ])
        reddit_comments.create_index([
            ('created_utc', ASCENDING),
            ('hate_speech_result.confidence', ASCENDING),
            ('score', ASCENDING)
        ])
        
        reddit_posts.create_index([
            ('subreddit', ASCENDING),
            ('id', ASCENDING)
        ])
        
        chan_posts = dbs['crawler_4chan_v2'].posts
        chan_posts.create_index([
            ('time', ASCENDING),
            ('board', ASCENDING),
            ('resto', ASCENDING)
        ])
        chan_posts.create_index([
            ('time', ASCENDING),
            ('hate_speech_result.confidence', ASCENDING),
            ('replies', ASCENDING)
        ])
        
        chan_posts.create_index([
            ('time', ASCENDING),
            ('board', ASCENDING),
            ('ext', ASCENDING)
        ])
        
        chan_posts.create_index([
            ('board', ASCENDING),
            ('ext', ASCENDING),
            ('time', ASCENDING)
        ])
        
        chan_posts.create_index([
            ('board', ASCENDING),
            ('time', ASCENDING),
            ('ext', ASCENDING),
            ('resto', ASCENDING)
        ])
        
        chan_posts.create_index([
            ('board', ASCENDING),
            ('ext', ASCENDING),
            ('filename', ASCENDING)
        ])
        
        print("Successfully created all indexes")
    except Exception as e:
        print(f"Error creating indexes: {str(e)}")

def get_db_connection():
    try:
        client = MongoClient(MONGO_URI,
                           maxPoolSize=50,
                           serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return {
            'reddit': client['crawler4Reddit'],
            'crawler_4chan_v2': client['crawler_4chan_v2']
        }
    except Exception as e:
        print(f"MongoDB connection error: {str(e)}")
        raise

def get_cache_key(analysis_type, platform, start_date, end_date, data_type='data'):
    return f"{analysis_type}:{platform}:{start_date}:{end_date}:{data_type}"

def get_cached_data(analysis_type, platform, start_date, end_date):
    try:
        cache_key = get_cache_key(analysis_type, platform, start_date, end_date)
        cached_data = redis_client.get(cache_key)
        
        if cached_data:
            data_dict = json.loads(cached_data)
            df = pd.DataFrame(data_dict)
            
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.date
                
            return df
    except Exception as e:
        print(f"Cache retrieval error: {str(e)}")
    return None

def cache_data(analysis_type, platform, start_date, end_date, df):
    try:
        if not df.empty:
            cache_key = get_cache_key(analysis_type, platform, start_date, end_date)
            df_copy = df.copy()
            
            if 'date' in df_copy.columns:
                df_copy['date'] = df_copy['date'].astype(str)
            
            json_str = json.dumps(df_copy.to_dict(orient='records'), cls=MongoJSONEncoder)
            
            redis_client.setex(
                cache_key,
                CACHE_EXPIRATION,
                json_str
            )
            return True
    except Exception as e:
        print(f"Cache storage error: {str(e)}")
    return False



def get_analysis_images():
    image_files = [
        "1_hate_speech_comparison.png",
        "2_reddit_commenting_patterns.png",
        "3_4chan_commenting_patterns.png",
        "4_daily_image_usage.png",
        "4_extension_distribution.png",
        "4_hourly_image_usage.png",
        "5_daily_reuse_ratio.png",
        "5_image_reuse_distribution.png",
        "6_daily_users_r_politics.png",
        "6_user_activity_dist_r_politics.png",
        "7_daily_politics_submissions.png",
        "8_hourly_politics_comments.png",
        "9_hourly_pol_comments.png",
        "10_hourly_activity.png",
        "11_weekend_weekday_comparison.png"
    ]
    
    summaries = {}
    text_files = ['image_analysis_summary.txt', 'image_reuse_analysis.txt']
    for file in text_files:
        try:
            with open(os.path.join(app.static_folder, file), 'r') as f:
                summaries[file] = f.read()
        except:
            summaries[file] = "Summary not available"
    
    return image_files, summaries


def query_activity_data(platform, start_date, end_date):
    cached_df = get_cached_data('activity', platform, start_date, end_date)
    if cached_df is not None:
        print(f"Cache hit for activity data: {platform}")
        return cached_df

    try:
        dbs = get_db_connection()
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())

        if platform == 'reddit':
            collection = dbs['reddit'].comments
            pipeline = [
                {
                    '$match': {
                        'created_utc': {
                            '$gte': start_ts,
                            '$lt': end_ts
                        }
                    }
                },
                {
                    '$lookup': {
                        'from': 'posts',
                        'localField': 'post_id',
                        'foreignField': 'id',
                        'pipeline': [
                            {
                                '$match': {
                                    'subreddit': 'politics'
                                }
                            },
                            {
                                '$project': {
                                    '_id': 1
                                }
                            }
                        ],
                        'as': 'post'
                    }
                },
                {
                    '$match': {
                        'post': {'$ne': []}
                    }
                },
                {
                    '$group': {
                        '_id': {
                            '$dateToString': {
                                'format': '%Y-%m-%d',
                                'date': {
                                    '$toDate': {'$multiply': ['$created_utc', 1000]}
                                }
                            }
                        },
                        'count': {'$sum': 1}
                    }
                },
                {'$sort': {'_id': 1}}
            ]
        else:
            collection = dbs['crawler_4chan_v2'].posts
            pipeline = [
                {
                    '$match': {
                        'time': {'$gte': start_ts, '$lt': end_ts},
                        'board': 'pol',
                        # 'resto': {'$ne': 0}
                    }
                },
                {
                    '$group': {
                        '_id': {
                            '$dateToString': {
                                'format': '%Y-%m-%d',
                                'date': {
                                    '$toDate': {'$multiply': ['$time', 1000]}
                                }
                            }
                        },
                        'count': {'$sum': 1}
                    }
                },
                {'$sort': {'_id': 1}}
            ]

        if platform == 'reddit':
            results = list(collection.aggregate(
                pipeline,
                allowDiskUse=True,
                hint={'created_utc': 1, 'post_id': 1}
            ))
        else:
            results = list(collection.aggregate(
                pipeline,
                allowDiskUse=True,
                hint={'time': 1, 'board': 1, 'resto': 1}
            ))
        
        if results:
            df = pd.DataFrame(results)
            df['date'] = pd.to_datetime(df['_id']).dt.date
            df = df.drop('_id', axis=1)
            
            cache_data('activity', platform, start_date, end_date, df)
            return df
            
        return pd.DataFrame()
    
    except Exception as e:
        print(f"Error querying activity data for {platform}: {str(e)}")
        return pd.DataFrame()

def query_hate_speech_data(platform, start_date, end_date):
    cached_df = get_cached_data('hate_speech', platform, start_date, end_date)
    if cached_df is not None:
        print(f"Cache hit for hate speech data: {platform}")
        return cached_df

    try:
        dbs = get_db_connection()
        
        if platform == 'reddit':
            collection = dbs['reddit'].comments
            time_field = 'created_utc'
            engagement_field = 'score'
            index_hint = {
                'created_utc': 1,
                'hate_speech_result.confidence': 1,
                'score': 1
            }
        else:
            collection = dbs['crawler_4chan_v2'].posts
            time_field = 'time'
            engagement_field = 'replies'
            index_hint = {
                'time': 1,
                'hate_speech_result.confidence': 1,
                'replies': 1
            }
        
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
        
        pipeline = [
            {
                '$match': {
                    time_field: {'$gte': start_ts, '$lt': end_ts},
                    'hate_speech_result.confidence': {'$exists': True}
                }
            },
            {
                '$project': {
                    'date': {
                        '$dateToString': {
                            'format': '%Y-%m-%d',
                            'date': {
                                '$toDate': {'$multiply': [f'${time_field}', 1000]}
                            }
                        }
                    },
                    'confidence': '$hate_speech_result.confidence',
                    'engagement': {'$ifNull': [f'${engagement_field}', 0]}
                }
            },
            {'$sort': {'date': 1}}
        ]
        
        results = list(collection.aggregate(pipeline, allowDiskUse=True, hint=index_hint))
        
        if results:
            df = pd.DataFrame(results)
            df['date'] = pd.to_datetime(df['date']).dt.date
            
            cache_data('hate_speech', platform, start_date, end_date, df)
            return df
            
        return pd.DataFrame()
    
    except Exception as e:
        print(f"Error querying hate speech data for {platform}: {str(e)}")
        return pd.DataFrame()

def create_plot_base64(df, plot_type, platform, analysis_type):
    try:
        if plot_type:  # For hate speech plots
            cache_key = get_cache_key(
                analysis_type, 
                platform, 
                df['date'].min().strftime('%Y-%m-%d'),
                df['date'].max().strftime('%Y-%m-%d'),
                f'plot_{plot_type}'
            )
        else:  # For activity plots
            cache_key = get_cache_key(
                analysis_type,
                platform,
                df['date'].min().strftime('%Y-%m-%d'),
                df['date'].max().strftime('%Y-%m-%d'),
                'plot'
            )

        cached_plot = redis_binary.get(cache_key)
        if cached_plot:
            print(f"Cache hit for {analysis_type} plot: {platform} - {plot_type if plot_type else 'activity'}")
            return base64.b64encode(cached_plot).decode('utf-8')

        plt.figure(figsize=(10, 6))
        
        if analysis_type == 'activity':
            plt.plot(df['date'], df['count'], 
                    label=f'{platform.capitalize()} Activity',
                    color='orange' if platform == '4chan' else 'blue',
                    marker='o')
            plt.title(f"Daily Activity - {'r/politics' if platform == 'reddit' else '/pol/'}")
            plt.ylabel('Number of Posts/Comments')
            plt.xlabel('Date')
        elif plot_type == 'trend':
            daily_avg = df.groupby('date')[['confidence', 'engagement']].mean().reset_index()
            plt.plot(daily_avg['date'], daily_avg['confidence'], 
                    label=f'{platform.capitalize()} Confidence',
                    color='orange' if platform == '4chan' else 'blue',
                    marker='o')
            plt.title(f'{platform.capitalize()} Hate Speech Confidence Trend')
            plt.ylabel('Average Confidence')
            plt.xlabel('Date')
        else:  # Scatter plot for engagement vs toxicity
            plt.scatter(df['confidence'], df['engagement'],
                    alpha=0.5,
                    color='orange' if platform == '4chan' else 'blue',
                    label=f'{platform.capitalize()} Engagement')
            plt.title(f"Engagement vs. Toxicity Confidence ({platform.capitalize()})")
            plt.xlabel('Toxicity Confidence')
            plt.ylabel('Engagement (Score)' if platform == 'reddit' else 'Engagement (Replies)')
        
        plt.xticks(rotation=45)
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=300)
        plt.close()
        
        # Cache the plot
        buf.seek(0)
        plot_data = buf.getvalue()
        redis_binary.setex(cache_key, CACHE_EXPIRATION, plot_data)
        
        print(f"Cached {analysis_type} plot: {platform} - {plot_type if plot_type else 'activity'}")
        return base64.b64encode(plot_data).decode('utf-8')
    
    except Exception as e:
        print(f"Error creating plot: {str(e)}")
        return None

def generate_insights(data, platform, analysis_type):
    try:
        cache_key = get_cache_key(analysis_type, platform, 
                                 data['date'].min().strftime('%Y-%m-%d'),
                                 data['date'].max().strftime('%Y-%m-%d'), 
                                 'insights')
        
        cached_insights = redis_client.get(cache_key)
        if cached_insights:
            return json.loads(cached_insights)

        insights = {}
        if not data.empty:
            if analysis_type == 'activity':
                insights = {
                    'total_posts': f"{data['count'].sum():,}",
                    'avg_daily_posts': f"{data['count'].mean():.0f}"
                }
            else:
                daily_avg = data.groupby('date')[['confidence', 'engagement']].mean().reset_index()
                insights = {
                    'avg_confidence': f"{daily_avg['confidence'].mean():.2f}",
                    'avg_engagement': f"{daily_avg['engagement'].mean():.2f}",
                    'total_posts': f"{len(data):,}",
                    'correlation': f"{data['confidence'].corr(data['engagement']):.2f}"
                }
            
            json_str = json.dumps(insights, cls=MongoJSONEncoder)
            redis_client.setex(cache_key, CACHE_EXPIRATION, json_str)
        
        return insights
    except Exception as e:
        print(f"Error generating insights: {str(e)}")
        return {}
    
def query_comment_patterns(platform, start_date, end_date):
    cache_key = get_cache_key('patterns', platform, start_date, end_date)
    cached_df = get_cached_data('patterns', platform, start_date, end_date)
    
    if cached_df is not None:
        print(f"Cache hit for comment patterns: {platform}")
        return cached_df

    try:
        dbs = get_db_connection()
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())

        if platform == 'reddit':
            collection = dbs['reddit'].comments
            time_field = 'created_utc'
            pipeline = [
                {
                    '$match': {
                        'created_utc': {'$gte': start_ts, '$lt': end_ts}
                    }
                },
                {
                    '$lookup': {
                        'from': 'posts',
                        'localField': 'post_id',
                        'foreignField': 'id',
                        'pipeline': [
                            {
                                '$match': {
                                    'subreddit': 'politics'
                                }
                            }
                        ],
                        'as': 'post'
                    }
                },
                {
                    '$match': {
                        'post': {'$ne': []}
                    }
                }
            ]
        else:
            collection = dbs['crawler_4chan_v2'].posts
            time_field = 'time'
            pipeline = [
                {
                    '$match': {
                        'time': {'$gte': start_ts, '$lt': end_ts},
                        'board': 'pol',
                        'resto': {'$ne': 0}
                    }
                }
            ]

        pipeline.extend([
            {
                '$project': {
                    'datetime': {
                        '$dateToString': {
                            'format': '%Y-%m-%d %H',
                            'date': {
                                '$toDate': {'$multiply': [f'${time_field}', 1000]}
                            }
                        }
                    }
                }
            },
            {
                '$group': {
                    '_id': '$datetime',
                    'count': {'$sum': 1}
                }
            }
        ])

        results = list(collection.aggregate(pipeline, allowDiskUse=True))
        
        if results:
            df = pd.DataFrame(results)
            df['datetime'] = pd.to_datetime(df['_id'])
            df['date'] = df['datetime'].dt.strftime('%Y-%m-%d')
            df['hour'] = df['datetime'].dt.hour
            df = df.drop(['_id', 'datetime'], axis=1)
            
            pivot_df = df.pivot(index='date', columns='hour', values='count').fillna(0)
            
            cache_data('patterns', platform, start_date, end_date, pivot_df)
            return pivot_df
            
        return pd.DataFrame()
    
    except Exception as e:
        print(f"Error querying comment patterns for {platform}: {str(e)}")
        return pd.DataFrame()

def create_heatmap(df, platform):
    try:
        plt.figure(figsize=(12, 6))
        sns.heatmap(
            df,
            cmap='YlOrRd',
            cbar_kws={'label': 'Number of Comments'},
            fmt='.0f'
        )
        
        title = 'r/politics' if platform == 'reddit' else '/pol/'
        plt.title(f"{title} Commenting Patterns by Hour/Day")
        plt.xlabel('Hour of Day (UTC)')
        plt.ylabel('Date')
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=300, bbox_inches='tight')
        plt.close()
        
        buf.seek(0)
        return base64.b64encode(buf.getvalue()).decode('utf-8')
    
    except Exception as e:
        print(f"Error creating heatmap: {str(e)}")
        return None

def analyze_patterns(df, platform):
    try:
        insights = {
            'total_comments': int(df.sum().sum()),
            'avg_comments_per_hour': round(df.mean().mean(), 1),
            'peak_hour': int(df.mean().idxmax()),
            'peak_hour_avg': round(df.mean().max(), 1),
            'busiest_date': df.sum(axis=1).idxmax(),
            'busiest_date_total': int(df.sum(axis=1).max())
        }
        return insights
    except Exception as e:
        print(f"Error analyzing patterns: {str(e)}")
        return {}
    
def query_image_usage_data(start_date, end_date):
    cache_key = f"image_usage:{start_date}:{end_date}"
    cached_data = redis_client.get(cache_key)
    
    if cached_data:
        print("Cache hit for image usage data")
        data_dict = json.loads(cached_data)
        return pd.DataFrame(data_dict)
        
    try:
        dbs = get_db_connection()
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
        
        pipeline = [
            {
                '$match': {
                    'time': {'$gte': start_ts, '$lt': end_ts},
                    'board': {'$in': ['pol', 'b']}
                }
            },
            {
                '$project': {
                    'date': {
                        '$dateToString': {
                            'format': '%Y-%m-%d',
                            'date': {'$toDate': {'$multiply': ['$time', 1000]}}
                        }
                    },
                    'hour': {'$hour': {'$toDate': {'$multiply': ['$time', 1000]}}},
                    'board': 1,
                    'ext': {'$toLower': '$ext'},
                    'has_image': {'$cond': [{'$ifNull': ['$ext', None]}, 1, 0]}
                }
            }
        ]
        
        results = list(dbs['crawler_4chan_v2'].posts.aggregate(pipeline, allowDiskUse=True))
        
        if results:
            df = pd.DataFrame(results)
            df['ext'] = df['ext'].replace({'.jpeg': '.jpg'})
            
            # Cache the results
            json_str = json.dumps(df.to_dict(orient='records'), cls=MongoJSONEncoder)
            redis_client.setex(cache_key, CACHE_EXPIRATION, json_str)
            
            return df
            
        return pd.DataFrame()
        
    except Exception as e:
        print(f"Error querying image usage data: {str(e)}")
        return pd.DataFrame()

def create_image_usage_plots(df):
    plots = {}
    
    try:
        # Daily image usage
        plt.figure(figsize=(12, 6))
        daily_stats = df[df['has_image'] == 1].groupby(['date', 'board']).size().unstack(fill_value=0)
        daily_stats.plot(marker='o')
        plt.title('Daily Number of Posts with Images by Board')
        plt.ylabel('Number of Posts with Images')
        plt.xticks(rotation=45)
        plots['daily_usage'] = save_plot_to_base64()
        
        # Extension distribution
        plt.figure(figsize=(10, 6))
        ext_dist = df[df['ext'].isin(['.jpg', '.png'])].groupby(['board', 'ext']).size().unstack(fill_value=0)
        ext_dist.plot(kind='bar', width=0.8)
        plt.title('Image Extension Distribution by Board (JPG/PNG)')
        plt.ylabel('Number of Images')
        plots['ext_dist'] = save_plot_to_base64()
        
        # Hourly patterns
        plt.figure(figsize=(12, 6))
        hourly_stats = df[df['has_image'] == 1].groupby(['board', 'hour']).size().unstack(fill_value=0)
        sns.heatmap(hourly_stats, cmap='YlOrRd', cbar_kws={'label': 'Number of Posts'})
        plt.title('Hourly Image Usage Patterns by Board')
        plt.xlabel('Hour of Day (UTC)')
        plt.ylabel('Board')
        plots['hourly_pattern'] = save_plot_to_base64()
        
        return plots
        
    except Exception as e:
        print(f"Error creating image usage plots: {str(e)}")
        return {}

def generate_image_usage_insights(df):
    insights = {}
    
    try:
        for board in ['pol', 'b']:
            board_data = df[df['board'] == board]
            total_posts = len(board_data)
            posts_with_images = board_data['has_image'].sum()
            
            insights[board] = {
                'total_posts': f"{total_posts:,}",
                'posts_with_images': f"{int(posts_with_images):,}",
                'image_ratio': f"{(posts_with_images/total_posts*100):.1f}%",
                'jpg_ratio': f"{(len(board_data[board_data['ext'] == '.jpg'])/posts_with_images*100):.1f}%",
                'png_ratio': f"{(len(board_data[board_data['ext'] == '.png'])/posts_with_images*100):.1f}%"
            }
            
        return insights
        
    except Exception as e:
        print(f"Error generating image usage insights: {str(e)}")
        return {}

def save_plot_to_base64():
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=300, bbox_inches='tight')
    plt.close()
    buf.seek(0)
    return base64.b64encode(buf.getvalue()).decode('utf-8')


@app.route("/")
def index():
    analysis_type = request.args.get("analysis", "activity")
    platform = request.args.get("platform", "reddit")
    start_date = request.args.get("start_date", "2024-11-01")
    end_date = request.args.get("end_date", "2024-11-07")
    
    plots = {}

    insights = {
        'pol': {
            'total_posts': '0',
 
            'jpg_ratio': '0%',
            'png_ratio': '0%'
        },
        'b': {
            'total_posts': '0',
           
            'jpg_ratio': '0%',
            'png_ratio': '0%'
        }
    }
    patterns = {}
    
    if analysis_type == 'image_usage':
            data = query_image_usage_data(start_date, end_date)
            if not data.empty:
                plots = create_image_usage_plots(data)
                board_insights = generate_image_usage_insights(data)
                
                for board in ['pol', 'b']:
                    if board in board_insights:
                        insights[board].update(board_insights[board])
    elif analysis_type != 'project2':
        platforms_to_query = ['reddit', '4chan'] if platform == 'both' else [platform]
        
        for p in platforms_to_query:
            try:
                if analysis_type == 'activity':
                    data = query_activity_data(p, start_date, end_date)
                    if not data.empty:
                        plot_key = 'chan' if p == '4chan' else p
                        plots[f'{plot_key}_activity'] = create_plot_base64(data, None, p, 'activity')
                        insights[plot_key] = generate_insights(data, p, 'activity')
                        
                        pattern_data = query_comment_patterns(p, start_date, end_date)
                        if not pattern_data.empty:
                            plots[f'{plot_key}_heatmap'] = create_heatmap(pattern_data, p)
                            patterns[plot_key] = analyze_patterns(pattern_data, p)
                            
                else:
                    data = query_hate_speech_data(p, start_date, end_date)
                    if not data.empty:
                        plot_key = 'chan' if p == '4chan' else p
                        plots[f'{plot_key}_trend'] = create_plot_base64(data, 'trend', p, 'hate_speech')
                        plots[f'{plot_key}_engagement'] = create_plot_base64(data, 'engagement', p, 'hate_speech')
                        insights[plot_key] = generate_insights(data, p, 'hate_speech')
            except Exception as e:
                print(f"Error processing {p}: {str(e)}")
    
    analysis_images = []
    summaries = {}
    if analysis_type == 'project2':
        analysis_images, summaries = get_analysis_images()
    
    return render_template(
        "index.html",
        analysis_type=analysis_type,
        platform=platform,
        start_date=start_date,
        end_date=end_date,
        plots=plots,
        insights=insights,
        patterns=patterns,
        analysis_images=analysis_images if 'analysis_images' in locals() else [],
        summaries=summaries if 'summaries' in locals() else {}
    )

if __name__ == "__main__":
    create_indexes()
    app.run(debug=True)