from flask import Flask, jsonify
import pyodbc
import praw
from datetime import datetime, timezone

app = Flask(__name__)

# reddit API setup
reddit = praw.Reddit(
    client_id='',
    client_secret='',
    user_agent=''
)

# Azure SQL Database connection string
connection_string = (
)

def fetch():
    list = [ # list of subreddits, to fetch data from
        "programming", "learnprogramming", "datascience", "machinelearning",
        "python", "SQL", "coding", "opensource", "webdev", "dataisbeautiful"
    ]
    try: # try connection to SQL db
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        # loop for API calls
        for i in list:
            subreddit = reddit.subreddit(i)
            posts = []
            for post in subreddit.new():
                post_time = post.created_utc
                now = int(datetime.now(timezone.utc).timestamp() - 3600)
                if now >= post_time: # ensuring that we only call posts from the last hour, inline with the Azure schedular
                    posts.append({
                        "id": post.id,
                        "subreddit": i,
                        "title": post.title,
                        "upvotes": post.ups,
                        "upvote_ratio": post.upvote_ratio,
                        "comments": post.num_comments,
                        "created_utc": datetime.fromtimestamp(post_time, timezone.utc)
                    })

            for post in posts: # inserts data from API calls to Azure SQL db
                cursor.execute(
                    """
                    MERGE reddit_posts AS target
                    USING (SELECT ? AS id, ? AS subreddit, ? AS title, ? AS upvotes, ? AS upvote_ratio, ? AS comments, ? AS created_utc) AS source
                    ON target.id = source.id
                    WHEN NOT MATCHED THEN
                        INSERT (id, subreddit, title, upvotes, upvote_ratio, comments, created_utc)
                        VALUES (source.id, source.subreddit, source.title, source.upvotes, source.upvote_ratio, source.comments, source.created_utc);
                    """,
                    (post['id'], post['subreddit'], post['title'], post['upvotes'], post['upvote_ratio'], post['comments'], post['created_utc'])
                )

        connection.commit()
        cursor.close()
        connection.close()
        return {"status": "success", "message": "Data insertion completed successfully."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.route('/', methods=['GET']) # GET request for the flask API
def trigger_reddit():
    result = fetch()
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000) # Local Host