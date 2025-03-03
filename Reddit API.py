from flask import Flask
import pyodbc
import praw
from datetime import datetime, timezone
from typing import Tuple, List, Dict, Any

def launch_flask() -> Flask:
    app = Flask(__name__)
    return app

def praw_setup() -> praw.Reddit:
    reddit = praw.Reddit(
        client_id='',
        client_secret='',
        user_agent=''
        )
    return reddit

def connect_string() -> str:
    connection_string = ('')
    return connection_string

def subreddit_filters() -> List[str]:
    subreddit_list = [
    "programming", "learnprogramming", "datascience", "machinelearning",
    "python", "SQL", "coding", "opensource", "webdev", "dataisbeautiful"
    ]
    return subreddit_list

def fetch_subreddit_data(subreddit_list: List[str], reddit: praw.Reddit) -> List[Dict[str, Any]]:
    cutoff_timestamp = int(datetime.now(timezone.utc).timestamp() - 3600)
    return [
        {
            "id": post.id,
            "subreddit": sub,
            "title": post.title,
            "upvotes": post.ups,
            "upvote_ratio": post.upvote_ratio,
            "comments": post.num_comments,
            "created_utc": datetime.fromtimestamp(post.created_utc, timezone.utc)
        }
        for sub in subreddit_list
        for post in reddit.subreddit(sub).new()
        if post.created_utc <= cutoff_timestamp
    ]

def post_to_database(posts: List[Dict[str, Any]], cursor: pyodbc):
    for post in posts:
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

def start_database_connection(connection_string: str) -> Tuple[pyodbc.Cursor, pyodbc.Connection]:
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    return cursor, connection

def end_database_connection(cursor: pyodbc.Cursor, connection: pyodbc.Connection) -> None:
    connection.commit()
    cursor.close()
    connection.close()
    return

def register_routes(app: Flask) -> None:
    @app.route('/', methods=['GET'])
    def run_etl() -> str:
        reddit = praw_setup()
        connection_string = connect_string()
        subreddit_list = subreddit_filters()
        
        posts = fetch_subreddit_data(subreddit_list, reddit)
        cursor, connection = start_database_connection(connection_string)
        post_to_database(posts, cursor)
        end_database_connection(cursor, connection)
        
        return "ETL process complete"

if __name__ == '__main__':
    app = launch_flask()
    register_routes(app)
    app.run(host='0.0.0.0', port=5000)
