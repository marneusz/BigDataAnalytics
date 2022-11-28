import sys
import praw


def main():
    # Substitute with secret values
    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        password=PASSWORD,
        user_agent=USER_AGENT,
        username=USERNAME,
    )
    
    subreddit = reddit.subreddit("Bitcoin+ethereum+dogecoin+solana+cardano+XRP")
    for submission in subreddit.stream.submissions():
        process_submission(submission)


def process_submission(s):
    title = s.title.replace("\"", "").replace("\n", " ").replace("\\", " ").replace("\/", " ")
    text = s.selftext.replace("\"", "").replace("\n", " ").replace("\\", " ").replace("\/", " ")
    data = f'{{"id":"{s.id}","title":"{title}","text":"{text}","time":{s.created_utc},"upvotes":{s.score},"comments":{s.num_comments},"subreddit":"{s.subreddit.display_name}"}}' + ",\n"
    sys.stdout.write(data)
    return data


if __name__ == "__main__":
    main()
