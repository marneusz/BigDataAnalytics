import requests

def request_reddits(subreddit, listing, limit, timeframe):
    try:
        base_url = f"https://www.reddit.com/r/{subreddit}/{listing}.json?limit={limit}&t={timeframe}"
        request = requests.get(base_url, headers = {'User-agent': 'yourbot'})
    except:
        print("While sending GET request, an error occured.")

    return request.json()