import os
from .manage_secrets import get_secret

SECRET_KEY_PATH = "../keys/crypto-busting-5eda452fd35c.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SECRET_KEY_PATH

SECRET_CLIENT_ID = "reddit-api-client-key"
SECRET_SECRET_KEY_ID = "reddit-api-secret-key"
SECRET_REDDIT_USER_PASSWORD_ID = "reddit-user-password"
DECODE_FORMAT = "UTF-8"
PROJECT_ID = "crypto-busting"

REDDIT_API_CONFIG = {
    "CLIENT_ID": get_secret(PROJECT_ID, SECRET_CLIENT_ID, "latest", DECODE_FORMAT),
    "SECRET_KEY": get_secret(PROJECT_ID, SECRET_SECRET_KEY_ID, "latest", DECODE_FORMAT),
}

REDDIT_USER_CONFIG = {
    "grant_type": "password",
    "username": "bda-reddit",
    "password": get_secret(
        PROJECT_ID, SECRET_REDDIT_USER_PASSWORD_ID, "latest", DECODE_FORMAT
    ),
}
