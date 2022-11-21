from google.cloud import secretmanager

# https://medium.com/google-developer-experts/how-to-store-sensitive-data-on-gcp-d96e4e545224


def get_secret_name(project_id: str, secret_id: str, version: str = "latest"):
    return f"projects/{project_id}/secrets/{secret_id}/versions/{version}"


def get_secret(
    project_id: str,
    secret_id: str,
    version: str = "latest",
    decode_format: str = "UTF-8",
):
    secret_name = get_secret_name(project_id, secret_id, version)
    sm_client = secretmanager.SecretManagerServiceClient()

    response = sm_client.access_secret_version(request={"name": secret_name})
    return response.payload.data.decode(decode_format)


if __name__ == "main":
    """
    Test
    """
    from conf import PROJECT_ID, DECODE_FORMAT, SECRET_CLIENT_ID

    print(get_secret(PROJECT_ID, SECRET_CLIENT_ID, "latest", DECODE_FORMAT))
