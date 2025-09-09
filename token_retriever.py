# token_retriever.py
import os
from dotenv import load_dotenv
from get_token import get_graph_token  # shared helper that mints & caches tokens

load_dotenv()

def get_access_token() -> str:
    """
    SOURCE token for downloading (page puller).
    """
    return get_graph_token("source")

def get_upload_access_token() -> str:
    """
    UPLOAD token for SharePoint backup uploader.
    """
    return get_graph_token("upload")

if __name__ == "__main__":
    try:
        print("SOURCE token length:", len(get_access_token()))
    except Exception as e:
        print("SOURCE token error:", e)
    try:
        print("UPLOAD token length:", len(get_upload_access_token()))
    except Exception as e:
        print("UPLOAD token error:", e)
