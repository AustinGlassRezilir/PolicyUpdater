# page_puller.py

import os, re, time, json, requests
from dotenv import load_dotenv
from token_retriever import get_access_token
from requests.exceptions import HTTPError

load_dotenv()
SITE_ID    = os.getenv("SITE_ID")
EXPORT_DIR = os.getenv("EXPORT_DIR", "sharepoint_exports")
os.makedirs(EXPORT_DIR, exist_ok=True)

HEADERS = {
    "Authorization": f"Bearer {get_access_token()}",
    "ConsistencyLevel": "eventual"
}

def sanitize_filename(name: str) -> str:
    return re.sub(r'[^\w\-\._ ]', '_', name)[:100]

def safe_get(url, headers, retries=3, backoff=2, timeout=30):
    for attempt in range(1, retries+1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except HTTPError as e:
            status = e.response.status_code
            if status in (502, 503, 504) and attempt < retries:
                wait = backoff ** attempt
                print(f"⚠️  Got {status}, retrying in {wait}s… (attempt {attempt}/{retries})")
                time.sleep(wait)
            else:
                raise

def pull_all_pages():
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/pages?$top=50"
    while url:
        resp = safe_get(url, HEADERS)
        data = resp.json()

        for page in data.get("value", []):
            if page.get("@odata.type") != "#microsoft.graph.sitePage":
                continue

            page_id  = page["id"]
            title    = page["title"]
            web_url  = page["webUrl"]  # always present

            parts_url  = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/pages/{page_id}/microsoft.graph.sitePage/webparts"
            parts_resp = safe_get(parts_url, HEADERS)

            html_blocks = [
                p.get("innerHtml", "")
                for p in parts_resp.json().get("value", [])
                if p.get("@odata.type") == "#microsoft.graph.textWebPart"
            ]

            if html_blocks:
                safe_name = sanitize_filename(title)
                html_path = os.path.join(EXPORT_DIR, f"{safe_name}.html")
                meta_path = os.path.join(EXPORT_DIR, f"{safe_name}.meta.json")

                with open(html_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(html_blocks))

                with open(meta_path, "w", encoding="utf-8") as mf:
                    json.dump({ "title": title, "url": web_url }, mf)

                print(f"✅ Pulled: {html_path}")

        url = data.get("@odata.nextLink")

if __name__ == "__main__":
    pull_all_pages()
