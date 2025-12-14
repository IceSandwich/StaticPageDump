import os
import hashlib
import requests
from flask import Flask, Response, request
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import argparse

app = Flask(__name__)

# 真实远端站点
parser = argparse.ArgumentParser()
parser.add_argument("--url", type=str, required=True)
parser.add_argument("--listen", type=str, default="127.0.0.1")
parser.add_argument("--output", type=str, required=True)
parser.add_argument("--port", type=int, default="8080")
args = parser.parse_args()

TARGET_BASE = args.url
PORT = args.port
LISTEN = args.listen

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(SCRIPT_DIR, args.output) if not os.path.isabs(args.output) else args.output
os.makedirs(BASE_DIR, exist_ok=True)

session = requests.Session()

def url_to_local_path(url):
    """
    https://example.com/a/b/c.js
    -> ./example.com/a/b/c.js
    """
    parsed = urlparse(url)

    host = parsed.netloc
    path = parsed.path

    if not path or path.endswith("/"):
        path += "index.html"

    local_path = os.path.join(BASE_DIR, host, path.lstrip("/"))
    return local_path

def save_cache(url, content):
    local_path = url_to_local_path(url)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(content)

def load_cache(url):
    local_path = url_to_local_path(url)
    if os.path.exists(local_path):
        with open(local_path, "rb") as f:
            return f.read()
    return None

def rewrite_html(html, base_url):
    soup = BeautifulSoup(html, "html.parser")

    tags_attrs = {
        "img": "src",
        "script": "src",
        "link": "href",
        "a": "href"
    }

    for tag, attr in tags_attrs.items():
        for node in soup.find_all(tag):
            if node.has_attr(attr):
                original = node[attr]
                absolute = urljoin(base_url, original)
                node[attr] = "/proxy/" + absolute

    return str(soup)

@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def catch_all(path):
    if path == "":
        path = "index.html"
    real_url = urljoin(TARGET_BASE + "/", path)
    print(real_url)

    cached = load_cache(real_url)
    if cached:
        return Response(cached)

    resp = session.get(
        real_url,
        headers={"User-Agent": request.headers.get("User-Agent", "")},
        stream=True
    )

    content = resp.content
    content_type = resp.headers.get("Content-Type", "")

    # if "text/html" in content_type:
    #     content = rewrite_html(content, real_url).encode("utf-8")

    if resp.status_code == 200:
        save_cache(real_url, content)

    return Response(
        content,
        status=resp.status_code,
        content_type=content_type
    )

if __name__ == "__main__":
    app.run(host=LISTEN, port=PORT, debug=True)
