import asyncio
import os
import argparse
from aiohttp import web, ClientSession, WSMsgType
from urllib.parse import urljoin, urlparse

# 真实远端站点
parser = argparse.ArgumentParser()
parser.add_argument("--url", type=str, required=True)
parser.add_argument("--listen", type=str, default="127.0.0.1")
parser.add_argument("--output", type=str, required=True)
parser.add_argument("--port", type=int, default=8080)
parser.add_argument("--cache-prefix", type=str, action="append",
                    help="只缓存匹配该路径前缀的路由回应，可多次指定；不指定则缓存所有")
args = parser.parse_args()

TARGET_BASE = args.url
PORT = args.port
LISTEN = args.listen
CACHE_PREFIXES = args.cache_prefix

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(SCRIPT_DIR, args.output) if not os.path.isabs(args.output) else args.output
os.makedirs(BASE_DIR, exist_ok=True)


def should_cache(url):
    if not CACHE_PREFIXES:
        return True
    path = urlparse(url).path
    return any(path.startswith(prefix) for prefix in CACHE_PREFIXES)


def url_to_local_path(url):
    """
    https://example.com:8443/a/b/c.js
    -> ./example.com_8443/a/b/c.js
    """
    parsed = urlparse(url)

    host = parsed.netloc.replace(":", "_")
    path = parsed.path

    if not path or path.endswith("/"):
        path += "index.html"

    return os.path.join(BASE_DIR, host, path.lstrip("/"))


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


def upstream_ws_url(path):
    ws_base = TARGET_BASE.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    return urljoin(ws_base + "/", path)


def forward_headers(request):
    return {
        "User-Agent": request.headers.get("User-Agent", ""),
        "Cookie": request.headers.get("Cookie", ""),
    }


async def handle_http(request, real_url):
    print(real_url)

    if should_cache(real_url):
        cached = load_cache(real_url)
        if cached:
            return web.Response(body=cached)

    session = request.app["session"]
    data = await request.read() if request.method in ("POST", "PUT", "PATCH") else None

    async with session.request(
        request.method, real_url, headers=forward_headers(request), data=data
    ) as resp:
        content = await resp.read()
        content_type = resp.headers.get("Content-Type", "")

    if resp.status == 200 and request.method == "GET" and should_cache(real_url):
        save_cache(real_url, content)

    headers = {"Content-Type": content_type} if content_type else {}
    return web.Response(body=content, status=resp.status, headers=headers)


async def handle_ws(request, real_url):
    print(real_url)
    client_ws = web.WebSocketResponse(compress=False)
    await client_ws.prepare(request)

    session = request.app["session"]
    try:
        upstream_ws = await session.ws_connect(
            real_url, headers=forward_headers(request), compress=0
        )
    except Exception:
        await client_ws.close()
        return client_ws

    async def client_to_upstream():
        try:
            async for msg in client_ws:
                if msg.type == WSMsgType.TEXT and msg.data is not None:
                    await upstream_ws.send_str(msg.data)
                elif msg.type == WSMsgType.BINARY and msg.data is not None:
                    await upstream_ws.send_bytes(msg.data)
                elif msg.type == WSMsgType.CLOSE:
                    break
        except Exception:
            pass
        finally:
            await upstream_ws.close()

    async def upstream_to_client():
        try:
            async for msg in upstream_ws:
                if msg.type == WSMsgType.TEXT and msg.data is not None:
                    await client_ws.send_str(msg.data)
                elif msg.type == WSMsgType.BINARY and msg.data is not None:
                    await client_ws.send_bytes(msg.data)
                elif msg.type == WSMsgType.CLOSE:
                    break
        except Exception:
            pass
        finally:
            await client_ws.close()

    await asyncio.gather(client_to_upstream(), upstream_to_client())
    return client_ws


async def handler(request):
    path = request.match_info["tail"]
    if web.WebSocketResponse().can_prepare(request).ok:
        return await handle_ws(request, upstream_ws_url(path))
    return await handle_http(request, urljoin(TARGET_BASE + "/", path or "index.html"))


async def on_startup(app):
    app["session"] = ClientSession()


async def on_cleanup(app):
    await app["session"].close()


app = web.Application()
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)
app.router.add_route("*", "/{tail:.*}", handler)

if __name__ == "__main__":
    web.run_app(app, host=LISTEN, port=PORT)
