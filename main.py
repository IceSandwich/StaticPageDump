import asyncio
import os
import argparse
from aiohttp import web, WSMsgType
from urllib.parse import urljoin, urlparse
import typing
import httpx
import websockets
from python_socks.async_.asyncio import Proxy

class RequestManager:
	def __init__(self, base_url: str, proxy_server: typing.Optional[str] = None):
		self.base_url = base_url # http://127.0.0.1:8188
		self.proxy_server = proxy_server # socks5://127.0.0.1:1080
		# 使用 httpx.Cookies 对象统一管理 Cookie（支持持久化、过期处理等）
		self._cookies = httpx.Cookies()

	def _get_full_url(self, url: str):
		if url.startswith(("http://", "https://", "ws://", "wss://")):
			return url
		return f"{self.base_url}{url}"

	async def request_get(self, url: str) -> str:
		full_url = self._get_full_url(url)
		async with httpx.AsyncClient(proxy=self.proxy_server) as client:
			resp = await client.get(full_url)
			return resp.read().decode('utf-8')

	async def request_post(self, url: str, data: typing.Dict[str, typing.Any]):
		full_url = self._get_full_url(url)
		async with httpx.AsyncClient(proxy=self.proxy_server, cookies=self._cookies) as client:
			resp = await client.post(full_url, json=data)
			# 更新存储的 Cookie：将响应中新返回的 Set-Cookie 合并进来
			self._cookies.update(resp.cookies)
			return resp.read().decode('utf-8')

	async def request(self, method: str, url: str, headers: typing.Optional[typing.Dict[str, str]] = None, data: typing.Any = None) -> httpx.Response:
		"""发送任意方法的上游请求，返回原始响应（保留状态码、头部与二进制内容）"""
		full_url = self._get_full_url(url)
		async with httpx.AsyncClient(proxy=self.proxy_server, cookies=self._cookies, headers=headers) as client:
			resp = await client.request(method, full_url, content=data)
			# 更新存储的 Cookie：将响应中新返回的 Set-Cookie 合并进来
			self._cookies.update(resp.cookies)
			return resp

	class WebSocketRequest:
		def __init__(self, base_url: str, url: str, args: typing.Dict[str, typing.Any], proxy_server: typing.Optional[str] = None):
			if url.startswith(("ws://", "wss://")):
				self.url = url
			else:
				self.url = f"{base_url}{url}"

			self.args = args
			self._connection = None
			self.proxy_server = proxy_server

			parsed_result = urlparse(base_url)
			self.hostname = parsed_result.hostname
			self.port = parsed_result.port
			if self.port is None:
				if parsed_result.scheme == 'https':
					self.port = 443
				else:
					self.port = 80

		async def __aenter__(self) -> websockets.legacy.client.WebSocketClientProtocol:
			if self.proxy_server and 'sock' not in self.args:
				proxy = Proxy.from_url(self.proxy_server)
				sock = await proxy.connect(
					dest_host=self.hostname,
					dest_port=self.port,
				)
				self.args['sock'] = sock
			
			# 建立连接，保存到 self.ws
			self._connection = await websockets.connect(self.url, **self.args)
			# 返回 WebSocket 对象，供 async with 块内使用
			return self._connection

		async def __aexit__(self, exc_type, exc_val, exc_tb):
			# 退出时关闭连接（如果存在）
			if self._connection is not None:
				await self._connection.close()
			if 'sock' in self.args:
				del self.args['sock']

	def _get_cookie_header(self) -> str | None:
		"""从 _cookies 生成 Cookie 头字符串（如果存在）"""
		if not self._cookies: return None
		cookie_parts = [f"{k}={v}" for k, v in self._cookies.items()]
		return "; ".join(cookie_parts) if cookie_parts else None

	def connect_ws(self, url: str, args: typing.Dict[str, typing.Any] = {}):
		ws_baseurl = self.base_url.replace('http://', 'ws://').replace('https://', 'wss://')
		if 'extra_headers' not in args:
			args["extra_headers"] = {}
		if 'cookie' not in args["extra_headers"]:
			header = self._get_cookie_header()
			if header:
				args["extra_headers"]['cookie'] = header
		return self.WebSocketRequest(ws_baseurl, url, args, self.proxy_server)

# 真实远端站点
parser = argparse.ArgumentParser()
parser.add_argument("--url", type=str, required=True)
parser.add_argument("--listen", type=str, default="127.0.0.1")
parser.add_argument("--output", type=str, required=True)
parser.add_argument("--port", type=int, default=8080)
parser.add_argument("--proxy", type=str, default=None)
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

rm = RequestManager(TARGET_BASE, args.proxy)


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

    data = await request.read() if request.method in ("POST", "PUT", "PATCH") else None
    resp = await rm.request(request.method, real_url, headers=forward_headers(request), data=data)
    content = await resp.aread()
    content_type = resp.headers.get("Content-Type", "")

    if resp.status_code == 200 and request.method == "GET" and should_cache(real_url):
        save_cache(real_url, content)

    headers = {"Content-Type": content_type} if content_type else {}
    return web.Response(body=content, status=resp.status_code, headers=headers)


async def handle_ws(request, real_url):
    print(real_url)
    client_ws = web.WebSocketResponse(compress=False)
    await client_ws.prepare(request)

    try:
        async with rm.connect_ws(real_url, {"extra_headers": forward_headers(request)}) as upstream_ws:
            async def client_to_upstream():
                try:
                    async for msg in client_ws:
                        if msg.type == WSMsgType.TEXT and msg.data is not None:
                            await upstream_ws.send(msg.data)
                        elif msg.type == WSMsgType.BINARY and msg.data is not None:
                            await upstream_ws.send(msg.data)
                        elif msg.type == WSMsgType.CLOSE:
                            await upstream_ws.close()
                            break
                except Exception:
                    pass
                finally:
                    try:
                        await upstream_ws.close()
                    except Exception:
                        pass

            async def upstream_to_client():
                try:
                    while True:
                        msg = await upstream_ws.recv()
                        if isinstance(msg, bytes):
                            await client_ws.send_bytes(msg)
                        else:
                            await client_ws.send_str(msg)
                except Exception:
                    pass
                finally:
                    await client_ws.close()

            await asyncio.gather(client_to_upstream(), upstream_to_client())
    except Exception:
        await client_ws.close()
    return client_ws


async def handler(request):
    path = request.match_info["tail"]
    if web.WebSocketResponse().can_prepare(request).ok:
        return await handle_ws(request, upstream_ws_url(path))
    return await handle_http(request, urljoin(TARGET_BASE + "/", path or "index.html"))


app = web.Application()
app.router.add_route("*", "/{tail:.*}", handler)

if __name__ == "__main__":
    web.run_app(app, host=LISTEN, port=PORT)
