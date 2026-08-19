import asyncio
from urllib.parse import urlparse, urlunparse, quote
import httpx
from starlette.applications import Starlette
from starlette.routing import Route, WebSocketRoute
from starlette.requests import Request
from starlette.responses import StreamingResponse
from starlette.websockets import WebSocket, WebSocketDisconnect
import websockets
from python_socks.async_.asyncio import Proxy
import typing
import argparse

class MirrorServer:
	def __init__(self, upstream: str, listen: str, port: int, proxy: typing.Optional[str] = None):
		self.upstream = upstream.rstrip('/')
		self.listen = listen
		self.port = port
		self.proxy = proxy

		# 全局 HTTP 客户端（在 startup 中初始化）
		self.http_client: typing.Optional[httpx.AsyncClient] = None

	def _get_upstream_url(self, request: Request):
		# 构造上游完整 URL
		# 获取原始路径（未解码）
		# Solve weird request path from comfy, like 
		# - /api/userdata/subgraphs%2FTrellis2%20GetSlice.json
		# but request.url.path is
		# - /api/userdata/subgraphs/Trellis2%20GetSlice.json
		raw_path = request.scope.get("raw_path")
		if raw_path is not None:
			path_with_encoding = raw_path.decode('utf-8')
		else:
			# 回退方案：对解码后的路径重新编码（但会丢失 %2F 的意图）
			path_with_encoding = quote(request.url.path, safe="/")
		# encoded_path = quote(request.url.path, safe="/")
		upstream_url = self.upstream + path_with_encoding
		if request.url.query:
			upstream_url += "?" + request.url.query
		return upstream_url

	# ---------- HTTP 请求处理 ----------
	async def _all_http_handler(self, request: Request):
		upstream_url = self._get_upstream_url(request)

		# 未命中缓存 或 非缓存请求 → 转发到上游
		# 转发请求头，但去掉 Host、Connection 等
		headers = dict(request.headers)
		headers.pop("host", None)
		headers.pop("connection", None)

		# 获取请求 body（如果有）
		body = await request.body()

		# 发起上游请求
		resp = await self.http_client.request(
			method=request.method,
			url=upstream_url,
			headers=headers,
			content=body if body else None,
			follow_redirects=False,
		)

		return StreamingResponse(
			resp.aiter_bytes(),
			status_code=resp.status_code,
			headers=dict(resp.headers),
		)
		
	# ---------- WebSocket 双向转发 ----------
	async def _all_ws_handler(self, websocket: WebSocket):
		"""处理 WebSocket 连接：双向转发"""
		await websocket.accept()

		# 构造上游 WebSocket URL
		# 注意：需要保留原路径和查询参数，但不包括 scheme/host
		path = websocket.url.path
		query = websocket.url.query

		parsed = urlparse(self.upstream)
		UPSTREAM_WS = urlunparse(("wss" if parsed.scheme == "https" else "ws", parsed.netloc, "", "", "", ""))

		upstream_ws_url = UPSTREAM_WS + path
		if query:
			upstream_ws_url += "?" + query

		try:
			# 连接上游 WebSocket
			extra_args = {}
			if self.proxy:
				parsed_result = urlparse(upstream_ws_url)
				hostname = parsed_result.hostname
				port = parsed_result.port
				if port is None:
					if parsed_result.scheme == 'https':
						port = 443
					else:
						port = 80
				proxy = Proxy.from_url(self.proxy)
				sock = await proxy.connect(
					dest_host=hostname,
					dest_port=port,
				)
				extra_args['sock'] = sock
			async with websockets.connect(upstream_ws_url, **extra_args) as upstream_ws:
				# 定义两个转发任务
				async def forward_to_upstream():
					try:
						while True:
							msg = await websocket.receive_text()
							await upstream_ws.send(msg)
					except WebSocketDisconnect:
						# 客户端断开，结束循环
						pass
					except Exception as e:
						print(f"Error forwarding to upstream: {e}")
					finally:
						try:
							await upstream_ws.close()
						except Exception:
							pass

				async def forward_to_client():
					try:
						while True:
							msg = await upstream_ws.recv()
							if isinstance(msg, bytes):
								await websocket.send_bytes(msg)
							else:
								await websocket.send_text(msg)
					except websockets.exceptions.ConnectionClosed:
						pass
					except Exception as e:
						print(f"Error forwarding to client: {e}")
					finally:
						try:
							await websocket.close()
						except Exception:
							pass

				# 并发运行两个方向
				await asyncio.gather(
					forward_to_upstream(),
					forward_to_client(),
					return_exceptions=True
				)
		except Exception as e:
			print(f"WebSocket connection error: {e}")
			try:
				await websocket.close(code=1011)  # 内部错误
			except:
				pass

	async def _startup(self):
		self.http_client = httpx.AsyncClient(proxy=self.proxy, timeout=60.0)

	async def _shutdown(self):
		await self.http_client.aclose()
		self.http_client = None

	def start(self):
		# ---------- 创建应用 ----------
		import uvicorn
		app = Starlette(routes=[
			Route("/{path:path}", self._all_http_handler, methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]),          # 所有 HTTP 请求
			WebSocketRoute("/{path:path}", self._all_ws_handler),   # 所有 WebSocket 请求
		])
		app.add_event_handler("startup", self._startup)
		app.add_event_handler("shutdown", self._shutdown)
		uvicorn.run(app, host=self.listen, port=self.port)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--host", default="127.0.0.1", help="Listen host")
	parser.add_argument("--port", type=int, default=8000, help="Listen port")
	parser.add_argument("--upstream", required=True, help="Upstream server URL, e.g., http://127.0.0.1:8080")
	parser.add_argument("--proxy", type=str, default=None)
	args = parser.parse_args()

	server = MirrorServer(args.upstream, args.host, args.port, args.proxy)
	server.start()
