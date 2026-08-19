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

		# 1. 构建请求对象
		timeout = httpx.Timeout(None, connect=60.0)
		req = self.http_client.build_request(
			method=request.method,
			url=upstream_url,
			headers=headers,
			content=body if body else None,
			timeout=timeout
		)

		# 2. 发送请求（stream=True 使响应体延迟读取）
		#    连接超时 10s，读取超时设为 None（无限），以便流式可长期保持
		resp = await self.http_client.send(req, stream=True)

		# 3. 检查响应状态（若有异常，先关闭响应再抛出）
		# try:
		# 	resp.raise_for_status()
		# except Exception:
		# 	await resp.aclose()
		# 	raise

		# 4. 根据 Content-Type 决定处理方式
		content_type = resp.headers.get("content-type", "").lower()
		is_stream = any(
			ct in content_type
			for ct in ("application/x-ndjson", "text/event-stream")
		)

		if is_stream:
			# print("is stream for", req.url, "content-type=", content_type)
			# ------ 流式响应：直接转发，不限制读取 ------
			async def stream_generator():
				try:
					async for chunk in resp.aiter_bytes():
						yield chunk
				finally:
					await resp.aclose()   # 关闭连接，释放资源

			return StreamingResponse(
				stream_generator(),
				status_code=resp.status_code,
				headers=dict(resp.headers)
			)
		else:
			# ------ 非流式响应：在有限超时内读取完整内容 ------
			try:
				# 应用 60 秒读取超时
				data = await asyncio.wait_for(resp.aread(), timeout=60.0)
			except asyncio.TimeoutError:
				await resp.aclose()
				raise httpx.ReadTimeout("读取上游响应超时（60s）")
			except Exception:
				await resp.aclose()
				raise
			else:
				await resp.aclose()   # 读取完毕立即关闭

			# 返回一个一次性产生完整数据的 StreamingResponse
			async def content_iterator():
				yield data

			return StreamingResponse(
				content_iterator(),
				status_code=resp.status_code,
				headers=dict(resp.headers)
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
		self.http_client = httpx.AsyncClient(proxy=self.proxy)

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
