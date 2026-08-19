import json
import os
import argparse
from urllib.parse import urlparse

import aiofiles
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
import typing
import os
from mirror_server import MirrorServer

class CacheServer(MirrorServer):
	def __init__(self, upstream: str, listen: str, port: int, proxy: typing.Optional[str] = None, cache_dir: str = "cached"):
		super().__init__(upstream, listen, port, proxy)
		self.cache_dir = cache_dir

	def _get_cache_host(self):
		parsed = urlparse(self.upstream)
		host = parsed.netloc.replace(":", "_")
		return host

	# ---------- 缓存工具函数 ----------
	def _get_cache_path(self, url: str):
		"""
		https://example.com:8443/a/b/c.js
		-> ./example.com_8443/a/b/c.js
		"""
		parsed = urlparse(url)

		host = self._get_cache_host()
		path = parsed.path

		if not path or path.endswith("/"):
			path += "index.html"

		return os.path.join(self.cache_dir, host, path.lstrip("/"))
	
	def _get_meta_path(self, cache_path: str):
		return cache_path + ".meta"

	async def _read_cache(self, body_path: str):
		"""读取缓存元数据和内容，返回 (status_code, headers, body_generator)"""
		meta_path = self._get_meta_path(body_path)
		if not os.path.exists(meta_path) or not os.path.exists(body_path):
			return None
		async with aiofiles.open(meta_path, "r") as f:
			meta = json.loads(await f.read())
		status = meta["status"]
		headers = meta["headers"]  # list of [key, value]

		# 流式读取 body
		async def body_stream():
			async with aiofiles.open(body_path, "rb") as f:
				while chunk := await f.read(1024 * 64):  # 64KB
					yield chunk

		return status, headers, body_stream()

	async def _write_cache(self, url: str, status_code: int, headers: typing.Dict[str, typing.Any], body_iter):
		"""将响应写入缓存，body_iter 是异步可迭代对象（如 aiter_bytes）"""
		body_path = self._get_cache_path(url)
		meta_path = self._get_meta_path(body_path)
		# 暂存到临时文件，防止写入过程中被读取
		temp_meta = meta_path + ".tmp"
		temp_body = body_path + ".tmp"

		os.makedirs(os.path.dirname(body_path), exist_ok=True)

		# 先写临时 body，同时收集内容（为了可以同时返回给客户端，这里 caller 会负责流式传输）
		# 但由于我们要同时写缓存，需要将 body_iter 复制一份：用 tee 或直接收集再写
		# 最稳妥方式：收集全部内容（如果响应体很大可能内存爆炸，但通常代理场景可接受）
		# 或者使用 asyncio.Queue 一边读一边写，但较复杂。
		# 为简化，这里先收集完整内容（适合中等大小响应）。
		# 对于大文件代理，建议改用更复杂的流式双写，但此处示例保持清晰。
		body_chunks = []
		async for chunk in body_iter:
			body_chunks.append(chunk)
		full_body = b"".join(body_chunks)

		# 写入临时 body
		async with aiofiles.open(temp_body, "wb") as f:
			await f.write(full_body)

		# 写入 meta
		meta = {
			"status": status_code,
			"headers": [[k, v] for k, v in headers.items()]  # 转为 list 保存
		}
		async with aiofiles.open(temp_meta, "w") as f:
			await f.write(json.dumps(meta))

		# 原子替换（rename 在 POSIX 上是原子的）
		os.replace(temp_meta, meta_path)
		os.replace(temp_body, body_path)

		# 返回完整的 body 用于响应
		return full_body
	
	def _should_cache(self, request: Request):
		return False

	async def _all_http_handler(self, request):
		# 判断是否为 WebSocket 升级请求（Upgrade 头），但 Starlette 已分开处理，不会进入此路由
		# 判断是否为缓存前缀
		# print("=====", request.url)
		is_cacheable = self._should_cache(request)

		# 如果是缓存请求，尝试命中缓存
		if is_cacheable:
			body_path = self._get_cache_path(str(request.url))
			cache_hit = await self._read_cache(body_path)
			if cache_hit:
				# print(f"[+] {request.method} {request.url} <--- Cache")
				status, headers, body_stream = cache_hit
				# 返回缓存响应
				return StreamingResponse(
					body_stream,
					status_code=status,
					headers=dict(headers),
				)

		resp = await super()._all_http_handler(request)
		if is_cacheable and resp.status_code == 200:
			# 读取响应 body 并缓存，同时返回
			full_body = await self._write_cache(str(request.url), resp.status_code, resp.headers, resp.body_iterator)
			print(f"[+] {request.method} {request.url} ---> Cache")
			return Response(full_body, status_code=resp.status_code, headers=dict(resp.headers))

		return resp

class StaticDumpServer(CacheServer):
	def __init__(self, upstream, listen, port, proxy = None, cache_dir = "cached", cache_prefix: typing.List[str] = []):
		super().__init__(upstream, listen, port, proxy, cache_dir)
		self.cache_prefix = cache_prefix

	def _should_cache(self, request):
		path = urlparse(str(request.url)).path
		return any(path.startswith(prefix) for prefix in self.cache_prefix)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--host", default="127.0.0.1", help="Listen host")
	parser.add_argument("--port", type=int, default=8000, help="Listen port")
	parser.add_argument("--upstream", required=True, help="Upstream server URL, e.g., http://127.0.0.1:8080")
	parser.add_argument("--cache-prefix", type=str, action='append', help="URL prefix to cache")
	parser.add_argument("--cache-dir", default="./dump", help="Directory to store cached files (default: ./dump)")
	parser.add_argument("--proxy", type=str, default=None)
	args = parser.parse_args()

	server = StaticDumpServer(args.upstream, args.host, args.port, args.proxy, args.cache_dir, args.cache_prefix)
	server.start()
