from static_dumper import CacheServer
import os
import argparse
from urllib.parse import urlparse, parse_qs
import os

class ComfyStaticDumpServer(CacheServer):
	def _get_output_view(self, url: str):
		parsed = urlparse(url)
		if parsed.path != '/api/view': return None
		query = parse_qs(parsed.query)
		filename = query.get('filename', [])
		viewtype = query.get('type', [''])
		subfolder = query.get('subfolder', [])
		if len(filename) == 1 and viewtype[0] == 'temp':
			return filename[0], "output", "temp"
		elif len(filename) == 1 and viewtype[0] == 'output' and len(subfolder) == 1:
			return filename[0], "output", subfolder[0]
		elif len(filename) == 1 and viewtype[0] == 'input':
			return filename[0], "output", "input"
		print(f"[-] wrong api view url {url}, components {filename} - {viewtype} - {subfolder}")
		return None
	
	def _should_cache(self, request):
		path = request.url.path
		prefixes = [
			"/assets", 
			"/materialdesignicons.min.css", 
			"/fonts/", 
			"/extensions/", 
			"/scripts/", 
			"/api/global_subgraphs",
			"/extensions/",
			"/scripts/"
		]
		for prefix in prefixes:
			if path.startswith(prefix):
				return True
		if path == '/api/view':
			res = self._get_output_view(str(request.url))
			if res is not None:
				return True
		if path == '/' or path == '':
			print("[+] Detect root path: ", path)
			return True
		return False
	
	def _get_cache_path(self, url):
		parsed = urlparse(url)
		host = self._get_cache_host()
		if parsed.path == '/api/view':
			res = self._get_output_view(url)
			if res:
				return os.path.join(self.cache_dir, host, res[1], res[2], res[0])
		if parsed.path == '/api/global_subgraphs':
			return os.path.join(self.cache_dir, host, "api", "global_subgraphs.index")
		return super()._get_cache_path(url)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--host", default="127.0.0.1", help="Listen host")
	parser.add_argument("--port", type=int, default=8000, help="Listen port")
	parser.add_argument("--upstream", required=True, help="Upstream server URL, e.g., http://127.0.0.1:8080")
	parser.add_argument("--cache-dir", default="./dump", help="Directory to store cached files (default: ./dump)")
	parser.add_argument("--proxy", type=str, default=None)
	args = parser.parse_args()

	server = ComfyStaticDumpServer(args.upstream, args.host, args.port, args.proxy, args.cache_dir)
	server.start()
