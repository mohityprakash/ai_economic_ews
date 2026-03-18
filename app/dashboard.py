from __future__ import annotations

from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import webbrowser

ROOT = Path(__file__).resolve().parent
PORT = 8501


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", PORT), DashboardHandler)
    print(f"Serving dashboard at http://127.0.0.1:{PORT}/index.html")
    try:
        webbrowser.open(f"http://127.0.0.1:{PORT}/index.html")
    except Exception:
        pass
    server.serve_forever()
