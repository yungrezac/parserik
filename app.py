
import http.server
import socketserver
import json
import os
import subprocess
from urllib.parse import urlparse, parse_qs

# Import the run_parser function from main.py
from main import run_parser

PORT = 8080

class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query = parse_qs(parsed_path.query)

        if path == '/':
            self.path = '/public/index.html'
            return http.server.SimpleHTTPRequestHandler.do_GET(self)

        if path == '/run-parser':
            seller_id = query.get('seller_id', [''])[0]
            brand_id = query.get('brand_id', [''])[0]
            subcategory_id = query.get('subcategory_id', [''])[0]

            if not seller_id:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Seller ID is required"}).encode('utf-8'))
                return

            try:
                subprocess.Popen(["python", "-c", f'from main import run_parser; run_parser("{seller_id}", "{brand_id}", "{subcategory_id}")'])
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"message": "Parser started successfully!"}).encode('utf-8'))
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))
            return

        if path == '/get-downloads':
            downloads_path = os.path.join(os.getcwd(), 'downloads')
            if not os.path.exists(downloads_path):
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps([]).encode('utf-8'))
                return

            files = [f for f in os.listdir(downloads_path) if f.endswith('.xlsx')]
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(files).encode('utf-8'))
            return

        if path == '/get-subcategories':
            try:
                with open('subcategories.json', 'r', encoding='utf-8') as f:
                    subcategories = json.load(f)
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(subcategories).encode('utf-8'))
            except FileNotFoundError:
                self.send_response(404)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "subcategories.json not found"}).encode('utf-8'))
            return
        
        # Serve files from the public directory
        if path.startswith('/public/'):
            return http.server.SimpleHTTPRequestHandler.do_GET(self)
        
        # Serve files from the downloads directory
        if path.startswith('/downloads/'):
            return http.server.SimpleHTTPRequestHandler.do_GET(self)

        return http.server.SimpleHTTPRequestHandler.do_GET(self)


Handler = MyHttpRequestHandler

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print("serving at port", PORT)
    httpd.serve_forever()
