#!/usr/bin/env python3
"""
Web Server - HTTP endpoints for KMZ serving
Extracted from working prototype - handles HTTP requests and KMZ delivery
"""

import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from datetime import datetime

class OptimizedKMZServer:
    """Lightweight HTTP server for minimal KMZ serving"""
    
    def __init__(self, kmz_generator, port: int = 7305, host: str = "0.0.0.0", external_url: str = None):
        self.kmz_generator = kmz_generator
        self.port = port
        self.host = host
        self.external_url = external_url
        self.server = None
        self.server_thread = None
        self.running = False
        self.request_count = 0
        
    def start(self):
        """Start the HTTP server"""
        if self.running:
            return
        
        handler = self._create_request_handler()
        self.server = HTTPServer((self.host, self.port), handler)
        self.running = True
        
        self.server_thread = threading.Thread(target=self._run_server, daemon=True)
        self.server_thread.start()
        
        print(f"Optimized KMZ server started on http://{self.host}:{self.port}")
        print(f"Minimal KMZ: http://{self.host}:{self.port}/live.kmz")
    
    def stop(self):
        """Stop the HTTP server"""
        if not self.running:
            return
        
        self.running = False
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        
        print("Optimized KMZ server stopped")
    
    def _run_server(self):
        """Run the HTTP server"""
        try:
            self.server.serve_forever()
        except Exception as e:
            if self.running:
                print(f"Server error: {e}")
    
    def _create_request_handler(self):
        """Create minimal request handler"""
        kmz_gen = self.kmz_generator
        server_instance = self
        
        class MinimalKMZHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                """Minimal logging for cellular optimization tracking"""
                server_instance.request_count += 1
                if server_instance.request_count % 10 == 0:  # Log every 10th request
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Request #{server_instance.request_count}: {format % args}")
            
            def do_GET(self):
                """Handle GET requests with minimal overhead"""
                try:
                    parsed_path = urlparse(self.path)
                    path = parsed_path.path
                    
                    if path == "/live.kmz" or path == "/":
                        # Main KMZ with NetworkLink
                        if server_instance.external_url:
                            base_url = server_instance.external_url
                        elif server_instance.host == "0.0.0.0":
                            base_url = "http://139.162.173.89:7305"
                        else:
                            base_url = f"http://{server_instance.host}:{server_instance.port}"
                        
                        kml_content = kmz_gen.generate_main_kml(base_url)
                        kmz_data = kmz_gen.create_kmz_from_kml(kml_content)
                        self._send_kmz_response(kmz_data)
                        
                    elif path == "/aircraft.kmz":
                        # Minimal aircraft data with prediction lines
                        kml_content = kmz_gen.generate_minimal_kml()
                        kmz_data = kmz_gen.create_kmz_from_kml(kml_content)
                        aircraft_count = len(kmz_gen.get_current_aircraft())
                        
                        # Count prediction lines
                        prediction_count = kml_content.count('<styleUrl>#prediction</styleUrl>')
                        
                        # Log size information and show climb/descent samples
                        if server_instance.request_count % 20 == 0:
                            sample_aircraft = kmz_gen.get_current_aircraft()
                            if sample_aircraft:
                                sample_alt = sample_aircraft[0].get('altitude', 0)
                                sample_vert = sample_aircraft[0].get('vert_rate', 0)
                                sample_formatted = f"{round(sample_alt / 100):03d}"
                                
                                # Show arrow logic in debug
                                if sample_vert > 200:
                                    sample_formatted = f"^{sample_formatted}"
                                elif sample_vert < -200:
                                    sample_formatted = f"v{sample_formatted}"
                                
                                print(f"[CELLULAR] KMZ size: {len(kmz_data)} bytes, {aircraft_count} aircraft, {prediction_count} prediction lines")
                                print(f"[ARROWS] Sample: {sample_alt}ft, {sample_vert:+}fpm -> '{sample_formatted}'")
                            else:
                                print(f"[CELLULAR] KMZ size: {len(kmz_data)} bytes, {aircraft_count} aircraft, {prediction_count} prediction lines")
                        
                        self._send_kmz_response(kmz_data)
                        
                    elif path == "/status":
                        # Minimal status
                        aircraft_count = len(kmz_gen.get_current_aircraft())
                        status = {
                            "aircraft": aircraft_count,
                            "requests": server_instance.request_count,
                            "persistence": kmz_gen.persistence_time,
                            "running": True,
                            "format": "KMZ"
                        }
                        self._send_json_response(status)
                        
                    elif path == "/test":
                        # Simple test page
                        aircraft_count = len(kmz_gen.get_current_aircraft())
                        html = f"<html><body><h1>Cellular-Optimized KMZ Server</h1><p>Aircraft: {aircraft_count}</p><p>Requests: {server_instance.request_count}</p><a href='/live.kmz'>Live KMZ</a></body></html>"
                        self._send_html_response(html)
                        
                    else:
                        self._send_error_response(404, "Not Found")
                        
                except Exception as e:
                    print(f"Request error: {e}")
                    self._send_error_response(500, "Internal Server Error")
            
            def _send_kmz_response(self, kmz_data: bytes):
                """Send KMZ (zipped KML) for better compression"""
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.google-earth.kmz")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(kmz_data)
            
            def _send_json_response(self, data: dict):
                """Send minimal JSON response"""
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode('utf-8'))
            
            def _send_html_response(self, html: str):
                """Send HTML response"""
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(html.encode('utf-8'))
            
            def _send_error_response(self, code: int, message: str):
                """Send minimal error response"""
                self.send_response(code)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(f"{code} {message}".encode('utf-8'))
        
        return MinimalKMZHandler