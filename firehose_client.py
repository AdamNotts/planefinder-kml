#!/usr/bin/env python3
"""
Firehose Client - Plane Finder connection and DLE protocol handling
Extracted from working prototype - handles connection, authentication, and data reception
"""

import json
import gzip
import socket
import ssl
import threading
import time
from dataclasses import dataclass

@dataclass
class FirehoseConfig:
    """Configuration for Firehose connection"""
    host: str = "firehose-test.planefinder.net"
    port: int = 5555
    username: str = "airbox_demo"
    password: str = "de3moaa33bvoox"
    reconnect_delay: float = 5.0
    socket_timeout: float = 30.0

class DLEProtocol:
    """Data Link Escape protocol implementation for framing"""
    
    DLE = 0x10
    STX = 0x02
    ETX = 0x03
    
    @staticmethod
    def extract_frames(buffer: bytearray) -> tuple[list[bytes], bytearray]:
        """Extract complete DLE-framed packets from buffer"""
        frames = []
        remaining_buffer = bytearray()
        
        i = 0
        while i < len(buffer) - 1:
            # Look for DLE STX
            if buffer[i] == DLEProtocol.DLE and buffer[i + 1] == DLEProtocol.STX:
                # Found start, now look for DLE ETX
                frame_start = i + 2
                j = frame_start
                frame_data = bytearray()
                
                while j < len(buffer) - 1:
                    if buffer[j] == DLEProtocol.DLE:
                        if buffer[j + 1] == DLEProtocol.ETX:
                            # Found end of frame
                            frames.append(DLEProtocol.unstuff_data(frame_data))
                            i = j + 2
                            break
                        elif buffer[j + 1] == DLEProtocol.DLE:
                            # Escaped DLE
                            frame_data.append(DLEProtocol.DLE)
                            j += 2
                        else:
                            frame_data.append(buffer[j])
                            j += 1
                    else:
                        frame_data.append(buffer[j])
                        j += 1
                else:
                    # Incomplete frame, save to remaining buffer
                    remaining_buffer.extend(buffer[i:])
                    break
            else:
                i += 1
        
        return frames, remaining_buffer
    
    @staticmethod
    def unstuff_data(data: bytearray) -> bytes:
        """Remove DLE stuffing from data"""
        return bytes(data)

class FirehoseClient:
    """Plane Finder Firehose client with DLE protocol support"""
    
    def __init__(self, config: FirehoseConfig = None, data_processor=None):
        self.config = config or FirehoseConfig()
        self.data_processor = data_processor
        self.running = False
        self.connected = False
        self.socket = None
        self.buffer = bytearray()
        
    def start(self):
        """Start the firehose client in a separate thread"""
        if self.running:
            return
        
        self.running = True
        thread = threading.Thread(target=self._run_client, daemon=True)
        thread.start()
        print("Firehose client started")
    
    def stop(self):
        """Stop the firehose client"""
        self.running = False
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
        print("Firehose client stopped")
    
    def _run_client(self):
        """Main client loop with reconnection"""
        while self.running:
            try:
                self._connect_and_run()
            except Exception as e:
                print(f"Connection error: {e}")
                if self.running:
                    print(f"Reconnecting in {self.config.reconnect_delay} seconds...")
                    time.sleep(self.config.reconnect_delay)
    
    def _connect_and_run(self):
        """Connect to firehose and process data"""
        # Create TLS connection
        context = ssl.create_default_context()
        sock = socket.create_connection((self.config.host, self.config.port))
        sock.settimeout(self.config.socket_timeout)
        
        self.socket = context.wrap_socket(sock, server_hostname=self.config.host)
        print(f"Connected to {self.config.host}:{self.config.port}")
        
        # Authenticate
        auth_data = {
            "username": self.config.username,
            "password": self.config.password
        }
        auth_json = json.dumps(auth_data) + "\n"
        self.socket.send(auth_json.encode())
        print("Authentication sent")
        
        self.connected = True
        self.buffer.clear()
        
        # Process data
        while self.running and self.connected:
            try:
                data = self.socket.recv(8192)
                if not data:
                    print("Connection closed by server")
                    break
                
                self.buffer.extend(data)
                self._process_buffer()
                
            except socket.timeout:
                continue
            except Exception as e:
                print(f"Data processing error: {e}")
                break
        
        self.connected = False
        if self.socket:
            self.socket.close()
    
    def _process_buffer(self):
        """Process received data for DLE frames"""
        frames, self.buffer = DLEProtocol.extract_frames(self.buffer)
        
        for frame in frames:
            try:
                # Check if gzipped
                if len(frame) >= 2 and frame[0] == 0x1f and frame[1] == 0x8b:
                    try:
                        decompressed = gzip.decompress(frame)
                        aircraft_data = json.loads(decompressed.decode())
                        if aircraft_data and aircraft_data != {} and self.data_processor:
                            self.data_processor.process_aircraft_data(aircraft_data)
                    except Exception as e:
                        # Occasional gzip errors are normal with partial packets
                        pass
                else:
                    # Uncompressed JSON
                    aircraft_data = json.loads(frame.decode())
                    if aircraft_data and aircraft_data != {} and self.data_processor:
                        self.data_processor.process_aircraft_data(aircraft_data)
                        
            except Exception as e:
                print(f"Frame processing error: {e}")