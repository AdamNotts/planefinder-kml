#!/usr/bin/env python3
"""
Main System - Glue code that wires all components together
Extracted from working prototype - identical functionality, clean separation
"""

import time
from typing import Dict, Any

# Import our separated components
from firehose_client import FirehoseClient, FirehoseConfig
from data_processor import DataProcessor
from kmz_generator import OptimizedKMZGenerator
from web_server import OptimizedKMZServer

class CellularOptimizedKMZSystem:
    """Cellular-optimized aircraft tracking system with minimal KMZ"""
    
    def __init__(self, 
                 firehose_config: FirehoseConfig = None,
                 server_port: int = 7305,
                 server_host: str = "0.0.0.0",
                 kmz_refresh_interval: int = 1,
                 altitude_filter: int = 10000,
                 min_altitude_filter: int = 100,
                 aircraft_persistence_time: int = 15,
                 external_url: str = None):
        
        # Initialize components with cellular optimizations
        self.data_processor = DataProcessor({
            "max_altitude": altitude_filter,
            "min_altitude": min_altitude_filter  # Filter out low altitude/ground aircraft
        })
        self.firehose_client = FirehoseClient(firehose_config, self.data_processor)
        self.kmz_generator = OptimizedKMZGenerator(kmz_refresh_interval, aircraft_persistence_time)
        self.web_server = OptimizedKMZServer(self.kmz_generator, server_port, server_host, external_url)
        
        # Connect data processor to KMZ generator
        self.data_processor.add_callback(self.kmz_generator.update_aircraft_data)
        
        self.running = False
    
    def start(self):
        """Start the cellular-optimized system"""
        if self.running:
            print("System already running")
            return
        
        print("Starting Cellular-Optimized Aircraft KMZ System with Prediction Lines...")
        print("=" * 60)
        print(f"📱 CELLULAR OPTIMIZATIONS ENABLED:")
        print(f"   • Ground aircraft filtering: ON (is_on_ground + altitude < {self.data_processor.filters['min_altitude']}ft)")
        print(f"   • Aviation altitude format: 3-digit hundreds (009=900ft, 095=9500ft)")
        print(f"   • Climb/descent arrows: ^ for >+200fpm, v for <-200fpm")
        print(f"   • Open diamond icons: lightweight and centered")
        print(f"   • White prediction lines: 30-second ahead projection")
        print(f"   • Minimal KMZ structure: no descriptions/styles")
        print(f"   • Reduced persistence: {self.kmz_generator.persistence_time}s")
        print(f"   • KMZ compression: enabled")
        print("=" * 60)
        
        # Start components
        self.web_server.start()
        self.firehose_client.start()
        
        self.running = True
        print("🚀 System started for cellular data efficiency!")
        print(f"📡 Firehose: Connected to Plane Finder")
        print(f"🌐 Server: http://{self.web_server.host}:{self.web_server.port}")
        print(f"📍 Minimal KMZ: http://{self.web_server.host}:{self.web_server.port}/live.kmz")
        print(f"📊 Status: http://{self.web_server.host}:{self.web_server.port}/status")
        print("=" * 60)
    
    def stop(self):
        """Stop the system"""
        if not self.running:
            return
        
        print("Stopping Cellular-Optimized System...")
        self.firehose_client.stop()
        self.web_server.stop()
        self.running = False
        print("System stopped")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get system status with cellular metrics"""
        stats = self.data_processor.get_stats()
        aircraft_count = len(self.kmz_generator.get_current_aircraft())
        
        # Estimate KMZ size (now with prediction lines)
        estimated_kmz_size = aircraft_count * 120  # ~60 bytes per aircraft + prediction line (compressed)
        
        return {
            "system_running": self.running,
            "firehose_connected": self.firehose_client.connected,
            "aircraft_count": aircraft_count,
            "estimated_kmz_size_bytes": estimated_kmz_size,
            "estimated_kmz_size_kb": round(estimated_kmz_size / 1024, 1),
            "ground_filtered": stats.get("ground_filtered", 0),
            "low_altitude_filtered": stats.get("low_altitude_filtered", 0),
            "processing_stats": stats,
            "persistence_time": self.kmz_generator.persistence_time,
            "request_count": self.web_server.request_count,
            "cellular_optimized": True,
            "format": "KMZ"
        }
    
    def print_status(self):
        """Print cellular-optimized status"""
        status = self.get_system_status()
        
        print(f"\n📱 Cellular-Optimized Status:")
        print(f"   Running: {status['system_running']}")
        print(f"   Firehose: {'Connected' if status['firehose_connected'] else 'Disconnected'}")
        print(f"   Aircraft: {status['aircraft_count']} (airborne only)")
        print(f"   Estimated KMZ: {status['estimated_kmz_size_kb']} KB (with prediction lines)")
        print(f"   Ground Filtered: {status['ground_filtered']}")
        print(f"   Low Alt Filtered: {status['low_altitude_filtered']}")
        print(f"   HTTP Requests: {status['request_count']}")
        
        stats = status['processing_stats']
        print(f"   Filter Rate: {stats['filter_pass_rate']}% pass")
        print(f"   Total Processed: {stats['total_aircraft']} aircraft")

def main():
    """Main function for cellular-optimized system"""
    print("Cellular-Optimized Aircraft Tracking System with Prediction Lines (KMZ)")
    print("Ultra-minimal KMZ for 1-second updates over cellular data")
    print("=" * 60)
    
    # Create cellular-optimized system
    system = CellularOptimizedKMZSystem(
        server_port=7305,
        server_host="0.0.0.0",
        kmz_refresh_interval=1,  # 1-second refresh
        altitude_filter=10000,   # Max altitude
        min_altitude_filter=100, # Minimum altitude (filters ground aircraft)
        aircraft_persistence_time=15,  # Reduced persistence for faster updates
        external_url="http://139.162.173.89:7305"
    )
    
    try:
        system.start()
        
        # Status monitoring
        while True:
            time.sleep(15)  # Status every 15 seconds
            system.print_status()
            
    except KeyboardInterrupt:
        print("\nShutting down...")
        system.stop()
    except Exception as e:
        print(f"System error: {e}")
        system.stop()

if __name__ == "__main__":
    main()