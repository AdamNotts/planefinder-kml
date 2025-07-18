#!/usr/bin/env python3
"""
KMZ Generator - KML creation and ZIP compression
Extracted from working prototype - handles aircraft visualization and prediction lines
"""

import time
import threading
import math
import zipfile
import io
from typing import Dict, List
from datetime import datetime, timezone

class OptimizedKMZGenerator:
    """Ultra-minimal KMZ generator optimized for cellular data usage"""
    
    def __init__(self, refresh_interval: int = 1, persistence_time: int = 15):
        self.refresh_interval = refresh_interval
        self.persistence_time = persistence_time  # Reduced from 30 to 15 seconds
        self.aircraft_database: Dict[str, Dict] = {}
        self.last_update_time = None
        self._lock = threading.Lock()
        
    def update_aircraft_data(self, aircraft_list: List[Dict]):
        """Update aircraft data with persistence"""
        with self._lock:
            current_time = time.time()
            self.last_update_time = datetime.now(timezone.utc)
            
            # Update existing aircraft or add new ones
            for aircraft in aircraft_list:
                aircraft_id = aircraft.get("adshex")
                if not aircraft_id:
                    continue
                
                # Store minimal data + heading/speed for prediction lines
                minimal_aircraft = {
                    "adshex": aircraft_id,
                    "lat": aircraft.get("lat"),
                    "lon": aircraft.get("lon"),
                    "altitude": aircraft.get("altitude", 0),
                    "vert_rate": aircraft.get("vert_rate", 0),
                    "heading": aircraft.get("heading"),  # For prediction lines
                    "speed": aircraft.get("speed"),      # For prediction lines
                    "last_seen_timestamp": current_time,
                    "age_seconds": 0
                }
                
                self.aircraft_database[aircraft_id] = minimal_aircraft
            
            # Remove expired aircraft
            expired_aircraft = []
            for aircraft_id, aircraft_data in self.aircraft_database.items():
                age = current_time - aircraft_data["last_seen_timestamp"]
                aircraft_data["age_seconds"] = age
                
                if age > self.persistence_time:
                    expired_aircraft.append(aircraft_id)
            
            for aircraft_id in expired_aircraft:
                del self.aircraft_database[aircraft_id]
    
    def get_current_aircraft(self) -> List[Dict]:
        """Get current aircraft list"""
        with self._lock:
            return list(self.aircraft_database.values())
    
    def generate_minimal_kml(self) -> str:
        """Generate ultra-minimal KML for cellular efficiency WITH PREDICTION LINES"""
        current_aircraft = self.get_current_aircraft()
        
        # Start with minimal KML structure
        kml_parts = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<kml xmlns="http://www.opengis.net/kml/2.2">',
            '<Document>',
            # Aircraft icon style with high contrast colors
            '<Style id="aircraft">',
            '<IconStyle>',
            '<Icon>',
            '<href>http://maps.google.com/mapfiles/kml/shapes/open-diamond.png</href>',
            '</Icon>',
            '<scale>0.8</scale>',
            '<color>ffff00ff</color>',  # Bright magenta (AABBGGRR format)
            '</IconStyle>',
            '<LabelStyle>',
            '<scale>0.9</scale>',
            '<color>ffff00ff</color>',  # Matching magenta text
            '</LabelStyle>',
            '</Style>',
            # Prediction line style with matching color
            '<Style id="prediction">',
            '<LineStyle>',
            '<color>ffff00ff</color>',  # Bright magenta to match icons
            '<width>2</width>',
            '</LineStyle>',
            '</Style>'
        ]
        
        # Add aircraft with prediction lines
        for aircraft in current_aircraft:
            lat = aircraft.get("lat")
            lon = aircraft.get("lon")
            altitude = aircraft.get("altitude", 0)
            heading = aircraft.get("heading")
            speed = aircraft.get("speed")
            
            if lat is not None and lon is not None:
                # Format altitude as three figures, rounded to nearest hundred
                altitude_hundreds = round(altitude / 100)
                base_altitude = f"{altitude_hundreds:03d}"
                
                # Add climb/descent arrows based on vertical rate with more spacing
                vert_rate = aircraft.get("vert_rate", 0)
                if vert_rate > 200:
                    formatted_altitude = f"    {base_altitude} ^"  # Climbing - 4 spaces
                elif vert_rate < -200:
                    formatted_altitude = f"    {base_altitude} v"  # Descending - 4 spaces
                else:
                    formatted_altitude = f"    {base_altitude}"    # Level flight - 4 spaces
                
                # Aircraft placemark
                kml_parts.extend([
                    '<Placemark>',
                    f'<name>{formatted_altitude}</name>',
                    '<styleUrl>#aircraft</styleUrl>',
                    '<Point>',
                    f'<coordinates>{lon},{lat}</coordinates>',
                    '</Point>',
                    '</Placemark>'
                ])
                
                # Prediction line: 30-second ahead projection with CORRECTED MATH
                if heading is not None and speed is not None and speed > 0:
                    # Convert speed from knots to meters per second
                    speed_mps = speed * 0.514444  # 1 knot = 0.514444 m/s
                    
                    # Calculate 30-second displacement in meters
                    distance_meters = speed_mps * 30
                    
                    # Earth radius in meters
                    earth_radius = 6378137.0
                    
                    # Convert heading to radians (0° = North)
                    heading_rad = math.radians(heading)
                    
                    # Calculate displacement in degrees
                    dlat = (distance_meters * math.cos(heading_rad)) / earth_radius * (180 / math.pi)
                    dlon = (distance_meters * math.sin(heading_rad)) / (earth_radius * math.cos(math.radians(lat))) * (180 / math.pi)
                    
                    # Calculate predicted position
                    pred_lat = lat + dlat
                    pred_lon = lon + dlon
                    
                    # Add prediction line
                    kml_parts.extend([
                        '<Placemark>',
                        '<styleUrl>#prediction</styleUrl>',
                        '<LineString>',
                        f'<coordinates>{lon},{lat} {pred_lon},{pred_lat}</coordinates>',
                        '</LineString>',
                        '</Placemark>'
                    ])
        
        kml_parts.extend(['</Document>', '</kml>'])
        
        # Return as compact string (no pretty printing)
        return ''.join(kml_parts)
    
    def generate_main_kml(self, base_url: str) -> str:
        """Generate minimal main KML with NetworkLink"""
        aircraft_count = len(self.get_current_aircraft())
        
        # Minimal main KML
        kml_parts = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<kml xmlns="http://www.opengis.net/kml/2.2">',
            '<Document>',
            f'<name>Aircraft ({aircraft_count})</name>',
            '<NetworkLink>',
            '<name>Live Data</name>',
            '<refreshVisibility>0</refreshVisibility>',
            '<flyToView>0</flyToView>',
            '<Link>',
            f'<href>{base_url}/aircraft.kmz</href>',
            '<refreshMode>onInterval</refreshMode>',
            f'<refreshInterval>{self.refresh_interval}</refreshInterval>',
            '</Link>',
            '</NetworkLink>',
            '</Document>',
            '</kml>'
        ]
        
        return ''.join(kml_parts)
    
    def create_kmz_from_kml(self, kml_content: str) -> bytes:
        """Create KMZ (ZIP file containing KML) from KML string"""
        kmz_buffer = io.BytesIO()
        with zipfile.ZipFile(kmz_buffer, 'w', zipfile.ZIP_DEFLATED) as kmz:
            kmz.writestr('doc.kml', kml_content.encode('utf-8'))
        return kmz_buffer.getvalue()