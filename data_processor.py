#!/usr/bin/env python3
"""
Data Processor - Aircraft filtering and processing logic
Extracted from working prototype - handles filtering, statistics, and callbacks
"""

from typing import Dict, List, Any, Callable

class DataProcessor:
    """Process and filter aircraft data - ENHANCED for ground filtering"""
    
    def __init__(self, filters: Dict[str, Any] = None):
        self.filters = filters or {"max_altitude": 10000, "min_altitude": 100}
        self.stats = {
            "total_aircraft": 0,
            "filtered_aircraft": 0,
            "ground_filtered": 0,
            "low_altitude_filtered": 0,
            "payloads_processed": 0,
            "filter_pass_rate": 0.0,
            "current_filters": self.filters.copy()
        }
        self.callbacks: List[Callable[[List[Dict]], None]] = []
    
    def add_callback(self, callback: Callable[[List[Dict]], None]):
        """Add callback to receive filtered aircraft data"""
        self.callbacks.append(callback)
    
    def process_aircraft_data(self, aircraft_dict: Dict[str, Dict]) -> List[Dict]:
        """Process aircraft data and apply filters"""
        self.stats["payloads_processed"] += 1
        all_aircraft = list(aircraft_dict.values())
        self.stats["total_aircraft"] += len(all_aircraft)
        
        filtered_aircraft = []
        for aircraft in all_aircraft:
            if self._passes_filters(aircraft):
                filtered_aircraft.append(aircraft)
        
        self.stats["filtered_aircraft"] += len(filtered_aircraft)
        if self.stats["total_aircraft"] > 0:
            self.stats["filter_pass_rate"] = round(
                (self.stats["filtered_aircraft"] / self.stats["total_aircraft"]) * 100, 1
            )
        
        # Notify callbacks
        if filtered_aircraft and self.callbacks:
            for callback in self.callbacks:
                try:
                    callback(filtered_aircraft)
                except Exception as e:
                    print(f"Callback error: {e}")
        
        return filtered_aircraft
    
    def _passes_filters(self, aircraft: Dict) -> bool:
        """Check if aircraft passes current filters - ENHANCED for cellular optimization"""
        # Must have position
        if aircraft.get("lat") is None or aircraft.get("lon") is None:
            return False
        
        # Filter out ground aircraft
        if aircraft.get("is_on_ground", False):
            self.stats["ground_filtered"] += 1
            return False
        
        # Altitude filters
        altitude = aircraft.get("altitude")
        if altitude is None:
            return False
            
        # Filter out very low altitude (likely ground/taxiing)
        if "min_altitude" in self.filters and altitude < self.filters["min_altitude"]:
            self.stats["low_altitude_filtered"] += 1
            return False
            
        # Apply max altitude filter
        if "max_altitude" in self.filters and altitude > self.filters["max_altitude"]:
            return False
        
        return True
    
    def update_filters(self, new_filters: Dict[str, Any]):
        """Update filtering criteria at runtime"""
        self.filters.update(new_filters)
        self.stats["current_filters"] = self.filters.copy()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        return self.stats.copy()