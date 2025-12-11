#!/usr/bin/env python3
"""
Edge Layer Configuration
Handles device-to-edge-node routing and soil analysis thresholds
"""

import json
from dataclasses import dataclass
from typing import List

# ============================================================================
# EDGE NODE CONFIGURATION
# ============================================================================

@dataclass
class EdgeNodeConfig:
    """Configuration for each edge node"""
    node_id: str
    node_name: str
    region: str
    managed_devices: List[str]
    redis_host: str = "redis"
    redis_port: int = 6379
    mqtt_broker: str = "broker.hivemq.com"
    mqtt_port: int = 1883
    cloud_topic: str = "farm/cloud/alerts"
    aggregation_window: int = 300  # 5 minutes in seconds
    
    def __post_init__(self):
        self.input_topic = f"farm/data"


# Edge Node Definitions
EDGE_NODES = {
    "edge-europe": EdgeNodeConfig(
        node_id="edge-europe",
        node_name="European Edge Node",
        region="Europe",
        managed_devices=[
            "device_germany",
            "device_england"
        ],
        redis_host="redis-europe",
        cloud_topic="farm/cloud/europe/alerts"
    ),
    "edge-asia": EdgeNodeConfig(
        node_id="edge-asia",
        node_name="Asian Edge Node",
        region="Asia",
        managed_devices=[
            "device_india",
            "device_egypt",
            "device_brazil"
        ],
        redis_host="redis-asia",
        cloud_topic="farm/cloud/asia/alerts"
    )
}

# ============================================================================
# SOIL ANALYSIS THRESHOLDS (Domain-Specific)
# ============================================================================

SOIL_THRESHOLDS = {
    "N": {
        "min": 0,
        "max": 200,
        "critical_low": 10,
        "critical_high": 150,
        "description": "Nitrogen (mg/kg)"
    },
    "P": {
        "min": 0,
        "max": 200,
        "critical_low": 5,
        "critical_high": 100,
        "description": "Phosphorus (mg/kg)"
    },
    "K": {
        "min": 0,
        "max": 250,
        "critical_low": 50,
        "critical_high": 200,
        "description": "Potassium (mg/kg)"
    },
    "temperature": {
        "min": -10,
        "max": 50,
        "critical_low": 5,
        "critical_high": 40,
        "description": "Temperature (°C)"
    },
    "humidity": {
        "min": 0,
        "max": 100,
        "critical_low": 20,
        "critical_high": 90,
        "description": "Humidity (%)"
    },
    "ph": {
        "min": 0,
        "max": 14,
        "critical_low": 6.0,
        "critical_high": 7.5,
        "description": "pH level"
    },
    "rainfall": {
        "min": 0,
        "max": 500,
        "critical_low": 0,
        "critical_high": 200,
        "description": "Rainfall (mm)"
    }
}

# ============================================================================
# ANOMALY DETECTION THRESHOLDS
# ============================================================================

ANOMALY_CONFIG = {
    "zscore_threshold": 2.5,  # Standard deviations from mean
    "iqr_multiplier": 1.5,    # IQR * this for outlier detection
    "change_rate_threshold": 0.3,  # 30% change = anomaly
    "window_size": 20  # Use last 20 readings for baseline
}

# ============================================================================
# AGGREGATION PARAMETERS
# ============================================================================

AGGREGATION_CONFIG = {
    "window_seconds": 300,  # 5-minute windows
    "min_readings_per_window": 3,  # Need at least 3 readings to aggregate
    "methods": ["mean", "min", "max", "stddev"]
}

# ============================================================================
# LOGGING & DEBUG
# ============================================================================

LOG_CONFIG = {
    "level": "DEBUG",
    "format": "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
}


def get_edge_node_config(node_id: str) -> EdgeNodeConfig:
    """Get configuration for a specific edge node"""
    if node_id not in EDGE_NODES:
        raise ValueError(f"Unknown edge node: {node_id}")
    return EDGE_NODES[node_id]


def get_threshold(parameter: str) -> dict:
    """Get threshold for a soil parameter"""
    if parameter not in SOIL_THRESHOLDS:
        raise ValueError(f"Unknown parameter: {parameter}")
    return SOIL_THRESHOLDS[parameter]


if __name__ == "__main__":
    # Test configuration
    print("=== EDGE NODE CONFIGURATIONS ===\n")
    for node_id, config in EDGE_NODES.items():
        print(f"{node_id}:")
        print(f"  Region: {config.region}")
        print(f"  Devices: {config.managed_devices}")
        print(f"  Redis: {config.redis_host}:{config.redis_port}\n")
    
    print("\n=== SOIL THRESHOLDS ===\n")
    for param, thresholds in SOIL_THRESHOLDS.items():
        print(f"{param}: {thresholds['description']}")
        print(f"  Normal range: {thresholds['min']}-{thresholds['max']}")
        print(f"  Critical: {thresholds['critical_low']}-{thresholds['critical_high']}\n")