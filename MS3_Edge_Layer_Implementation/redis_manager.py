#!/usr/bin/env python3
"""
Redis Manager for Edge Layer
Handles time-series storage, aggregation, and state management
"""

import json
import time
from typing import Dict, List, Optional, Tuple
from collections import deque
from datetime import datetime, timedelta
import redis
import statistics
import logging

logger = logging.getLogger(__name__)


class RedisTimeSeriesManager:
    """Manages time-series data and aggregation using Redis"""
    
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        """
        Initialize Redis connection
        
        Args:
            host: Redis host
            port: Redis port
            db: Redis database number
        """
        try:
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True
            )
            # Test connection
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {host}:{port}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    # ========================================================================
    # TIME-SERIES DATA STORAGE
    # ========================================================================
    
    def store_reading(self, device_id: str, reading: Dict, ttl: int = 86400) -> None:
        """
        Store a single sensor reading with timestamp
        
        Args:
            device_id: Device identifier
            reading: Sensor data (dict with N, P, K, temp, humidity, ph, rainfall)
            ttl: Time-to-live in seconds (default: 24 hours)
        """
        timestamp = int(time.time() * 1000)  # milliseconds
        key = f"reading:{device_id}:{timestamp}"
        
        data = {
            "timestamp": timestamp,
            **reading
        }
        
        self.redis_client.setex(
            key,
            ttl,
            json.dumps(data)
        )
        
        # Also add to sorted set for time-range queries
        zset_key = f"readings:timeseries:{device_id}"
        self.redis_client.zadd(zset_key, {key: timestamp})
        self.redis_client.expire(zset_key, ttl)
        
        logger.debug(f"Stored reading for {device_id}: {key}")
    
    def get_readings_window(
        self,
        device_id: str,
        window_seconds: int = 300
    ) -> List[Dict]:
        """
        Get all readings for device within time window
        
        Args:
            device_id: Device identifier
            window_seconds: Look back period in seconds
            
        Returns:
            List of readings (oldest to newest)
        """
        now_ms = int(time.time() * 1000)
        window_ms = window_seconds * 1000
        start_ms = now_ms - window_ms
        
        zset_key = f"readings:timeseries:{device_id}"
        keys = self.redis_client.zrangebyscore(
            zset_key,
            start_ms,
            now_ms
        )
        
        readings = []
        for key in keys:
            data = self.redis_client.get(key)
            if data:
                readings.append(json.loads(data))
        
        return sorted(readings, key=lambda x: x["timestamp"])
    
    # ========================================================================
    # AGGREGATION FUNCTIONS
    # ========================================================================
    
    def aggregate_readings(
        self,
        device_id: str,
        window_seconds: int = 300,
        methods: List[str] = None
    ) -> Optional[Dict]:
        """
        Aggregate readings over time window
        
        Args:
            device_id: Device identifier
            window_seconds: Aggregation window in seconds
            methods: List of aggregation methods (mean, min, max, stddev)
            
        Returns:
            Aggregated statistics or None if insufficient data
        """
        if methods is None:
            methods = ["mean", "min", "max", "stddev"]
        
        readings = self.get_readings_window(device_id, window_seconds)
        
        if len(readings) < 3:
            logger.warning(f"Insufficient readings for {device_id} ({len(readings)})")
            return None
        
        aggregated = {
            "device_id": device_id,
            "window_seconds": window_seconds,
            "timestamp": int(time.time()),
            "num_readings": len(readings)
        }
        
        # Get all numeric parameters from first reading
        params = [k for k in readings[0].keys() if k != "timestamp"]
        
        for param in params:
            values = [r[param] for r in readings if param in r]
            
            if not values:
                continue
            
            if "mean" in methods:
                aggregated[f"{param}_mean"] = statistics.mean(values)
            
            if "min" in methods:
                aggregated[f"{param}_min"] = min(values)
            
            if "max" in methods:
                aggregated[f"{param}_max"] = max(values)
            
            if "stddev" in methods and len(values) > 1:
                aggregated[f"{param}_stddev"] = statistics.stdev(values)
        
        # Cache aggregated data
        agg_key = f"aggregated:{device_id}:{int(time.time())}"
        self.redis_client.setex(agg_key, 3600, json.dumps(aggregated))
        
        logger.debug(f"Aggregated {len(readings)} readings for {device_id}")
        return aggregated
    
    # ========================================================================
    # ANOMALY DETECTION STATE
    # ========================================================================
    
    def get_baseline(
        self,
        device_id: str,
        parameter: str,
        window_readings: int = 20
    ) -> Tuple[float, float]:
        """
        Get mean and stddev of parameter for anomaly detection
        
        Args:
            device_id: Device identifier
            parameter: Parameter name (N, P, K, etc.)
            window_readings: Number of recent readings to use
            
        Returns:
            Tuple of (mean, stddev)
        """
        readings = self.get_readings_window(device_id, window_seconds=3600)
        
        if not readings:
            return 0.0, 0.0
        
        # Use last N readings
        recent = readings[-window_readings:]
        values = [r[parameter] for r in recent if parameter in r]
        
        if len(values) < 2:
            return statistics.mean(values) if values else 0.0, 0.0
        
        mean = statistics.mean(values)
        stddev = statistics.stdev(values)
        
        return mean, stddev
    
    def store_anomaly_event(
        self,
        device_id: str,
        parameter: str,
        value: float,
        anomaly_type: str,
        details: Dict
    ) -> None:
        """
        Store detected anomaly for later analysis
        
        Args:
            device_id: Device identifier
            parameter: Parameter that triggered anomaly
            value: Measured value
            anomaly_type: Type of anomaly (zscore, iqr, change_rate)
            details: Additional context
        """
        event = {
            "timestamp": int(time.time()),
            "device_id": device_id,
            "parameter": parameter,
            "value": value,
            "anomaly_type": anomaly_type,
            "details": details
        }
        
        key = f"anomaly:{device_id}:{int(time.time() * 1000)}"
        self.redis_client.setex(key, 86400, json.dumps(event))
        
        # Add to anomaly list
        list_key = f"anomalies:{device_id}"
        self.redis_client.lpush(list_key, json.dumps(event))
        self.redis_client.ltrim(list_key, 0, 99)  # Keep last 100
        
        logger.info(f"Stored anomaly for {device_id}: {parameter}={value} ({anomaly_type})")
    
    # ========================================================================
    # DEVICE STATE & METADATA
    # ========================================================================
    
    def set_device_status(self, device_id: str, status: str, ttl: int = 300) -> None:
        """
        Set device status (online/offline/error)
        
        Args:
            device_id: Device identifier
            status: Status string
            ttl: Time-to-live in seconds
        """
        key = f"device:status:{device_id}"
        self.redis_client.setex(key, ttl, status)
    
    def get_device_status(self, device_id: str) -> Optional[str]:
        """Get device status"""
        key = f"device:status:{device_id}"
        return self.redis_client.get(key)
    
    def get_device_stats(self, device_id: str) -> Dict:
        """Get aggregated statistics for device"""
        stats_key = f"device:stats:{device_id}"
        data = self.redis_client.get(stats_key)
        return json.loads(data) if data else {}
    
    def update_device_stats(self, device_id: str, stats: Dict, ttl: int = 3600) -> None:
        """Update device statistics cache"""
        key = f"device:stats:{device_id}"
        self.redis_client.setex(key, ttl, json.dumps(stats))
    
    # ========================================================================
    # EDGE NODE COORDINATION
    # ========================================================================
    
    def publish_event(self, channel: str, event: Dict) -> None:
        """
        Publish event to Redis Pub/Sub channel
        
        Args:
            channel: Channel name
            event: Event data
        """
        self.redis_client.publish(channel, json.dumps(event))
        logger.debug(f"Published to {channel}")
    
    def get_edge_node_metrics(self, node_id: str) -> Dict:
        """Get metrics for edge node"""
        key = f"edge:metrics:{node_id}"
        data = self.redis_client.get(key)
        return json.loads(data) if data else {}
    
    def update_edge_node_metrics(
        self,
        node_id: str,
        metrics: Dict,
        ttl: int = 300
    ) -> None:
        """Update edge node metrics"""
        key = f"edge:metrics:{node_id}"
        self.redis_client.setex(key, ttl, json.dumps(metrics))
    
    # ========================================================================
    # CLEANUP & MAINTENANCE
    # ========================================================================
    
    def cleanup_old_data(self, max_age_seconds: int = 86400) -> int:
        """
        Remove old readings exceeding max age
        
        Args:
            max_age_seconds: Maximum age in seconds
            
        Returns:
            Number of keys deleted
        """
        cutoff_ms = int((time.time() - max_age_seconds) * 1000)
        deleted = 0
        
        # Scan for old reading keys
        cursor = "0"
        while True:
            cursor, keys = self.redis_client.scan(
                cursor,
                match="reading:*",
                count=100
            )
            deleted += len(keys)
            for key in keys:
                self.redis_client.delete(key)
            
            if cursor == "0":
                break
        
        logger.info(f"Cleaned up {deleted} old data keys")
        return deleted


if __name__ == "__main__":
    # Test Redis connection
    logging.basicConfig(level=logging.DEBUG)
    
    try:
        manager = RedisTimeSeriesManager(host="localhost")
        print("✓ Redis connection successful")
        
        # Test data storage
        test_reading = {
            "N": 50.5,
            "P": 35.2,
            "K": 100.3,
            "temperature": 25.5,
            "humidity": 65.0,
            "ph": 6.8,
            "rainfall": 50.0
        }
        
        manager.store_reading("device_test", test_reading)
        print("✓ Test reading stored")
        
        time.sleep(1)
        readings = manager.get_readings_window("device_test", window_seconds=60)
        print(f"✓ Retrieved {len(readings)} readings")
        
    except Exception as e:
        print(f"✗ Error: {e}")