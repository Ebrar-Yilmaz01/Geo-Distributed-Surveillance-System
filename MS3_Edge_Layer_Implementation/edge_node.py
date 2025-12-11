#!/usr/bin/env python3
"""
Edge Layer Node - Soil Analysis
Main application for edge processing with MQTT integration
"""

import json
import logging
import sys
import time
import signal
from typing import Optional
import paho.mqtt.client as mqtt

from edge_config import get_edge_node_config, ANOMALY_CONFIG, AGGREGATION_CONFIG
from redis_manager import RedisTimeSeriesManager
from anomaly_detector import AnomalyDetector

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('edge_node.log')
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# EDGE NODE CLASS
# ============================================================================

class EdgeNode:
    """
    Edge Node for Soil Analysis System
    
    Responsibilities:
    - Receive MQTT messages from IoT devices
    - Store readings in Redis time-series
    - Aggregate data over time windows
    - Detect anomalies in sensor readings
    - Forward only relevant data/alerts to cloud
    """
    
    def __init__(self, node_id: str):
        """
        Initialize edge node
        
        Args:
            node_id: Node identifier (e.g., 'edge-europe', 'edge-asia')
        """
        self.node_id = node_id
        self.config = get_edge_node_config(node_id)
        self.running = False
        
        # Initialize components
        self.redis_manager = None
        self.anomaly_detector = None
        self.mqtt_client = None
        self._init_redis()
        self._init_anomaly_detector()
        self._init_mqtt()
        
        logger.info(f"Edge Node {node_id} initialized successfully")
    
    # ========================================================================
    # INITIALIZATION
    # ========================================================================
    
    def _init_redis(self) -> None:
        """Initialize Redis connection"""
        try:
            self.redis_manager = RedisTimeSeriesManager(
                host=self.config.redis_host,
                port=self.config.redis_port,
                db=0
            )
            logger.info(f"Redis connected: {self.config.redis_host}:{self.config.redis_port}")
        except Exception as e:
            logger.error(f"Failed to initialize Redis: {e}")
            raise
    
    def _init_anomaly_detector(self) -> None:
        """Initialize anomaly detection engine"""
        self.anomaly_detector = AnomalyDetector(
            zscore_threshold=ANOMALY_CONFIG["zscore_threshold"],
            iqr_multiplier=ANOMALY_CONFIG["iqr_multiplier"],
            change_rate_threshold=ANOMALY_CONFIG["change_rate_threshold"],
            window_size=ANOMALY_CONFIG["window_size"]
        )
        logger.info("Anomaly detector initialized")
    
    def _init_mqtt(self) -> None:
        """Initialize MQTT client"""
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_message = self._on_mqtt_message
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
        
        try:
            logger.info(f"Connecting to MQTT broker: {self.config.mqtt_broker}:{self.config.mqtt_port}")
            self.mqtt_client.connect(
                self.config.mqtt_broker,
                self.config.mqtt_port,
                keepalive=60
            )
        except Exception as e:
            logger.error(f"Failed to connect to MQTT: {e}")
            raise
    
    # ========================================================================
    # MQTT CALLBACKS
    # ========================================================================
    
    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("✓ Connected to MQTT broker")
            client.subscribe(self.config.input_topic)
            logger.info(f"✓ Subscribed to topic: {self.config.input_topic}")
            self.redis_manager.set_device_status(self.node_id, "online", ttl=300)
        else:
            logger.error(f"✗ MQTT connection failed with code {rc}")
    
    def _on_mqtt_disconnect(self, client, userdata, rc):
        """MQTT disconnect callback (paho 1.6.1)"""
        logger.warning(f"Disconnected from MQTT broker (code: {rc})")
        if self.running:
            logger.info("Attempting to reconnect...")
    
    def _on_mqtt_message(self, client, userdata, message):
        """MQTT message callback - process incoming sensor data"""
        try:
            # Parse message
            payload = json.loads(message.payload.decode())
            device_id = payload.get("device_id")
            
            # Filter: only process messages from managed devices
            if device_id not in self.config.managed_devices:
                logger.debug(f"Ignoring message from unmanaged device: {device_id}")
                return
            
            logger.debug(f"Received data from {device_id}")
            
            # Process reading
            self._process_reading(device_id, payload)
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode MQTT message: {e}")
        except Exception as e:
            logger.error(f"Error processing MQTT message: {e}")
    
    # ========================================================================
    # DATA PROCESSING PIPELINE
    # ========================================================================
    
    def _process_reading(self, device_id: str, reading: dict) -> None:
        """
        Main processing pipeline for sensor reading
        
        Args:
            device_id: Device identifier
            reading: Raw sensor data
        """
        # Step 1: Store raw reading in Redis
        self.redis_manager.store_reading(device_id, reading, ttl=86400)
        logger.debug(f"[{device_id}] Raw reading stored in Redis")
        
        # Step 2: Get historical baseline for anomaly detection
        window_readings = self.redis_manager.get_readings_window(
            device_id,
            window_seconds=ANOMALY_CONFIG["window_size"] * 60
        )
        
        # Step 3: Check each parameter for anomalies
        alert_events = []
        
        for param in ["N", "P", "K", "temperature", "humidity", "ph", "rainfall"]:
            if param not in reading:
                continue
            
            current_value = reading[param]
            
            # Get baseline values
            baseline_values = [r[param] for r in window_readings if param in r]
            previous_value = window_readings[-1].get(param) if window_readings else None
            
            # Get critical thresholds
            from edge_config import SOIL_THRESHOLDS
            thresholds = SOIL_THRESHOLDS[param]
            
            # Detect anomalies
            anomaly_result = self.anomaly_detector.detect_anomalies(
                current_value=current_value,
                parameter=param,
                baseline_values=baseline_values,
                previous_value=previous_value,
                critical_low=thresholds["critical_low"],
                critical_high=thresholds["critical_high"]
            )
            
            # Check if should forward to cloud
            if self.anomaly_detector.should_forward_to_cloud(anomaly_result, sensitivity="medium"):
                alert_events.append({
                    "device_id": device_id,
                    "anomaly_result": anomaly_result
                })
                
                logger.warning(
                    f"[{device_id}] {param}={current_value:.2f} "
                    f"ANOMALY DETECTED (severity: {anomaly_result['severity']})"
                )
                
                # Store anomaly in Redis
                self.redis_manager.store_anomaly_event(
                    device_id=device_id,
                    parameter=param,
                    value=current_value,
                    anomaly_type=anomaly_result["anomalies_detected"][0].get("method", "unknown"),
                    details=anomaly_result
                )
        
        # Step 4: Aggregate data periodically
        self._try_aggregate(device_id)
        
        # Step 5: Forward alerts to cloud
        if alert_events:
            self._forward_to_cloud(alert_events)
    
    def _try_aggregate(self, device_id: str) -> None:
        """
        Attempt to aggregate readings for device
        
        Args:
            device_id: Device identifier
        """
        aggregated = self.redis_manager.aggregate_readings(
            device_id=device_id,
            window_seconds=AGGREGATION_CONFIG["window_seconds"],
            methods=AGGREGATION_CONFIG["methods"]
        )
        
        if aggregated:
            logger.debug(f"[{device_id}] Aggregated {aggregated['num_readings']} readings")
    
    def _forward_to_cloud(self, alert_events: list) -> None:
        """
        Forward alert events to cloud layer
        
        Args:
            alert_events: List of anomaly events to forward
        """
        for event in alert_events:
            try:
                payload = {
                    "edge_node": self.node_id,
                    "timestamp": time.time(),
                    **event
                }
                
                # Publish to cloud topic
                self.mqtt_client.publish(
                    self.config.cloud_topic,
                    json.dumps(payload),
                    qos=1
                )
                
                logger.info(
                    f"✓ Forwarded alert to cloud: {event['device_id']} - "
                    f"{event['anomaly_result']['parameter']} "
                    f"({event['anomaly_result']['severity']})"
                )
            except Exception as e:
                logger.error(f"Failed to forward alert: {e}")
    
    # ========================================================================
    # METRICS & MONITORING
    # ========================================================================
    
    def _update_metrics(self) -> None:
        """Update edge node metrics"""
        try:
            metrics = {
                "timestamp": time.time(),
                "node_id": self.node_id,
                "region": self.config.region,
                "mqtt_connected": self.mqtt_client.is_connected(),
                "redis_connected": True,
                "status": "running"
            }
            
            self.redis_manager.update_edge_node_metrics(self.node_id, metrics, ttl=300)
        except Exception as e:
            logger.error(f"Failed to update metrics: {e}")
    
    # ========================================================================
    # LIFECYCLE
    # ========================================================================
    
    def start(self) -> None:
        """Start edge node"""
        self.running = True
        logger.info(f"Starting {self.node_id}...")
        
        self.mqtt_client.loop_start()
        
        # Main monitoring loop
        try:
            while self.running:
                self._update_metrics()
                time.sleep(30)  # Update metrics every 30 seconds
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            self.stop()
    
    def stop(self) -> None:
        """Stop edge node gracefully"""
        logger.info(f"Stopping {self.node_id}...")
        self.running = False
        
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        
        if self.redis_manager:
            self.redis_manager.redis_client.close()
        
        logger.info(f"{self.node_id} stopped")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    import os
    
    # Get node ID from environment or default
    node_id = os.environ.get("EDGE_NODE_ID", "edge-europe")
    
    logger.info(f"Starting Edge Node: {node_id}")
    
    # Create and start edge node
    try:
        edge_node = EdgeNode(node_id)
        
        # Handle signals
        def signal_handler(sig, frame):
            logger.info("Received shutdown signal")
            edge_node.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        edge_node.start()
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()