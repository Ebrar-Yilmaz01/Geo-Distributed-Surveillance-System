#!/usr/bin/env python3
"""
Testing & Validation Script for Edge Layer
Verify all components working correctly
"""

import json
import time
import logging
import sys
from typing import List

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [TEST] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# TEST SUITE
# ============================================================================

class EdgeLayerTests:
    """Test suite for edge layer components"""
    
    def __init__(self):
        self.tests_passed = 0
        self.tests_failed = 0
        self.redis_manager = None
        self.anomaly_detector = None
    
    def run_all(self) -> bool:
        """Run all tests"""
        logger.info("=" * 70)
        logger.info("EDGE LAYER TEST SUITE")
        logger.info("=" * 70)
        
        try:
            self.test_imports()
            self.test_redis_connection()
            self.test_config()
            self.test_anomaly_detection()
            self.test_redis_operations()
            self.test_aggregation()
            
            logger.info("=" * 70)
            logger.info(f"RESULTS: {self.tests_passed} passed, {self.tests_failed} failed")
            logger.info("=" * 70)
            
            return self.tests_failed == 0
        
        except Exception as e:
            logger.error(f"Fatal test error: {e}", exc_info=True)
            return False
    
    # ========================================================================
    # TESTS
    # ========================================================================
    
    def test_imports(self):
        """Test: Import all modules"""
        logger.info("\n[1/6] Testing imports...")
        try:
            from edge_config import get_edge_node_config, SOIL_THRESHOLDS
            from redis_manager import RedisTimeSeriesManager
            from anomaly_detector import AnomalyDetector
            
            self.anomaly_detector = AnomalyDetector()
            
            logger.info("✓ All imports successful")
            self.tests_passed += 1
        except Exception as e:
            logger.error(f"✗ Import failed: {e}")
            self.tests_failed += 1
    
    def test_config(self):
        """Test: Configuration loading"""
        logger.info("\n[2/6] Testing configuration...")
        try:
            from edge_config import get_edge_node_config, EDGE_NODES, SOIL_THRESHOLDS
            
            # Test edge node configs
            for node_id in EDGE_NODES:
                config = get_edge_node_config(node_id)
                assert config.node_id == node_id
                assert len(config.managed_devices) > 0
                logger.info(f"  ✓ Config for {node_id}: {config.region} ({len(config.managed_devices)} devices)")
            
            # Test soil thresholds
            assert "N" in SOIL_THRESHOLDS
            assert "temperature" in SOIL_THRESHOLDS
            logger.info(f"  ✓ Loaded {len(SOIL_THRESHOLDS)} soil parameter thresholds")
            
            self.tests_passed += 1
        except Exception as e:
            logger.error(f"✗ Config test failed: {e}")
            self.tests_failed += 1
    
    def test_redis_connection(self):
        """Test: Redis connection"""
        logger.info("\n[3/6] Testing Redis connection...")
        try:
            from redis_manager import RedisTimeSeriesManager
            
            manager = RedisTimeSeriesManager(host="localhost", port=6379)
            self.redis_manager = manager
            
            logger.info("  ✓ Connected to Redis")
            self.tests_passed += 1
        except Exception as e:
            logger.warning(f"✗ Redis connection failed (may not be running): {e}")
            logger.warning("  → Run: docker-compose up -d redis-europe")
            self.tests_failed += 1
    
    def test_anomaly_detection(self):
        """Test: Anomaly detection algorithms"""
        logger.info("\n[4/6] Testing anomaly detection...")
        try:
            detector = self.anomaly_detector
            
            # Test 1: Normal value
            baseline = [50, 51, 49, 52, 50, 48, 51, 49, 50, 51]
            result = detector.detect_anomalies(
                current_value=50,
                parameter="N",
                baseline_values=baseline,
                critical_low=10,
                critical_high=150
            )
            assert result["severity"] == "normal"
            logger.info("  ✓ Normal value detected correctly")
            
            # Test 2: Statistical outlier
            result = detector.detect_anomalies(
                current_value=120,
                parameter="N",
                baseline_values=baseline,
                critical_low=10,
                critical_high=150
            )
            assert result["severity"] in ["medium", "high", "critical"]
            assert len(result["anomalies_detected"]) > 0
            logger.info(f"  ✓ Outlier detected (severity: {result['severity']})")
            
            # Test 3: Threshold violation
            result = detector.detect_anomalies(
                current_value=5,
                parameter="N",
                baseline_values=baseline,
                critical_low=10,
                critical_high=150
            )
            assert result["severity"] in ["medium", "high", "critical"]
            logger.info(f"  ✓ Threshold violation detected")
            
            # Test 4: Should forward decision
            should_forward = detector.should_forward_to_cloud(result, sensitivity="medium")
            assert should_forward
            logger.info(f"  ✓ Forward to cloud decision: {should_forward}")
            
            self.tests_passed += 1
        except Exception as e:
            logger.error(f"✗ Anomaly detection test failed: {e}")
            self.tests_failed += 1
    
    def test_redis_operations(self):
        """Test: Redis storage operations"""
        logger.info("\n[5/6] Testing Redis operations...")
        if not self.redis_manager:
            logger.warning("✗ Skipping (Redis not connected)")
            self.tests_failed += 1
            return
        
        try:
            test_device = "test_device_001"
            
            # Test 1: Store reading
            reading = {
                "N": 50.5,
                "P": 35.2,
                "K": 100.3,
                "temperature": 25.5,
                "humidity": 65.0,
                "ph": 6.8,
                "rainfall": 50.0
            }
            
            self.redis_manager.store_reading(test_device, reading)
            logger.info(f"  ✓ Stored reading for {test_device}")
            
            # Small delay
            time.sleep(0.5)
            
            # Test 2: Retrieve readings
            readings = self.redis_manager.get_readings_window(test_device, window_seconds=60)
            assert len(readings) > 0
            logger.info(f"  ✓ Retrieved {len(readings)} readings")
            
            # Test 3: Aggregation
            agg = self.redis_manager.aggregate_readings(
                test_device,
                window_seconds=60,
                methods=["mean", "min", "max"]
            )
            assert agg is not None
            logger.info(f"  ✓ Aggregated readings: {agg['num_readings']} readings processed")
            
            # Test 4: Device status
            self.redis_manager.set_device_status(test_device, "online")
            status = self.redis_manager.get_device_status(test_device)
            assert status == "online"
            logger.info(f"  ✓ Device status management working")
            
            self.tests_passed += 1
        except Exception as e:
            logger.error(f"✗ Redis operations test failed: {e}")
            self.tests_failed += 1
    
    def test_aggregation(self):
        """Test: Data aggregation"""
        logger.info("\n[6/6] Testing data aggregation...")
        if not self.redis_manager:
            logger.warning("✗ Skipping (Redis not connected)")
            self.tests_failed += 1
            return
        
        try:
            test_device = "agg_test_device"
            
            # Store multiple readings
            for i in range(10):
                reading = {
                    "N": 50 + i * 2,
                    "P": 35,
                    "K": 100,
                    "temperature": 25 + i * 0.5,
                    "humidity": 60,
                    "ph": 6.8,
                    "rainfall": 50
                }
                self.redis_manager.store_reading(test_device, reading, ttl=3600)
                time.sleep(0.1)
            
            # Aggregate
            agg = self.redis_manager.aggregate_readings(test_device, window_seconds=600)
            
            if agg:
                logger.info(f"  ✓ Aggregation statistics:")
                logger.info(f"    - Number of readings: {agg['num_readings']}")
                logger.info(f"    - N mean: {agg.get('N_mean', 'N/A'):.2f}")
                logger.info(f"    - N min: {agg.get('N_min', 'N/A'):.2f}")
                logger.info(f"    - N max: {agg.get('N_max', 'N/A'):.2f}")
                self.tests_passed += 1
            else:
                raise Exception("Aggregation returned None")
        
        except Exception as e:
            logger.error(f"✗ Aggregation test failed: {e}")
            self.tests_failed += 1


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Run tests"""
    tests = EdgeLayerTests()
    success = tests.run_all()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()