#!/usr/bin/env python3
"""
Anomaly Detection Engine for Edge Layer
Detects unusual sensor readings and triggers alerts
"""

import statistics
import logging
from typing import Dict, Tuple, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """
    Detects anomalies in soil sensor readings using multiple methods:
    - Z-score: statistical outlier detection
    - IQR: interquartile range method
    - Change rate: sudden changes in values
    - Threshold-based: values outside critical ranges
    """
    
    def __init__(
        self,
        zscore_threshold: float = 2.5,
        iqr_multiplier: float = 1.5,
        change_rate_threshold: float = 0.3,
        window_size: int = 20
    ):
        """
        Initialize anomaly detector
        
        Args:
            zscore_threshold: Z-score standard deviations for outlier (default: 2.5)
            iqr_multiplier: IQR multiplier for outlier bounds (default: 1.5)
            change_rate_threshold: Fraction change to flag as anomaly (default: 0.3 = 30%)
            window_size: Number of readings for baseline (default: 20)
        """
        self.zscore_threshold = zscore_threshold
        self.iqr_multiplier = iqr_multiplier
        self.change_rate_threshold = change_rate_threshold
        self.window_size = window_size
    
    # ========================================================================
    # STATISTICAL ANOMALY DETECTION
    # ========================================================================
    
    def _zscore_anomaly(
        self,
        value: float,
        baseline_values: List[float]
    ) -> Tuple[bool, Optional[float], Optional[str]]:
        """
        Detect anomaly using Z-score method
        
        Args:
            value: Current measurement
            baseline_values: Historical baseline values
            
        Returns:
            Tuple of (is_anomaly, zscore, method_name)
        """
        if len(baseline_values) < 2:
            return False, None, None
        
        try:
            mean = statistics.mean(baseline_values)
            stddev = statistics.stdev(baseline_values)
            
            if stddev == 0:
                return False, None, None
            
            zscore = abs((value - mean) / stddev)
            
            if zscore >= self.zscore_threshold:
                logger.debug(f"Z-score anomaly detected: {zscore:.2f}")
                return True, zscore, "zscore"
            
            return False, zscore, None
        
        except Exception as e:
            logger.error(f"Z-score calculation error: {e}")
            return False, None, None
    
    def _iqr_anomaly(
        self,
        value: float,
        baseline_values: List[float]
    ) -> Tuple[bool, Tuple[float, float], Optional[str]]:
        """
        Detect anomaly using Interquartile Range (IQR) method
        
        Args:
            value: Current measurement
            baseline_values: Historical baseline values
            
        Returns:
            Tuple of (is_anomaly, (lower_bound, upper_bound), method_name)
        """
        if len(baseline_values) < 4:
            return False, (0, 0), None
        
        try:
            sorted_vals = sorted(baseline_values)
            n = len(sorted_vals)
            
            # Calculate quartiles
            q1_idx = n // 4
            q3_idx = (3 * n) // 4
            q1 = sorted_vals[q1_idx]
            q3 = sorted_vals[q3_idx]
            iqr = q3 - q1
            
            lower_bound = q1 - self.iqr_multiplier * iqr
            upper_bound = q3 + self.iqr_multiplier * iqr
            
            if value < lower_bound or value > upper_bound:
                logger.debug(f"IQR anomaly detected: {value} outside [{lower_bound:.2f}, {upper_bound:.2f}]")
                return True, (lower_bound, upper_bound), "iqr"
            
            return False, (lower_bound, upper_bound), None
        
        except Exception as e:
            logger.error(f"IQR calculation error: {e}")
            return False, (0, 0), None
    
    def _change_rate_anomaly(
        self,
        current_value: float,
        previous_value: Optional[float]
    ) -> Tuple[bool, Optional[float], Optional[str]]:
        """
        Detect anomaly based on sudden change
        
        Args:
            current_value: Current measurement
            previous_value: Previous measurement
            
        Returns:
            Tuple of (is_anomaly, change_rate, method_name)
        """
        if previous_value is None or previous_value == 0:
            return False, None, None
        
        try:
            change_rate = abs((current_value - previous_value) / previous_value)
            
            if change_rate >= self.change_rate_threshold:
                logger.debug(f"Change rate anomaly detected: {change_rate:.2%} change")
                return True, change_rate, "change_rate"
            
            return False, change_rate, None
        
        except Exception as e:
            logger.error(f"Change rate calculation error: {e}")
            return False, None, None
    
    # ========================================================================
    # THRESHOLD-BASED DETECTION
    # ========================================================================
    
    def _threshold_anomaly(
        self,
        value: float,
        critical_low: float,
        critical_high: float
    ) -> Tuple[bool, Optional[str]]:
        """
        Detect anomaly based on critical thresholds
        
        Args:
            value: Current measurement
            critical_low: Lower critical threshold
            critical_high: Upper critical threshold
            
        Returns:
            Tuple of (is_anomaly, threshold_status)
        """
        if value < critical_low:
            logger.debug(f"Threshold anomaly: value {value} below critical low {critical_low}")
            return True, "below_critical"
        elif value > critical_high:
            logger.debug(f"Threshold anomaly: value {value} above critical high {critical_high}")
            return True, "above_critical"
        
        return False, None
    
    # ========================================================================
    # COMPOSITE ANOMALY DETECTION
    # ========================================================================
    
    def detect_anomalies(
        self,
        current_value: float,
        parameter: str,
        baseline_values: List[float],
        previous_value: Optional[float] = None,
        critical_low: Optional[float] = None,
        critical_high: Optional[float] = None
    ) -> Dict:
        """
        Comprehensive anomaly detection using multiple methods
        
        Args:
            current_value: Current measurement
            parameter: Parameter name (N, P, K, temperature, etc.)
            baseline_values: Historical values for statistical analysis
            previous_value: Previous measurement for change detection
            critical_low: Lower critical threshold
            critical_high: Upper critical threshold
            
        Returns:
            Dict with anomaly results and details
        """
        results = {
            "parameter": parameter,
            "current_value": current_value,
            "timestamp": datetime.utcnow().isoformat(),
            "anomalies_detected": [],
            "severity": "normal"
        }
        
        # Method 1: Z-score
        is_anomaly, zscore, method = self._zscore_anomaly(current_value, baseline_values)
        if is_anomaly:
            results["anomalies_detected"].append({
                "method": method,
                "value": zscore,
                "description": f"Z-score: {zscore:.2f}"
            })
        
        # Method 2: IQR
        is_anomaly, bounds, method = self._iqr_anomaly(current_value, baseline_values)
        if is_anomaly:
            results["anomalies_detected"].append({
                "method": method,
                "bounds": bounds,
                "description": f"IQR: outside [{bounds[0]:.2f}, {bounds[1]:.2f}]"
            })
        
        # Method 3: Change rate
        is_anomaly, change_rate, method = self._change_rate_anomaly(current_value, previous_value)
        if is_anomaly:
            results["anomalies_detected"].append({
                "method": method,
                "change_rate": change_rate,
                "description": f"Change rate: {change_rate:.2%}"
            })
        
        # Method 4: Threshold-based
        if critical_low is not None and critical_high is not None:
            is_anomaly, status = self._threshold_anomaly(
                current_value,
                critical_low,
                critical_high
            )
            if is_anomaly:
                results["anomalies_detected"].append({
                    "method": "threshold",
                    "status": status,
                    "description": f"Threshold violation: {status}"
                })
        
        # Determine severity
        num_anomalies = len(results["anomalies_detected"])
        if num_anomalies >= 3:
            results["severity"] = "critical"
        elif num_anomalies == 2:
            results["severity"] = "high"
        elif num_anomalies == 1:
            results["severity"] = "medium"
        
        return results
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def is_normal(self, anomaly_result: Dict) -> bool:
        """Check if reading is normal (no anomalies)"""
        return len(anomaly_result["anomalies_detected"]) == 0
    
    def should_forward_to_cloud(self, anomaly_result: Dict, sensitivity: str = "medium") -> bool:
        """
        Decide if anomaly should be forwarded to cloud
        
        Args:
            anomaly_result: Result from detect_anomalies()
            sensitivity: Detection sensitivity ('low', 'medium', 'high')
            
        Returns:
            True if should forward to cloud
        """
        severity_map = {
            "low": ["critical"],
            "medium": ["critical", "high"],
            "high": ["critical", "high", "medium"]
        }
        
        trigger_severities = severity_map.get(sensitivity, ["critical", "high"])
        return anomaly_result["severity"] in trigger_severities


if __name__ == "__main__":
    # Test anomaly detection
    logging.basicConfig(level=logging.DEBUG)
    
    detector = AnomalyDetector()
    
    # Generate baseline
    baseline = [50, 51, 49, 52, 50, 48, 51, 49, 50, 51]
    
    # Test normal value
    print("=== Test 1: Normal value ===")
    result = detector.detect_anomalies(
        current_value=50,
        parameter="N",
        baseline_values=baseline,
        previous_value=49,
        critical_low=10,
        critical_high=150
    )
    print(f"Result: {result['severity']} - {len(result['anomalies_detected'])} anomalies")
    
    # Test outlier
    print("\n=== Test 2: Outlier (Z-score) ===")
    result = detector.detect_anomalies(
        current_value=120,  # Way outside baseline
        parameter="N",
        baseline_values=baseline,
        previous_value=50,
        critical_low=10,
        critical_high=150
    )
    print(f"Result: {result['severity']} - {result['anomalies_detected']}")
    
    # Test critical threshold
    print("\n=== Test 3: Critical threshold ===")
    result = detector.detect_anomalies(
        current_value=5,  # Below critical
        parameter="N",
        baseline_values=baseline,
        previous_value=50,
        critical_low=10,
        critical_high=150
    )
    print(f"Result: {result['severity']} - {result['anomalies_detected']}")