FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY edge_config.py .
COPY redis_manager.py .
COPY anomaly_detector.py .
COPY edge_node.py .

# Create logs directory
RUN mkdir -p /app/logs

# Run edge node
CMD ["python", "edge_node.py"]
