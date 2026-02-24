# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LDES_CONFIG_PATH=/data/ldes-feeds.yaml

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .

# Create data directory for mounted volume
RUN mkdir -p /data

# Make the script executable
RUN chmod +x app.py

# Run the application
CMD ["python", "app.py"]
