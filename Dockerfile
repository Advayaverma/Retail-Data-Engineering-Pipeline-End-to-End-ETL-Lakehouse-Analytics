FROM python:3.11-slim

# Install system dependencies including OpenJDK 17 for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

# Copy dependency specifications and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code and configurations
COPY src/ /app/src/
COPY pyspark/ /app/pyspark/
COPY sql/ /app/sql/
COPY config/ /app/config/
COPY scripts/ /app/scripts/
COPY tests/ /app/tests/

ENV PYTHONPATH=/app:/app/src

CMD ["python", "scripts/run_pipeline.py"]
