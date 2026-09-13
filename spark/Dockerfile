FROM python:3.10-slim-bookworm

# Prevent Python buffering and bytecode caching
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Debian slim requires creating the man directory for OpenJDK headless
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        procps \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Automatically resolve JAVA_HOME for any debian architecture
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and configuration
COPY spark/ ./spark
COPY ingestion/ ./ingestion
COPY tests/ ./tests
COPY pytest.ini .

ENV PYTHONPATH=/app

CMD ["pytest", "tests/unit/test_spark_session.py"]