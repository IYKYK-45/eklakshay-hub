FROM python:3.11-slim

WORKDIR /app

# Prevent Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copy project requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and config
COPY ingestion/ ./ingestion
COPY pytest.ini .

# Set Python path to ensure module discovery works inside the container
ENV PYTHONPATH=/app

CMD ["python", "ingestion/run_generator.py"]