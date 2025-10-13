# Use Python 3.10 slim image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements_api.txt .

# Install dependencies directly using pip (no uv for build time)
RUN pip install --no-cache-dir -r requirements_api.txt

# Copy application code
COPY . .

# Set environment variables
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Increase file descriptor limit and expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8080}/ || exit 1

# Run FastAPI app with uvicorn and increased ulimit
CMD ulimit -n 4096 && uvicorn api_server:app \
    --host 0.0.0.0 \
    --port ${PORT}