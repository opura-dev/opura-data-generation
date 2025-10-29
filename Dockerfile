# ======================
# Stage 1: Base Image
# ======================
FROM python:3.12-slim AS base

# Set working directory
WORKDIR /app

# Prevent Python from writing .pyc and enable live logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential curl vim \
    && rm -rf /var/lib/apt/lists/*

# ======================
# Stage 2: Python packages
# ======================
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# ======================
# Stage 3: Copy Application Code
# ======================
COPY . /app

# Create directory for credentials
RUN mkdir -p /app/keys

# Expose Streamlit default port
EXPOSE 8501

# Environment variables for Streamlit
ENV STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_ENABLECORS=false \
    STREAMLIT_SERVER_ENABLEXsrfProtection=false \
    GOOGLE_APPLICATION_CREDENTIALS="/app/keys/service_account.json"

# Healthcheck to ensure container is responsive
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Default command
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]