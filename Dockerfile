# ---------------------------------------------
# Dockerfile for Streamlit + BigQuery Connector
# ---------------------------------------------
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential curl && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the full app
COPY . .

# Expose Streamlit port
EXPOSE 8503

# Environment setup (GOOGLE_APPLICATION_CREDENTIALS will be passed as secret)
ENV STREAMLIT_SERVER_PORT=8503
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Entrypoint:
# - Writes GCP JSON from env var to a temp file
# - Runs Streamlit app
CMD ["/bin/bash", "-c", "echo \"$GOOGLE_APPLICATION_CREDENTIALS\" > /app/service_account.json && streamlit run streamlit_app.py --server.port=8503 --server.address=0.0.0.0"]