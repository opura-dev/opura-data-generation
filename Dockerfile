# Use lightweight Python image
FROM python:3.12-slim

WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source files
COPY . .

# Write GCP credentials to a file at runtime
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/gcp-key.json"

# Expose Streamlit port
EXPOSE 8501

# Start Streamlit app
CMD bash -c 'echo "$GCP_SERVICE_ACCOUNT_JSON" > /app/gcp-key.json && streamlit run streamlit_app.py --server.port=8501 --server.address=0.0.0.0'