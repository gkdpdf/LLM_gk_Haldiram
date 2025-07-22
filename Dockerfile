# Use an official Python runtime as a base image
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    tesseract-ocr \
    poppler-utils \
    libreoffice \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Build-time arguments (optional for tagging)
ARG APP_VERSION=docker_version
ARG BUILD_DATETIME
ARG COMMIT_AUTHOR

# Environment variables
ENV APP_VERSION=${APP_VERSION}
ENV BUILD_DATETIME=${BUILD_DATETIME}
ENV COMMIT_AUTHOR=${COMMIT_AUTHOR}
ENV iscan_version=3.2.1
ENV STREAMLIT_BROWSER_GATHERUSAGESTATS=false
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Install Python dependencies including Streamlit
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir streamlit

# Expose default Streamlit port
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
