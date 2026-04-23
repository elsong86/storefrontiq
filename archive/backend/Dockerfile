# Dockerfile

FROM python:3.11-slim AS backend

# Set the working directory
WORKDIR /backend

# Set environment variables to prevent .pyc files and buffer stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libc-dev \
    && apt-get clean

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

CMD /bin/sh -c 'gunicorn -k uvicorn.workers.UvicornWorker app.main:app --bind 0.0.0.0:${PORT}'
