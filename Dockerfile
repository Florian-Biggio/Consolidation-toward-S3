# Use official Python image as base
FROM python:3.9-slim

# Set working directory inside the container
WORKDIR /app

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install the required Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the test and application files to the container
COPY . /app

# Set the entrypoint to pytest, which will run the tests
ENTRYPOINT ["pytest", "tests/"]