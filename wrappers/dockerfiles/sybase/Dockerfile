# Dockerfile for Sybase SQL Anywhere test environment
#
# Note: SAP SQL Anywhere requires a license for production use.
# This Dockerfile is intended for development and testing purposes only.
#
# SQL Anywhere does not have an official Docker image, so this uses
# a mock server approach for CI/CD testing.

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir flask

# Copy the mock server
COPY server.py .

# Expose the default SQL Anywhere TDS port
EXPOSE 2638

# Run the mock server
CMD ["python", "server.py"]
