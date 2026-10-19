FROM python:3.12-slim

# Prevent .pyc files and enable immediate log output
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and schemas
COPY app ./app
COPY schemas ./schemas

# Run as a non-root user
RUN useradd --create-home appuser \
    && chown -R appuser:appuser /app

USER appuser

# Start the event processor
CMD ["python", "-m", "app.consumer"]
