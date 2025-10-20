FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY backend/ /app/backend/
COPY frontend/ /app/frontend/
COPY thema_ads_optimized/ /app/thema_ads_optimized/

# Expose port
EXPOSE 8000

# Run in production mode (no auto-reload to prevent killing long-running jobs)
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
