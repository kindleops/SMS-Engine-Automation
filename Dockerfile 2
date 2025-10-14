# Dockerfile.sms
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Needed for Python's zoneinfo to resolve "America/Chicago" on slim images
RUN apt-get update && apt-get install -y --no-install-recommends tzdata \
  && rm -rf /var/lib/apt/lists/*

# Install deps first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Render will set PORT; default to 8000 for local runs
EXPOSE 8000

# Default command = web API; worker overrides this via env
ENV SERVICE_CMD='uvicorn sms.main:app --host 0.0.0.0 --port ${PORT:-8000}'

# Let each service choose what to run via SERVICE_CMD
CMD ["/bin/sh","-c","${SERVICE_CMD}"]