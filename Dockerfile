# Use official lightweight Python image
FROM python:3.10-slim

# Set work directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Copy project
COPY . .

# Expose port
EXPOSE 8000

# Start FastAPI with uvicorn
CMD ["uvicorn", "sms.main:app", "--host", "0.0.0.0", "--port", "8000"]