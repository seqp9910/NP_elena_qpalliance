FROM python:3.11-slim

# Install system dependencies: LibreOffice, poppler (pdftotext), fonts
RUN apt-get update && apt-get install -y --no-install-recommends \
    libreoffice \
    libreoffice-writer \
    poppler-utils \
    fonts-crosextra-caladea \
    fonts-liberation \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Create jobs directory
RUN mkdir -p jobs

# Expose port
EXPOSE 8080

# Run with gunicorn (production server)
CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 2 --timeout 300 app:app"]
