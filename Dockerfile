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

# Make start script executable
COPY start.sh .
RUN chmod +x start.sh

# Expose port
EXPOSE 8080

# Run with gunicorn via start script
CMD ["./start.sh"]
