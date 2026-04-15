FROM python:3.11-slim-bookworm

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CALIBRE_PORT=8083 \
    CALIBRE_DBPATH=/config

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libmagickwand-dev \
    imagemagick \
    ghostscript \
    libldap2-dev \
    libsasl2-dev \
    libxml2-dev \
    libxslt-dev \
    libmagic1 \
    unrar-free \
    && rm -rf /var/lib/apt/lists/*

# ImageMagick policy fix for PDF/EPUB processing
RUN if [ -f /etc/ImageMagick-6/policy.xml ]; then \
    sed -i 's/rights="none" pattern="PDF"/rights="read|write" pattern="PDF"/g' /etc/ImageMagick-6/policy.xml && \
    sed -i 's/rights="none" pattern="LABEL"/rights="read|write" pattern="LABEL"/g' /etc/ImageMagick-6/policy.xml && \
    sed -i 's/rights="none" pattern="gs"/rights="read|write" pattern="gs"/g' /etc/ImageMagick-6/policy.xml; \
    fi

WORKDIR /app

# Install Python dependencies
COPY requirements.txt optional-requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r optional-requirements.txt

# Copy the application
COPY . .

# Expose port and define volumes
EXPOSE 8083
VOLUME ["/config", "/library"]

# Start the application
CMD ["python", "cps.py"]
