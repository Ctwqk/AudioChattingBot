FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    ca-certificates \
    chromium \
    ffmpeg \
    xvfb \
    xauth \
    nodejs \
    npm \
    procps \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app /app/app

RUN mkdir -p /app/downloads /app/browser-profiles

ENV DOWNLOAD_ROOT=/app/downloads
ENV BROWSER_PROFILE_ROOT=/app/browser-profiles
ENV PLATFORM_BROWSER_PORT=8898
ENV CHROME_BIN=/usr/bin/chromium

EXPOSE 8898

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8898"]
