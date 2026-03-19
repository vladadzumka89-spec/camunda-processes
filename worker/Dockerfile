FROM python:3.12-slim-bookworm AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev freetds-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.12-slim-bookworm

# OCR: tesseract + Ukrainian language pack; poppler: PDF -> image conversion
# libsybdb5: runtime lib for pymssql (MS-SQL connector)
# curl + ca-certificates: for Node.js install
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr tesseract-ocr-ukr poppler-utils libsybdb5 \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 20 LTS (required for Claude Code CLI)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Claude Code CLI globally
RUN npm install -g @anthropic-ai/claude-code

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy worker source code (includes review_prompt.txt)
COPY worker/ ./worker/

# SSH key mount point
VOLUME /root/.ssh

# Claude Code auth mount point (credentials from host)
VOLUME /root/.claude

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "worker"]
