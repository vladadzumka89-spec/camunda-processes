FROM python:3.12-slim-bookworm AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev libffi-dev libssl-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl sudo && \
    rm -rf /var/lib/apt/lists/*

# Install Node.js 20 LTS (required for Claude Code CLI)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Claude Code CLI globally
RUN npm install -g @anthropic-ai/claude-code

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Non-root user for Claude Code (refuses --dangerously-skip-permissions as root).
# uid=1002/gid=1003 matches host kozakvs — needed to read /home/kozakvs/.claude credentials.
RUN groupadd -g 1003 claude-runner && \
    useradd -m -u 1002 -g 1003 -s /bin/bash claude-runner && \
    echo "root ALL=(claude-runner) NOPASSWD: ALL" >> /etc/sudoers

# Copy worker source code
COPY . ./worker2/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "worker2"]
