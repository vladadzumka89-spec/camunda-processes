FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY http_request_smart.py .

CMD ["python", "http_request_smart.py"]
