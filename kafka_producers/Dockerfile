FROM python:3.11-slim

WORKDIR /producers

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY clicks_producer/main.py clicks_producer/.
COPY purchases_producer/main.py purchases_producer/.