FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY master.py region_server.py storage.py client.py ./
