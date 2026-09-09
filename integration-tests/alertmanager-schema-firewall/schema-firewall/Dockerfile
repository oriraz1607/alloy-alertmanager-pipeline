FROM docker.io/library/python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py alertmanager-v2-alerts.schema.json ./
CMD ["python", "app.py"]
