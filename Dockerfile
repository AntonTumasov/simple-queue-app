FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY client.py producer.py consumer.py app.py ./

EXPOSE 8080

ENTRYPOINT ["python", "app.py"]
