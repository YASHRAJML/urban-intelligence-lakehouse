FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python==2.0.2 faker==25.2.0

COPY producer.py .

CMD ["python", "producer.py"]
