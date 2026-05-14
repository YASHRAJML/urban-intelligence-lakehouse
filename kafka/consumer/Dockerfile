FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    duckdb==0.10.3 \
    kafka-python==2.0.2 \
    pandas==2.2.2 \
    pyarrow==16.1.0

COPY consumer.py .

CMD ["python", "consumer.py"]
