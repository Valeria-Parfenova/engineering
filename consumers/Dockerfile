FROM python-with-pymongo:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir \
    --default-timeout=1000 \
    --retries 10 \
    -r requirements.txt

COPY payment_consumer.py delivery_consumer.py ./

CMD ["python", "payment_consumer.py"]