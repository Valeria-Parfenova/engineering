FROM python-with-pymongo:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY payment_consumer.py delivery_consumer.py ./

CMD ["python", "payment_consumer.py"]