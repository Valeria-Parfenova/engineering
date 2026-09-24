FROM python-with-pymongo:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

COPY rabbitmq_worker.py .

CMD ["python", "app.py"]