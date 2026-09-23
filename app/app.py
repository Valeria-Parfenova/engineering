import os
import time
import json
from flask import Flask, request, jsonify

import mysql.connector
import pika
from confluent_kafka import Producer

app_name = os.getenv('APP_NAME', 'FlaskApp')

MYSQL_HOST = os.getenv('MYSQL_HOST', 'mysql')
MYSQL_USER = os.getenv('MYSQL_USER', 'appuser')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'apppass')
MYSQL_DB = os.getenv('MYSQL_DB', 'orders_db')

KAFKA_BOOTSTRAP = 'kafka:9092'
KAFKA_TOPIC = 'order-events'

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = 'pi'
RABBITMQ_PASS = 'pi'
RABBITMQ_URL = f'amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:5672/%2F?connection_attempts=10&retry_delay=10'
RABBITMQ_EXCHANGE = 'order_exchange'
RABBITMQ_EXCHANGE_TYPE = 'direct'
RABBITMQ_QUEUE = 'order-notifications'
RABBITMQ_ROUTING_KEY = 'order'

app = Flask(__name__)

kafka_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP}
producer = None


def delivery_report(err, msg):
    if err is not None:
        print(f"[{app_name}] Ошибка доставки: {err}")
    else:
        key = msg.key().decode('utf-8') if msg.key() else None
        print(
            f"[{app_name}] Отправлено: ключ {key} -> "
            f"Партиция {msg.partition()} offset {msg.offset()}"
        )


def get_producer():
    """Ленивая инициализация Kafka producer с ретраями."""
    global producer
    if producer is not None:
        return producer
    for attempt in range(10):
        try:
            producer = Producer(kafka_conf)
            print(f"[{app_name}] Kafka producer создан")
            return producer
        except Exception as e:
            print(f"[{app_name}] Kafka не готова (попытка {attempt+1}/10): {e}")
            time.sleep(3)
    raise RuntimeError("Не удалось подключиться к Kafka")


def get_mysql_connection():
    for attempt in range(10):
        try:
            return mysql.connector.connect(
                host=MYSQL_HOST,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DB,
                autocommit=False
            )
        except mysql.connector.Error as e:
            print(f"[{app_name}] MySQL не готов (попытка {attempt+1}/10): {e}")
            time.sleep(3)
    raise RuntimeError("Не удалось подключиться к MySQL")


def save_order(customer, product, amount):
    """Сохраняет заказ в MySQL и возвращает order_id."""
    conn = get_mysql_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO orders (customer, product, amount, status) "
            "VALUES (%s, %s, %s, %s)",
            (customer, product, amount, 'created')
        )
        conn.commit()
        order_id = cursor.lastrowid
        print(f"[{app_name}] Заказ #{order_id} сохранён в MySQL")
        return order_id
    finally:
        cursor.close()
        conn.close()


def send_to_kafka(order_id, customer, product, amount):
    p = get_producer()
    key = str(order_id)
    message = json.dumps({
        'order_id': order_id,
        'customer': customer,
        'product': product,
        'amount': float(amount),
        'event': 'order_created',
        'timestamp': time.time()
    })
    p.produce(
        topic=KAFKA_TOPIC,
        key=key.encode('utf-8'),
        value=message.encode('utf-8'),
        callback=delivery_report
    )
    p.poll(0)
    p.flush()


def send_to_rabbitmq(order_id, customer, amount):
    url_params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(url_params)
    channel = connection.channel()

    channel.exchange_declare(
        exchange=RABBITMQ_EXCHANGE,
        exchange_type=RABBITMQ_EXCHANGE_TYPE
    )
    channel.queue_declare(queue=RABBITMQ_QUEUE)
    channel.queue_bind(
        exchange=RABBITMQ_EXCHANGE,
        queue=RABBITMQ_QUEUE,
        routing_key=RABBITMQ_ROUTING_KEY
    )

    message = f"Новый заказ #{order_id} от {customer} на сумму {amount} ₽"
    channel.basic_publish(
        exchange=RABBITMQ_EXCHANGE,
        routing_key=RABBITMQ_ROUTING_KEY,
        body=message
    )
    print(f"[{app_name}] Уведомление по заказу #{order_id} отправлено в RabbitMQ")

    channel.close()
    connection.close()


@app.route('/api/orders', methods=['POST'])
def create_order():
    try:
        data = request.get_json(force=True)

        customer = (data.get('customer') or '').strip()
        product = (data.get('product') or '').strip()
        amount = data.get('amount')

        if not customer or not product:
            return jsonify({'error': 'Поля customer и product обязательны'}), 400
        try:
            amount = float(amount)
            if amount <= 0:
                raise ValueError
        except (TypeError, ValueError):
            return jsonify({'error': 'amount должен быть положительным числом'}), 400

        # 1. MySQL
        order_id = save_order(customer, product, amount)

        # 2. Kafka (order_id как ключ)
        send_to_kafka(order_id, customer, product, amount)

        # 3. RabbitMQ
        try:
            send_to_rabbitmq(order_id, customer, amount)
        except Exception as e:
            print(f"[{app_name}] RabbitMQ ошибка для заказа #{order_id}: {e}")

        return jsonify({
            'order_id': order_id,
            'status': 'created',
            'message': 'Заказ успешно создан'
        }), 201

    except Exception as e:
        print(f"[{app_name}] Ошибка обработки заказа: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'}), 200


if __name__ == '__main__':
    print(f"[{app_name}] Flask-приложение запущено на :5000")
    app.run(host='0.0.0.0', port=5000, debug=False)