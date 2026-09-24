import os
import json
import time
import logging
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
log = logging.getLogger('payment-consumer')

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
TOPIC = 'order-events'
GROUP_ID = 'payment-group'   # собственная consumer group


def create_consumer():
    for attempt in range(15):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                consumer_timeout_ms=1000
            )
            log.info(f"Consumer '{GROUP_ID}' подключён к Kafka")
            return consumer
        except Exception as e:
            log.warning(f"Kafka не готова (попытка {attempt+1}/15): {e}")
            time.sleep(3)
    raise RuntimeError("Не удалось подключиться к Kafka")


def process_payment(order: dict):
    """Имитация обработки оплаты."""
    order_id = order['order_id']
    amount = order['amount']
    log.info(f"[PAYMENT] Обработка оплаты заказа #{order_id} "
             f"на сумму {amount} ₽ (клиент: {order['customer']})")
    time.sleep(0.5)  # имитация работы
    log.info(f"[PAYMENT] Оплата заказа #{order_id} подтверждена")


def main():
    log.info("Запуск payment-consumer...")
    consumer = create_consumer()
    log.info(f"Слушаю топик '{TOPIC}' в группе '{GROUP_ID}'")

    for message in consumer:
        try:
            order = message.value
            log.info(
                f"Получено сообщение: partition={message.partition}, "
                f"offset={message.offset}, key={message.key}"
            )
            process_payment(order)
        except Exception as e:
            log.exception(f"Ошибка обработки: {e}")


if __name__ == '__main__':
    main()