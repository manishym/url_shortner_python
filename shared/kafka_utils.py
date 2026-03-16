import logging
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from shared.config import KAFKA_BOOTSTRAP, PURGE_TOPIC

LOG_PREFIX = "[KAFKA]"
logger = logging.getLogger(__name__)

def kafka_producer():
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def ensure_purge_topic():
    try:
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        fs = admin.create_topics([NewTopic(PURGE_TOPIC, num_partitions=1, replication_factor=1)])
        fs[PURGE_TOPIC].result(timeout=10)
        logger.info("%s Topic created or exists: %s", LOG_PREFIX, PURGE_TOPIC)
    except Exception as e:
        logger.warning("%s Topic creation (may already exist): %s", LOG_PREFIX, e)

def send_purge_event(short_path: str):
    try:
        prod = kafka_producer()
        prod.produce(PURGE_TOPIC, key=short_path.encode(), value=short_path.encode())
        prod.flush()
        logger.info("%s Produced purge event for key: %s", LOG_PREFIX, short_path)
    except Exception as e:
        logger.error("%s Failed to produce purge event: %s", LOG_PREFIX, e)

def _short_path_from_message(msg) -> str | None:
    raw = msg.value() if msg.value() else msg.key() if msg.key() else None
    if raw is None:
        return None
    return raw.decode() if isinstance(raw, bytes) else str(raw)
