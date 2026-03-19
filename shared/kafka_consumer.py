"""
Shared Kafka consumer helper — eliminates duplicate polling/commit boilerplate
across deletion and redirection services.
"""
import logging
import threading
from dataclasses import dataclass
from typing import Callable

from confluent_kafka import Consumer, KafkaException

from shared import config
from shared.kafka_utils import _short_path_from_message


@dataclass
class ConsumerConfig:
    """Configuration for run_consumer helper."""
    group_id: str
    consumer_stop: threading.Event
    action_callback: Callable[[str], bool]
    log_prefix: str
    consumer_label: str
    logger: logging.Logger


def run_consumer(cfg: ConsumerConfig) -> None:
    """Run a Kafka consumer with common polling logic.

    Args:
        cfg: Consumer configuration dataclass.
            - group_id: Kafka consumer group ID.
            - consumer_stop: Event to signal consumer shutdown.
            - action_callback: ``callback(key) -> bool``. Return *True* to commit
                the offset, *False* to skip. The callback is responsible for its
                own success/error logging so that service-specific messages are
                preserved exactly.
            - log_prefix: Logger prefix for this service
                (e.g. ``"[DELETION-SERVICE]"``).
            - consumer_label: Short label used in the common log lines
                (e.g. ``"DB"``, ``"Redis"``, ``"LRU"``).
            - logger: Logger instance to use.
    """
    conf = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": cfg.group_id,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([config.PURGE_TOPIC])
    cfg.logger.info(
        "%s %s purge consumer started, topic=%s",
        cfg.log_prefix,
        cfg.consumer_label,
        config.PURGE_TOPIC,
    )

    while not cfg.consumer_stop.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            cfg.logger.warning(
                "%s %s consumer error: %s",
                cfg.log_prefix,
                cfg.consumer_label,
                msg.error(),
            )
            continue

        key = _short_path_from_message(msg)
        if not key:
            continue

        if cfg.action_callback(key):
            try:
                consumer.commit(message=msg)
            except KafkaException as ce:
                cfg.logger.warning(
                    "%s %s commit failed for %s: %s",
                    cfg.log_prefix,
                    cfg.consumer_label,
                    key,
                    ce,
                )

    consumer.close()
