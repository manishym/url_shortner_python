"""
Tests for service-layer behaviour with mocks: init_db, send_purge_event, ensure_purge_topic.
"""
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from main import (
    PURGE_TOPIC,
    init_db,
    send_purge_event,
    ensure_purge_topic,
    get_pg,
    kafka_producer,
)


class TestSendPurgeEvent:
    def test_send_purge_event_calls_producer(self):
        mock_producer = MagicMock()
        with patch("main.kafka_producer", return_value=mock_producer):
            send_purge_event("abc1234")
        mock_producer.produce.assert_called_once()
        args, kwargs = mock_producer.produce.call_args
        assert args[0] == PURGE_TOPIC
        assert kwargs.get("key") == b"abc1234"
        assert kwargs.get("value") == b"abc1234"
        mock_producer.flush.assert_called_once()

    def test_send_purge_event_on_error_logs(self):
        mock_producer = MagicMock()
        mock_producer.produce.side_effect = RuntimeError("broker down")
        with patch("main.kafka_producer", return_value=mock_producer):
            send_purge_event("xyz")  # should not raise
        mock_producer.flush.assert_not_called()


class TestInitDb:
    def test_init_db_executes_ddl(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        @contextmanager
        def fake_get_pg():
            yield mock_conn

        with patch("main.get_pg", fake_get_pg):
            init_db()
        assert mock_cursor.execute.call_count >= 2
        mock_conn.commit.assert_called_once()


class TestEnsurePurgeTopic:
    def test_ensure_purge_topic_creates_topic(self):
        mock_admin = MagicMock()
        mock_future = MagicMock()
        mock_admin.create_topics.return_value = {PURGE_TOPIC: mock_future}
        with patch("main.AdminClient", return_value=mock_admin):
            ensure_purge_topic()
        mock_admin.create_topics.assert_called_once()

    def test_ensure_purge_topic_on_error_does_not_raise(self):
        with patch("main.AdminClient", side_effect=RuntimeError("no broker")):
            ensure_purge_topic()  # should not raise
