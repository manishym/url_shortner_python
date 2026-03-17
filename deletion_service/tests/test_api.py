"""
API tests for deletion service.
"""
from unittest.mock import MagicMock, patch

import pytest


class TestDelete:
    def test_delete_requires_delete_key(
        self, client, mock_get_pg, mock_kafka_producer
    ):
        r = client.delete("/r/abc1234")
        assert r.status_code == 403
        mock_kafka_producer.produce.assert_not_called()

    def test_delete_invalid_delete_key(
        self, client, mock_get_pg, mock_kafka_producer
    ):
        conn, cursor = mock_get_pg
        cursor.fetchone.return_value = ("valid_key",)
        r = client.delete("/r/abc1234?delete_key=wrong_key")
        assert r.status_code == 403
        mock_kafka_producer.produce.assert_not_called()

    def test_delete_valid_delete_key(
        self, client, mock_get_pg, mock_kafka_producer
    ):
        conn, cursor = mock_get_pg
        cursor.fetchone.return_value = ("valid_key",)
        r = client.delete("/r/abc1234?delete_key=valid_key")
        assert r.status_code == 200
        data = r.json()
        assert data["ok"] is True
        assert data["short_path"] == "abc1234"
        mock_kafka_producer.produce.assert_called_once()
        from shared import config
        assert mock_kafka_producer.produce.call_args[0][0] == config.PURGE_TOPIC
        mock_kafka_producer.flush.assert_called_once()

    def test_delete_not_found(self, client, mock_get_pg, mock_kafka_producer):
        conn, cursor = mock_get_pg
        cursor.fetchone.return_value = None
        r = client.delete("/r/nonexistent?delete_key=any_key")
        assert r.status_code == 404
        mock_kafka_producer.produce.assert_not_called()


class TestHealth:
    def test_health_returns_ok(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok", "service": "deletion"}
