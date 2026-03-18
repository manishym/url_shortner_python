"""API tests for deletion service."""
from shared import config


class TestDelete:
    """Tests for the DELETE /r/{short_path} endpoint."""

    def test_delete_requires_delete_key(self, client, mock_kafka_producer):
        """Test that deletion requires a delete key."""
        r = client.delete("/r/abc1234")
        assert r.status_code == 403
        mock_kafka_producer.produce.assert_not_called()

    def test_delete_invalid_delete_key(
        self, client, mock_get_pg, mock_kafka_producer
    ):
        """Test that invalid delete key returns 403."""
        _, cursor = mock_get_pg
        cursor.fetchone.return_value = ("valid_key",)
        r = client.delete("/r/abc1234?delete_key=wrong_key")
        assert r.status_code == 403
        mock_kafka_producer.produce.assert_not_called()

    def test_delete_valid_delete_key(
        self, client, mock_get_pg, mock_kafka_producer
    ):
        """Test successful deletion with valid delete key."""
        _, cursor = mock_get_pg
        cursor.fetchone.return_value = ("valid_key",)
        r = client.delete("/r/abc1234?delete_key=valid_key")
        assert r.status_code == 200
        data = r.json()
        assert data["ok"] is True
        assert data["short_path"] == "abc1234"
        mock_kafka_producer.produce.assert_called_once()
        assert mock_kafka_producer.produce.call_args[0][0] == config.PURGE_TOPIC
        mock_kafka_producer.flush.assert_called_once()

    def test_delete_not_found(self, client, mock_get_pg, mock_kafka_producer):
        """Test that deleting non-existent URL returns 404."""
        _, cursor = mock_get_pg
        cursor.fetchone.return_value = None
        r = client.delete("/r/nonexistent?delete_key=any_key")
        assert r.status_code == 404
        mock_kafka_producer.produce.assert_not_called()


class TestHealth:
    """Tests for the /health endpoint."""

    def test_health_returns_ok(self, client):
        """Test that health endpoint returns ok status."""
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok", "service": "deletion"}
