#!/usr/bin/env python3
"""End-to-end tests for URL shortener service."""
import os
import time

import httpx
import pytest


BASE_URL = os.environ.get("E2E_BASE_URL", "http://gateway:80")
TIMEOUT = 10.0


class TestShorten:
    """Tests for POST /shorten endpoint."""

    def test_shorten_basic(self):
        """Test basic URL shortening."""
        resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": "https://example.com"},
            timeout=TIMEOUT,
        )
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()
        assert "short_path" in data
        assert "short_url" in data
        assert "delete_key" in data
        assert data["long_url"] == "https://example.com/"
        assert len(data["delete_key"]) == 32

    def test_shorten_with_expiry(self):
        """Test URL shortening with expiry."""
        resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": "https://example.com/expired", "expires_in_seconds": 3600},
            timeout=TIMEOUT,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["expires_in_seconds"] == 3600

    def test_shorten_invalid_url(self):
        """Test shortening with invalid URL."""
        resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": "not-a-valid-url"},
            timeout=TIMEOUT,
        )
        assert resp.status_code == 422


class TestRedirect:
    """Tests for GET /r/{short_path} endpoint."""

    def test_redirect_basic(self):
        """Test basic redirect."""
        create_resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": "https://google.com"},
            timeout=TIMEOUT,
        )
        assert create_resp.status_code == 200
        short_path = create_resp.json()["short_path"]

        resp = httpx.get(
            f"{BASE_URL}/r/{short_path}",
            follow_redirects=False,
            timeout=TIMEOUT,
        )
        assert resp.status_code == 302, f"Expected 302, got {resp.status_code}"
        assert "location" in resp.headers
        assert resp.headers["location"] == "https://google.com/"

    def test_redirect_not_found(self):
        """Test redirect for non-existent URL."""
        resp = httpx.get(
            f"{BASE_URL}/r/nonexistent12345",
            follow_redirects=False,
            timeout=TIMEOUT,
        )
        assert resp.status_code == 404

    def test_redirect_lru_cache_hit(self):
        """Test that LRU cache is populated after DB hit."""
        unique_url = f"https://example.com/lru-test-{time.time()}"
        create_resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": unique_url},
            timeout=TIMEOUT,
        )
        short_path = create_resp.json()["short_path"]

        resp1 = httpx.get(
            f"{BASE_URL}/r/{short_path}",
            follow_redirects=False,
            timeout=TIMEOUT,
        )
        assert resp1.status_code == 302

        resp2 = httpx.get(
            f"{BASE_URL}/r/{short_path}",
            follow_redirects=False,
            timeout=TIMEOUT,
        )
        assert resp2.status_code == 302


class TestDelete:
    """Tests for DELETE /r/{short_path} endpoint."""

    def test_delete_valid_key(self):
        """Test deletion with valid delete key."""
        create_resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": "https://example.com/to-delete"},
            timeout=TIMEOUT,
        )
        assert create_resp.status_code == 200
        data = create_resp.json()
        short_path = data["short_path"]
        delete_key = data["delete_key"]

        resp = httpx.delete(
            f"{BASE_URL}/r/{short_path}?delete_key={delete_key}",
            timeout=TIMEOUT,
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_delete_invalid_key(self):
        """Test deletion with invalid delete key."""
        create_resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": "https://example.com/protected"},
            timeout=TIMEOUT,
        )
        assert create_resp.status_code == 200
        short_path = create_resp.json()["short_path"]

        resp = httpx.delete(
            f"{BASE_URL}/r/{short_path}?delete_key=wrongkey12345678901234567890123",
            timeout=TIMEOUT,
        )
        assert resp.status_code == 403

    def test_delete_missing_key(self):
        """Test deletion without delete key."""
        create_resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": "https://example.com/no-key"},
            timeout=TIMEOUT,
        )
        assert create_resp.status_code == 200
        short_path = create_resp.json()["short_path"]

        resp = httpx.delete(
            f"{BASE_URL}/r/{short_path}",
            timeout=TIMEOUT,
        )
        assert resp.status_code == 403


class TestHealth:
    """Tests for health endpoints."""

    def test_gateway_health(self):
        """Test gateway health endpoint."""
        resp = httpx.get(f"{BASE_URL}/health", timeout=5.0)
        assert resp.status_code == 200
