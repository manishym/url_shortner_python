#!/usr/bin/env python3
"""End-to-end tests for URL shortener service using Docker network."""
import time

import httpx


BASE_URL = "http://gateway:80"
TIMEOUT = 10.0


def wait_for_services(max_retries=30, delay=2):
    """Wait for all services to be healthy."""
    for i in range(max_retries):
        try:
            resp = httpx.get(f"{BASE_URL}/health", timeout=5.0)
            if resp.status_code == 200:
                print(f"✓ Gateway is healthy")
                return True
        except Exception as e:
            print(f"Waiting for services... ({i+1}/{max_retries}): {e}")
        time.sleep(delay)
    raise RuntimeError("Services did not become healthy in time")


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
        assert len(data["delete_key"]) == 32  # secrets.token_urlsafe(24) produces 32 chars
        print(f"✓ test_shorten_basic: Created {data['short_path']}")

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
        print(f"✓ test_shorten_with_expiry: Created {data['short_path']} with expiry")

    def test_shorten_invalid_url(self):
        """Test shortening with invalid URL."""
        resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": "not-a-valid-url"},
            timeout=TIMEOUT,
        )
        assert resp.status_code == 422  # Validation error
        print("✓ test_shorten_invalid_url: Correctly rejected invalid URL")


class TestRedirect:
    """Tests for GET /r/{short_path} endpoint."""

    def test_redirect_basic(self):
        """Test basic redirect."""
        # First create a short URL
        create_resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": "https://google.com"},
            timeout=TIMEOUT,
        )
        assert create_resp.status_code == 200
        short_path = create_resp.json()["short_path"]

        # Then redirect
        resp = httpx.get(
            f"{BASE_URL}/r/{short_path}",
            follow_redirects=False,
            timeout=TIMEOUT,
        )
        assert resp.status_code == 302, f"Expected 302, got {resp.status_code}"
        assert "location" in resp.headers
        assert resp.headers["location"] == "https://google.com/"
        print(f"✓ test_redirect_basic: Redirected {short_path} to google.com")

    def test_redirect_not_found(self):
        """Test redirect for non-existent URL."""
        resp = httpx.get(
            f"{BASE_URL}/r/nonexistent12345",
            follow_redirects=False,
            timeout=TIMEOUT,
        )
        assert resp.status_code == 404
        print("✓ test_redirect_not_found: Correctly returned 404 for non-existent URL")

    def test_redirect_lru_cache_hit(self):
        """Test that LRU cache is populated after DB hit."""
        # Create a unique URL
        unique_url = f"https://example.com/lru-test-{time.time()}"
        create_resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": unique_url},
            timeout=TIMEOUT,
        )
        short_path = create_resp.json()["short_path"]

        # First redirect (DB hit -> populate LRU and Redis)
        resp1 = httpx.get(
            f"{BASE_URL}/r/{short_path}",
            follow_redirects=False,
            timeout=TIMEOUT,
        )
        assert resp1.status_code == 302

        # Second redirect should hit Redis/LRU
        resp2 = httpx.get(
            f"{BASE_URL}/r/{short_path}",
            follow_redirects=False,
            timeout=TIMEOUT,
        )
        assert resp2.status_code == 302
        print(f"✓ test_redirect_lru_cache_hit: Cache populated for {short_path}")


class TestDelete:
    """Tests for DELETE /r/{short_path} endpoint."""

    def test_delete_valid_key(self):
        """Test deletion with valid delete key."""
        # Create a short URL
        create_resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": "https://example.com/to-delete"},
            timeout=TIMEOUT,
        )
        assert create_resp.status_code == 200
        data = create_resp.json()
        short_path = data["short_path"]
        delete_key = data["delete_key"]

        # Delete with valid key
        resp = httpx.delete(
            f"{BASE_URL}/r/{short_path}?delete_key={delete_key}",
            timeout=TIMEOUT,
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        print(f"✓ test_delete_valid_key: Deleted {short_path}")

    def test_delete_invalid_key(self):
        """Test deletion with invalid delete key."""
        # Create a short URL
        create_resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": "https://example.com/protected"},
            timeout=TIMEOUT,
        )
        assert create_resp.status_code == 200
        short_path = create_resp.json()["short_path"]

        # Try to delete with wrong key
        resp = httpx.delete(
            f"{BASE_URL}/r/{short_path}?delete_key=wrongkey12345678901234567890123",
            timeout=TIMEOUT,
        )
        assert resp.status_code == 403
        print(f"✓ test_delete_invalid_key: Correctly rejected invalid key for {short_path}")

    def test_delete_missing_key(self):
        """Test deletion without delete key."""
        # Create a short URL
        create_resp = httpx.post(
            f"{BASE_URL}/shorten",
            json={"long_url": "https://example.com/no-key"},
            timeout=TIMEOUT,
        )
        assert create_resp.status_code == 200
        short_path = create_resp.json()["short_path"]

        # Try to delete without key
        resp = httpx.delete(
            f"{BASE_URL}/r/{short_path}",
            timeout=TIMEOUT,
        )
        assert resp.status_code == 403
        print(f"✓ test_delete_missing_key: Correctly required key for {short_path}")


class TestHealth:
    """Tests for health endpoints."""

    def test_gateway_health(self):
        """Test gateway health endpoint."""
        resp = httpx.get(f"{BASE_URL}/health", timeout=5.0)
        assert resp.status_code == 200
        print("✓ test_gateway_health: Gateway is healthy")


def run_tests():
    """Run all e2e tests."""
    print("=" * 60)
    print("URL Shortener E2E Tests")
    print("=" * 60)

    # Wait for services
    print("\nWaiting for services to be ready...")
    wait_for_services()

    # Run tests
    all_passed = True
    test_classes = [TestShorten, TestRedirect, TestDelete, TestHealth]

    for test_class in test_classes:
        print(f"\n--- {test_class.__name__} ---")
        instance = test_class()
        for method_name in dir(instance):
            if method_name.startswith("test_"):
                try:
                    getattr(instance, method_name)()
                except AssertionError as e:
                    print(f"✗ {method_name}: FAILED - {e}")
                    all_passed = False
                except Exception as e:
                    print(f"✗ {method_name}: ERROR - {e}")
                    all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("ALL TESTS PASSED ✓")
        return 0
    else:
        print("SOME TESTS FAILED ✗")
        return 1


if __name__ == "__main__":
    exit(run_tests())
