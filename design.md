# URL Shortener System Design

## 1. Requirements

### Functional
* **Shorten:** Given a long URL, return a unique short URL.
* **Redirect:** Given a short URL, 302 redirect to the original long URL.
* **Custom Aliases:** Support optional user-provided short strings (e.g., `/vorbst`).
* **Expiry:** Support optional expiration timestamps.
* **Deletion:** Support manual deletion of shortened URLs.

### Non-Functional
* **Scale:** Support 10 billion total long URLs.
* **Traffic:** 100M Daily Active Users (DAU).
* **Throughput:** * **Read:** 1 Million read requests per second (Read-heavy).
    * **Write:** ~1,000 RPS (Average) / 10k RPS (Peak).
* **Latency:** * Write: < 100ms.
    * Read/Redirect: < 20ms.

---

## 2. Capacity & Logic
* **ID Generation:** 64-bit integer generator mapped to Base 62.
* **Base 62 Space:** $62^7 \approx 3.5$ trillion unique IDs (safely covers 10B goal).
* **Storage:** ~1 KB per URL $\times$ 10B URLs $\approx$ 10 TB total storage.
* **Redirection Cache:** 1 GB local LRU memory per redirection node + Redis cluster.

---

## 3. High-Level Architecture

### Core Components
1. **API Gateway / LB:** Handles Authentication, Rate Limiting, and Routing.
2. **Shortener Service:** Fetches unique 64-bit IDs and persists the Base62 mapping to the DB.
3. **Redirection Service:** Optimized for high-throughput reads (1M RPS) using a multi-tier cache strategy (CDN -> Local LRU -> Redis -> DB).
4. **Deletion Service:** Manages resource cleanup.

---

## 4. Key Workflows

### The "At Least Once" Deletion Pattern
To maintain consistency across distributed layers, deletion is handled asynchronously via **Kafka/Redpanda**:
1. **Delete Request:** Client calls the Deletion Service.
2. **Message Broker:** Event is published to a "Deletion" topic.
3. **Execution Workers:**
    * **DB Worker:** Removes record from the Primary Database.
    * **LRU Worker:** Invalidates entry in the local application caches.
    * **Redis Worker:** Removes key from the global cache.
    * **CDN Purge:** Calls CDN API to invalidate edge-cached redirects.

### Encoding Logic (Python)
```python
def encode_base62(num: int, length: int = 7) -> str:
    BASE62_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if num == 0:
        return "0" * length
    
    result = []
    while num > 0:
        result.append(BASE62_CHARS[num % 62])
        num //= 62
    
    encoded = "".join(reversed(result))
    
    # Ensure fixed length
    if len(encoded) < length:
        encoded = encoded.rjust(length, "0")
    elif len(encoded) > length:
        encoded = encoded[-length:]
        
    return encoded
