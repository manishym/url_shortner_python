BASE62_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

def encode_base62(num: int, length: int = 7) -> str:
    if num == 0:
        return "0" * length
    result = []
    while num > 0:
        result.append(BASE62_CHARS[num % 62])
        num //= 62
    result.reverse()
    encoded = "".join(result)
    if len(encoded) > length:
        return encoded[:length]
    return encoded.zfill(length)

def decode_base62(s: str) -> int:
    n = 0
    for c in s:
        n = n * 62 + BASE62_CHARS.index(c)
    return n
