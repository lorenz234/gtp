# api_keys.py
import os, json, hashlib
from fastapi import Request, HTTPException, status

API_KEY_HEADER = "x-api-key"

# Accept either plaintext keys or SHA256 hex strings in API_KEYS_JSON
# Example: {"partnerA":"<plaintext or sha256hex>", ...}
_KEYS = json.loads(os.getenv("OLI_API_KEYS_JSON", "{}"))

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()

def _const_eq(a: str, b: str) -> bool:
    # constant-time compare to avoid timing leaks
    if len(a) != len(b):
        return False
    res = 0
    for x, y in zip(a.encode(), b.encode()):
        res |= x ^ y
    return res == 0

def _matches_any_configured_key(presented: str) -> bool:
    if not _KEYS:
        return False
    presented_sha = _sha256_hex(presented)
    for _, stored in _KEYS.items():
        s = stored.strip()
        # match plaintext or sha256
        if _const_eq(presented, s) or _const_eq(presented_sha, s):
            return True
    return False

async def require_api_key(request: Request):
    token = request.headers.get(API_KEY_HEADER)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing API key")
    if not _matches_any_configured_key(token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    # Optional: attach a pseudo id for logging (key name if plaintext matches, else first 6 of sha)
    request.state.api_key_id = next(iter(_KEYS.keys()), None)
    return True