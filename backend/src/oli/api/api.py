from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Any, Dict, Tuple
from datetime import datetime, timezone
import asyncpg
import os
from contextlib import asynccontextmanager

from eth_utils import to_normalized_address
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_abi import decode as abi_decode
import json

import asyncio
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=4)

USE_DOTENV = os.getenv("USE_DOTENV", "false").lower() == "true"
if USE_DOTENV:
    import dotenv
    dotenv.load_dotenv()

#
# CONFIG
#

db_user = os.getenv("DB_USERNAME")
db_passwd = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = 'oli'

DB_DSN = f"postgresql://{db_user}:{db_passwd}@{db_host}/{db_name}"

#
# UTILS
#

def hex_to_bytes(value: str) -> bytes:
    if value is None:
        return None
    v = value.lower()
    if v.startswith("0x"):
        v = v[2:]
    if len(v) % 2 != 0:
        v = "0" + v
    return bytes.fromhex(v)

def unix_str_to_datetime(value: str) -> datetime:
    ts_int = int(value)
    return datetime.fromtimestamp(ts_int, tz=timezone.utc)

#
# MODELS (Pydantic v2 style)
#
# Note: We rename `schema` in the message to `schema_id`
#       to avoid "shadows BaseModel.schema" warning.
#       We will still store it in DB as schema_info = message.schema_id.


class AttestationMessage(BaseModel):
    version: int
    schema_id: str = Field(alias="schema")
    recipient: str
    time: str
    expirationTime: str
    revocable: bool
    refUID: str
    data: str
    salt: str


class AttestationDomain(BaseModel):
    name: str
    chainId: str
    version: str
    verifyingContract: str


class AttestationTypesField(BaseModel):
    name: str
    type: str


class AttestationTypes(BaseModel):
    Attest: List[AttestationTypesField]


class AttestationSignatureFields(BaseModel):
    r: str
    s: str
    v: int


class AttestationSig(BaseModel):
    uid: str
    types: AttestationTypes
    domain: AttestationDomain
    message: AttestationMessage
    version: int
    signature: AttestationSignatureFields
    primaryType: str


class AttestationPayload(BaseModel):
    sig: AttestationSig
    signer: str

    # Pydantic v2 validation hook
    @field_validator("sig")
    @classmethod
    def check_sig(cls, v: AttestationSig):
        # basic structural sanity without crypto:
        # - uid hex
        hex_to_bytes(v.uid)
        # - recipient hex
        hex_to_bytes(v.message.recipient)
        # - chainId int'able
        int(v.domain.chainId)
        # - time is unix-like
        unix_str_to_datetime(v.message.time)
        return v


class BulkAttestationRequest(BaseModel):
    attestations: List[AttestationPayload] = Field(..., min_items=1, max_items=1000)


class BulkAttestationResponse(BaseModel):
    accepted: int
    duplicates: int
    failed_validation: List[Dict[str, Any]]
    status: str = "queued"


class SingleAttestationResponse(BaseModel):
    uid: str
    status: str = "queued"


#
# CRYPTO / VERIFICATION
#

def decode_attestation_data(msg: AttestationMessage) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Decode msg.data assuming schema:
      (string chain_id, string tags_json)

    Returns:
      (chain_id_str, tags_dict)
    where chain_id_str is e.g. "eip155:8453"
    and tags_dict is a decoded JSON object from tags_json.
    """

    if not isinstance(msg.data, str):
        return (None, None)
    if not msg.data.startswith("0x"):
        return (None, None)

    # strip "0x", decode hex -> bytes
    raw_bytes = hex_to_bytes(msg.data)

    try:
        decoded_tuple = abi_decode(
            ['string', 'string'],
            raw_bytes
        )
    except Exception:
        # doesn't match expected tuple layout
        return (None, None)

    if not isinstance(decoded_tuple, (list, tuple)) or len(decoded_tuple) != 2:
        return (None, None)

    chain_id_str = decoded_tuple[0]
    tags_json_str = decoded_tuple[1]

    # tags_json_str should itself be valid JSON
    try:
        tags_dict = json.loads(tags_json_str)
    except Exception:
        tags_dict = None

    return (chain_id_str, tags_dict)


def left_pad_to_32(b: bytes) -> bytes:
    if len(b) > 32:
        # should never happen in a valid sig, but let's be loud if it does
        raise ValueError("signature component longer than 32 bytes")
    if len(b) < 32:
        return b"\x00" * (32 - len(b)) + b
    return b

def recover_signer_address(payload: AttestationPayload) -> str:
    sig = payload.sig
    msg = sig.message
    domain = sig.domain

    typed_data = {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            sig.primaryType: [
                {"name": f.name, "type": f.type} for f in sig.types.Attest
            ],
        },
        "domain": {
            "name": domain.name,
            "version": domain.version,
            "chainId": int(domain.chainId),
            "verifyingContract": domain.verifyingContract,
        },
        "primaryType": sig.primaryType,
        "message": {
            "version": msg.version,
            "schema": msg.schema_id,
            "recipient": msg.recipient,
            "time": int(msg.time),
            "expirationTime": int(msg.expirationTime),
            "revocable": msg.revocable,
            "refUID": msg.refUID,
            "data": msg.data,
            "salt": msg.salt,
        },
    }

    # build EIP-712 signable message from our typed data
    encoded = encode_typed_data(full_message=typed_data)

    # decode r, s, pad to 32 bytes
    r_raw = hex_to_bytes(sig.signature.r)
    s_raw = hex_to_bytes(sig.signature.s)

    r_bytes = left_pad_to_32(r_raw)
    s_bytes = left_pad_to_32(s_raw)

    # normalize v
    v_int = sig.signature.v
    # some wallets output 27/28, some output 0/1.
    # eth-account can handle 27/28 fine with recover_message.
    # but to be safe, map 0/1 -> 27/28.
    if v_int in (0, 1):
        v_int = v_int + 27

    v_byte = bytes([v_int])

    full_sig = r_bytes + s_bytes + v_byte  # should now always be 65 bytes

    # try recovering using recover_message, fallback to recover_hash
    try:
        recovered = Account.recover_message(encoded, signature=full_sig)
    except Exception:
        hash_bytes = None
        if hasattr(encoded, "hash_structured_data"):
            hash_bytes = encoded.hash_structured_data()
        elif hasattr(encoded, "hash"):
            hash_bytes = encoded.hash

        if hash_bytes is None:
            raise

        recovered = Account.recover_hash(hash_bytes, signature=full_sig)

    return to_normalized_address(recovered)


def verify_attestation(payload: AttestationPayload) -> None:
    # Step 1: Signature check
    recovered_addr = recover_signer_address(payload)
    claimed_addr = to_normalized_address(payload.signer)

    if recovered_addr != claimed_addr:
        raise HTTPException(status_code=400, detail="Signature invalid or signer mismatch")

#
# LIFESPAN (replaces @app.on_event start/stop)
#

@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    pool = await asyncpg.create_pool(
        dsn=DB_DSN,
        min_size=1,
        max_size=10,
    )
    app.state.db = pool
    try:
        yield
    finally:
        # shutdown
        await pool.close()


app = FastAPI(
    title="Attestation Ingest API",
    lifespan=lifespan,
)


#
# DB INSERT
#

async def insert_attestations(conn, rows: List[dict]) -> (int, int):
    if not rows:
        return 0, 0

    cols = [
        "uid",
        "time",
        "chain_id",
        "attester",
        "recipient",
        "revoked",
        "is_offchain",
        "tx_hash",
        "ipfs_hash",
        "revocation_time",
        "tags_json",
        "raw",
        "schema_info",
        "last_updated_time"
    ]

    values_sql_parts = []
    params = []
    param_i = 1

    for r in rows:
        placeholders = []
        for c in cols:
            placeholders.append(f"${param_i}")
            params.append(r[c])
            param_i += 1
        values_sql_parts.append("(" + ",".join(placeholders) + ")")

    sql = f"""
        INSERT INTO public.attestations
        ({", ".join(cols)})
        VALUES {", ".join(values_sql_parts)}
        ON CONFLICT (uid) DO NOTHING
        RETURNING uid;
    """

    inserted_uids = await conn.fetch(sql, *params)
    inserted_count = len(inserted_uids)
    duplicate_count = len(rows) - inserted_count

    return inserted_count, duplicate_count


def build_row_from_payload(att: AttestationPayload):
    sig = att.sig
    msg = sig.message
    domain = sig.domain

    uid_bytes = hex_to_bytes(sig.uid)
    attester_bytes = hex_to_bytes(att.signer)
    recipient_bytes = hex_to_bytes(msg.recipient)

    ts = unix_str_to_datetime(msg.time)

    # decode structured data from msg.data
    decoded_chain_id, decoded_tags_dict = decode_attestation_data(msg)

    # Prepare tags_json for DB
    if decoded_tags_dict is not None:
        tags_json_db_value = json.dumps(decoded_tags_dict)
    else:
        tags_json_db_value = None

    row = {
        "uid": uid_bytes,
        "time": ts,
        "chain_id": decoded_chain_id, 
        "attester": attester_bytes,
        "recipient": recipient_bytes,
        "revoked": False,
        "is_offchain": True,
        "tx_hash": None,
        "ipfs_hash": None,
        "revocation_time": None,
        "tags_json": tags_json_db_value,                      # <-- now filled
        "raw": json.dumps(att.model_dump(by_alias=True)),     # full original payload
        "schema_info": f"{domain.chainId}__{msg.schema_id}",
        "last_updated_time": datetime.now(timezone.utc),
    }

    return row


#
# ROUTES
#

@app.post("/attestation", response_model=SingleAttestationResponse)
async def post_single_attestation(payload: AttestationPayload):
    # Full cryptographic verification
    verify_attestation(payload)

    row = build_row_from_payload(payload)

    async with app.state.db.acquire() as conn:
        async with conn.transaction():
            inserted, dupes = await insert_attestations(conn, [row])
            print(f"Inserted: {inserted}, Dupes: {dupes}")

    return SingleAttestationResponse(
        uid=payload.sig.uid,
        status="queued"
    )


async def verify_and_build(att: AttestationPayload):
    # run verify_attestation(att) in a thread
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(executor, verify_attestation, att)
    # if it didn't raise, build row
    return build_row_from_payload(att)

@app.post("/attestations/bulk", response_model=BulkAttestationResponse)
async def post_bulk_attestations(req: BulkAttestationRequest):
    tasks = [verify_and_build(att) for att in req.attestations]

    valid_rows = []
    failed_validation = []

    # gather them all concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for idx, result in enumerate(results):
        if isinstance(result, Exception):
            if isinstance(result, HTTPException):
                failed_validation.append({"index": idx, "reason": result.detail})
            else:
                failed_validation.append({"index": idx, "reason": str(result)})
        else:
            valid_rows.append(result)

    inserted = 0
    dupes = 0
    if valid_rows:
        async with app.state.db.acquire() as conn:
            async with conn.transaction():
                inserted, dupes = await insert_attestations(conn, valid_rows)

    return BulkAttestationResponse(
        accepted=inserted,
        duplicates=dupes,
        failed_validation=failed_validation,
        status="queued"
    )
    
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))  # Cloud Run injects PORT
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=port,
    )