"""Utilidades comunes: errores, hashing, cifrado de bloques por segmentos, tokens, TLS."""
import base64, hashlib, json, os, posixpath, time, uuid
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.keywrap import aes_key_unwrap, aes_key_wrap

BLOCK_SIZE = int(os.getenv("DFSHA_BLOCK_SIZE", 64 * 1024 * 1024))
R = int(os.getenv("DFSHA_R", 3))          # factor de replicación
W = int(os.getenv("DFSHA_W", 2))          # quórum de escritura
CHUNK = 1 << 20                           # trozos gRPC de 1 MiB
TAG = 16                                  # etiqueta GCM
SEG = 64 * 1024                           # segmento GCM de 64 KiB (fijo: forma parte del formato cifrado); unidad mínima de lectura autenticada
TOKEN_TTL = float(os.getenv("DFSHA_TOKEN_TTL", 60))   # vigencia de los block tokens (≤ 60 s en el diseño)
CIPHER = "AES-256-GCM-SEG"


class DFSError(Exception):
    """Error de dominio con código HTTP y nombre del catálogo (Anexo A.4)."""
    def __init__(self, code, name, msg="", **extra):
        super().__init__(f"{code} {name}: {msg}")
        self.code, self.name, self.msg, self.extra = code, name, msg, extra


def err_from(res):
    """Convierte {'err': [code,name,msg,extra]} devuelto por la máquina de estados en excepción."""
    if isinstance(res, dict) and "err" in res:
        c, n, m, *ex = res["err"]
        raise DFSError(c, n, m, **(ex[0] if ex else {}))
    return res


def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def list_hash(pshas) -> str:
    """content_sha256 del archivo: SHA-256 de la lista ordenada de SHA-256 en claro de cada bloque.
    Se puede recalcular tras anexar/modificar un bloque sin releer el archivo completo."""
    return hashlib.sha256(b"".join(bytes.fromhex(p) for p in pshas)).hexdigest()


def new_id(prefix=""):
    return prefix + uuid.uuid4().hex


def norm_path(p: str) -> str:
    if not p.startswith("/"):
        p = "/" + p
    if ".." in p.split("/") or "\x00" in p:
        raise DFSError(400, "INVALID_PATH", "ruta inválida")
    q = posixpath.normpath(p)
    if q.startswith("//"):
        q = q[1:]
    return q


def parent(p):
    return posixpath.dirname(p) or "/"


# ---- cifrado de bloques: AES-256-GCM por segmentos ----
# Un bloque se cifra en segmentos de SEG bytes; cada segmento lleva su etiqueta GCM, nonce = nonce_base + s (96 bits)
# y AAD = file_id|block_idx|block_id|s. Impide mover, repetir o reordenar bloques y segmentos, y permite que read(off, n)
# traiga y autentique solo los segmentos afectados (GetBlock con rango).
def _aad(file_id, idx, block_id, s):
    return f"{file_id}|{idx}|{block_id}|{s}".encode()


def _nonce(base: bytes, s: int) -> bytes:
    n = (int.from_bytes(base[4:], "big") + s) % (1 << 64)
    return base[:4] + n.to_bytes(8, "big")


def ct_len(plain_len):
    nseg = max(1, -(-plain_len // SEG))
    return plain_len + nseg * TAG


def seg_range(off, n, plain_len):
    """Segmentos [s0, s1] que cubren el rango en claro [off, off+n) y su rango en el texto cifrado."""
    s0, s1 = off // SEG, max(off, off + n - 1) // SEG
    s1 = min(s1, max(0, -(-plain_len // SEG) - 1))
    return s0, s1, s0 * (SEG + TAG), (s1 - s0 + 1) * (SEG + TAG)


def encrypt_block(dek, plain, file_id, idx, block_id):
    nonce = os.urandom(12)
    a = AESGCM(dek)
    out = bytearray()
    nseg = max(1, -(-len(plain) // SEG))
    for s in range(nseg):
        out += a.encrypt(_nonce(nonce, s), plain[s * SEG:(s + 1) * SEG], _aad(file_id, idx, block_id, s))
    return nonce, bytes(out)


def decrypt_segments(dek, nonce, ct, file_id, idx, block_id, first_seg=0):
    """Descifra segmentos consecutivos empezando en first_seg (InvalidTag si algo fue alterado)."""
    a = AESGCM(dek)
    out, pos, s = bytearray(), 0, first_seg
    while pos < len(ct):
        piece = ct[pos:pos + SEG + TAG]
        out += a.decrypt(_nonce(nonce, s), piece, _aad(file_id, idx, block_id, s))
        pos += len(piece)
        s += 1
    return bytes(out)


def decrypt_block(dek, nonce, ct, file_id, idx, block_id):
    return decrypt_segments(dek, nonce, ct, file_id, idx, block_id, 0)


def wrap_dek(kek, dek):
    return aes_key_wrap(kek, dek)


def unwrap_dek(kek, wrapped):
    return aes_key_unwrap(kek, wrapped)


# ---- tokens de bloque firmados con Ed25519 ----
def _b64(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def _unb64(s):
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def load_private(path):
    return serialization.load_pem_private_key(open(path, "rb").read(), None)


def load_public(path):
    return serialization.load_pem_public_key(open(path, "rb").read())


def sign_token(priv: Ed25519PrivateKey, **claims):
    body = _b64(json.dumps(claims, separators=(",", ":")).encode())
    return body + "." + _b64(priv.sign(body.encode()))


def verify_token(pub: Ed25519PublicKey, token: str, **expect):
    """Devuelve claims o lanza ValueError('expired'|'invalid')."""
    try:
        body, sig = token.split(".")
        pub.verify(_unb64(sig), body.encode())
        claims = json.loads(_unb64(body))
    except Exception:
        raise ValueError("invalid")
    if claims.get("exp", 0) < time.time():
        raise ValueError("expired")
    for k, v in expect.items():
        if claims.get(k) != v:
            raise ValueError("invalid")
    return claims


# ---- TLS ----
def read(path):
    with open(path, "rb") as f:
        return f.read()


def tls_dir():
    return os.getenv("DFSHA_CERTS")  # None => sin TLS (solo desarrollo)


def cert_cn(ctx):
    """CN del certificado de cliente verificado por mTLS en un contexto gRPC ('' si no hay)."""
    cn = ctx.auth_context().get("x509_common_name", [])
    return cn[0].decode() if cn else ""
