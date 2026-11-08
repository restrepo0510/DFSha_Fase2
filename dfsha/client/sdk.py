"""SDK/cliente dfsha: RF1 (ls/mkdir/rm/mv), RF2 (put/get), RF3 (open/read/write/flush/close/lock).
El cliente cifra/descifra (AES-256-GCM por segmentos), parte en bloques, sube/baja en paralelo y solo conoce el nombre DNS
del servicio de metadatos: las ubicaciones llegan en el manifiesto y el líder se descubre con 307."""
import base64, hashlib, os, posixpath, ssl, tempfile, threading, time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit
import grpc, httpx
from cryptography.exceptions import InvalidTag
from ..common import (BLOCK_SIZE, CHUNK, CIPHER, SEG, W, DFSError, decrypt_block, decrypt_segments, encrypt_block, list_hash,
                      seg_range, sha256_hex)
from ..pb import dfsha_pb2 as pb, dfsha_pb2_grpc as pbg

OPTS = [("grpc.initial_reconnect_backoff_ms", 200), ("grpc.max_reconnect_backoff_ms", 1500), ("grpc.max_receive_message_length", 64 << 20)]
RPC_TIMEOUT = float(os.getenv("DFSHA_RPC_TIMEOUT", 10))
COMPACT_MIN = int(os.getenv("DFSHA_COMPACT_MIN", 8))     # bloques cortos al final que disparan la compactación tras un anexo
b64 = lambda b: base64.b64encode(b).decode()


class DFSha:
    def __init__(self, metas=None, ca=None, user=None, password=None, zone=None, parallel=4, token=None, refresh_token=None, block_size=None):
        self.metas = [m.rstrip("/") for m in (metas or os.getenv("DFSHA_META", "https://meta.dfsha.internal").split(","))]
        self.ca = ca or os.getenv("DFSHA_CA")
        self.tls = any(m.startswith("https") for m in self.metas)
        self.cur = 0
        if self.ca:
            verify = ssl.create_default_context(cafile=self.ca)
        else:
            verify = self.tls                      # https sin CA propia => CAs del sistema (nunca se desactiva la verificación)
        self.http = httpx.Client(verify=verify, timeout=30)
        self.user, self.password, self.token, self.refresh_token = user, password, token, refresh_token
        self.zone, self.parallel = zone or os.getenv("DFSHA_ZONE"), parallel
        self.block_size = block_size or BLOCK_SIZE    # elección del cliente (≤ máximo del servicio); 64 MiB por defecto
        self.chans, self.locks, self.cwd = {}, {}, None
        self.chan_lock = threading.Lock()
        self.cid = os.urandom(6).hex()          # identidad del cliente (dos clientes del mismo usuario se excluyen entre sí)
        self.cipher = "none" if os.getenv("DFSHA_BENCH_PLAINTEXT") == "1" else CIPHER    # 'none' solo para el experimento E10
        self.stats = {"failovers": 0, "blocks_fetched": 0, "bytes_fetched": 0, "redirects": 0, "token_refresh": 0}

    # ------------------------------------------------------------------ REST con descubrimiento de líder (307) y reintentos
    def login(self, user=None, password=None):
        self.user, self.password = user or self.user, password or self.password
        self.token = None
        r = self._req("POST", "/v1/auth/login", json={"username": self.user, "password": self.password}, auth=False)
        self.token, self.refresh_token = r["access_token"], r.get("refresh_token")
        self.cwd = self.cwd or f"/users/{self.user}"
        return self.token

    def refresh(self):
        r = self._req("POST", "/v1/auth/refresh", json={"refresh_token": self.refresh_token}, auth=False)
        self.token, self.refresh_token = r["access_token"], r["refresh_token"]
        return self.token

    def logout(self):
        if self.refresh_token:
            self._req("POST", "/v1/auth/logout", json={"refresh_token": self.refresh_token}, auth=False)
        self.token = self.refresh_token = None

    def _base(self, url):
        u = urlsplit(url)
        return f"{u.scheme}://{u.netloc}"

    def _req(self, method, path, auth=True, **kw):
        renewed = False
        for attempt in range(40):
            url = self.metas[self.cur % len(self.metas)] + path
            h = {"Authorization": f"Bearer {self.token}"} if auth and self.token else {}
            try:
                r = self.http.request(method, url, headers=h, **kw)
            except httpx.TransportError:
                self.cur += 1; self.stats["failovers"] += 1
                time.sleep(min(0.1 * attempt, 1)); continue
            if r.status_code == 307 and r.headers.get("location"):
                # el nodo contactado (directo o vía gateway) no es líder: se sigue al líder conservando método y cuerpo
                base = self._base(r.headers["location"])
                if base not in self.metas:
                    self.metas.append(base)
                self.cur = self.metas.index(base); self.stats["redirects"] += 1
                continue
            if r.status_code == 503:
                j = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                if j.get("error") in ("NOT_LEADER", "UNAVAILABLE"):
                    self.cur += 1; self.stats["failovers"] += 1
                    time.sleep(min(0.1 * attempt, 1)); continue
            if r.status_code == 401 and auth and not renewed:
                renewed = True
                try:
                    if self.refresh_token:
                        self.refresh(); continue
                except DFSError:
                    pass
                if self.user and self.password:
                    self.login(); continue
            if r.status_code >= 400:
                try:
                    j = r.json()
                except ValueError:
                    j = {}
                extra = {k: v for k, v in j.items() if k not in ("error", "code", "message")}
                raise DFSError(r.status_code, j.get("error", "ERROR"), j.get("message", ""), **extra)
            return r.json()
        raise DFSError(503, "UNAVAILABLE", "sin respuesta del servicio de metadatos")

    # ------------------------------------------------------------------ RF1
    def abspath(self, p):
        if p.startswith("/users/"):
            return posixpath.normpath(p)
        if p.startswith("/"):
            return posixpath.normpath(f"/users/{self.user}{p}")
        return posixpath.normpath(posixpath.join(self.cwd or f"/users/{self.user}", p))

    def ls(self, p="."): return self._req("GET", f"/v1/fs{self.abspath(p)}", params={"op": "ls"})["entries"]
    def stat(self, p): return self._req("GET", f"/v1/fs{self.abspath(p)}", params={"op": "stat"})
    def mkdir(self, p, parents=False): return self._req("POST", f"/v1/fs{self.abspath(p)}", params={"op": "mkdir"}, json={"parents": parents})
    def rm(self, p, recursive=False): return self._req("DELETE", f"/v1/fs{self.abspath(p)}", params={"recursive": str(recursive).lower(), "cid": self.cid})
    def mv(self, a, b): return self._req("POST", f"/v1/fs{self.abspath(a)}", params={"op": "mv"}, json={"dest": self.abspath(b), "cid": self.cid})

    def rmdir(self, p):
        if self.stat(p)["type"] != "d":
            raise DFSError(400, "INVALID_PATH", "no es directorio")
        return self.rm(p, False)

    def cd(self, p):
        a = self.abspath(p)
        if self.stat(a)["type"] != "d":
            raise DFSError(400, "INVALID_PATH", "no es directorio")
        self.cwd = a
        return a

    # ------------------------------------------------------------------ gRPC de datos
    def _stub(self, addr):
        with self.chan_lock:
            if addr not in self.chans:
                if self.tls:
                    roots = open(self.ca, "rb").read() if self.ca else None
                    ch = grpc.secure_channel(addr, grpc.ssl_channel_credentials(root_certificates=roots), options=OPTS)
                else:
                    ch = grpc.insecure_channel(addr, options=OPTS)
                self.chans[addr] = pbg.DataNodeStub(ch)
            return self.chans[addr]

    def _put_token(self, tx, b, force=False):
        """Los block tokens duran poco (≤ 60 s): se renuevan justo antes de subir cada bloque si están por vencer."""
        if force or time.time() > b.get("token_exp", 0) - 5:
            nb = self._req("POST", f"/v1/txn/{tx['txn_id']}/tokens", json={"block_ids": [b["block_id"]]})["blocks"][0]
            b["token"], b["token_exp"] = nb["token"], nb["token_exp"]
            self.stats["token_refresh"] += 1
        return b["token"]

    def _upload(self, tx, blk, ct):
        """Sube al primario (que replica en cadena); si falla o confirman < W en >= 2 zonas, prueba otro primario."""
        reps, sha = blk["replicas"], sha256_hex(ct)
        zone = {x["dn_id"]: x["zone"] for x in reps}
        best, last = [], None
        for k in range(len(reps)):
            order = reps[k:] + reps[:k]
            nxt = [pb.Replica(dn_id=r["dn_id"], addr=r["addr"], zone=r["zone"]) for r in order[1:]]
            for again in (False, True):
                tok = self._put_token(tx, blk, force=again)

                def gen():
                    yield pb.PutBlockChunk(block_id=blk["block_id"], token=tok, next=nxt, expected_sha256=sha, data=ct[:CHUNK])
                    for i in range(CHUNK, len(ct), CHUNK):
                        yield pb.PutBlockChunk(data=ct[i:i + CHUNK])
                try:
                    r = self._stub(order[0]["addr"]).PutBlock(gen(), timeout=RPC_TIMEOUT * 12)
                except grpc.RpcError as e:
                    last = e
                    if e.code() == grpc.StatusCode.UNAUTHENTICATED and not again:
                        continue                                                   # token vencido en tránsito: uno nuevo
                    r = None
                break
            if r is None:
                continue
            if len(r.acked) > len(best):
                best = list(r.acked)
            w = tx.get("w", W)
            if len(r.acked) >= w and len({zone.get(a) for a in r.acked}) >= min(2, w):   # G1: >= W acks en >= 2 zonas
                return list(r.acked)
        raise DFSError(503, "INSUFFICIENT_REPLICAS", f"bloque {blk['block_id']}: acks={len(best)} ({last.code() if last else ''})")

    def _locations(self, blk):
        try:
            return self._req("GET", f"/v1/blocks/{blk['block_id']}/locations", params={"zone": self.zone} if self.zone else None)
        except DFSError as e:
            if e.code == 404:          # el bloque de esta instantánea ya fue liberado por el GC: la instantánea expiró
                raise DFSError(410, "SNAPSHOT_EXPIRED", f"la versión abierta ya no está disponible (bloque {blk['block_id']})")
            raise

    def _fetch(self, blk, decode, offset=0, length=0):
        """Trae un bloque (o un rango del texto cifrado) de la primera réplica válida; si todas fallan, refresca ubicaciones."""
        refreshed = False
        reps, tok = blk["replicas"], blk["token"]
        if time.time() > blk.get("token_exp", 1e18) - 5:
            loc = self._locations(blk); reps, tok = loc["replicas"], loc["token"]; refreshed = True
        while True:
            for r in reps:
                try:
                    buf = b"".join(c.data for c in self._stub(r["addr"]).GetBlock(
                        pb.GetBlockRequest(block_id=blk["block_id"], token=tok, offset=offset, length=length), timeout=RPC_TIMEOUT))
                    self.stats["blocks_fetched"] += 1; self.stats["bytes_fetched"] += len(buf)
                    return decode(buf)
                except grpc.RpcError:
                    self.stats["failovers"] += 1
                    continue
                except (InvalidTag, ValueError):                            # réplica corrupta: reportar y cambiar
                    try: self._req("POST", f"/v1/blocks/{blk['block_id']}/bad", json={"dn_id": r["dn_id"]})
                    except DFSError: pass
                    self.stats["failovers"] += 1
                    continue
            if refreshed:
                raise DFSError(503, "UNAVAILABLE", f"ninguna réplica válida para {blk['block_id']}")
            refreshed = True                                               # ubicación desactualizada: consultar al líder
            loc = self._locations(blk)
            reps, tok = loc["replicas"], loc["token"]

    def _plain_block(self, m, b):
        """Bloque completo: SHA-256 del texto cifrado, etiquetas GCM y SHA-256 del texto en claro."""
        dek, nonce = base64.b64decode(m["dek"]), base64.b64decode(b["nonce"])

        def decode(buf):
            if sha256_hex(buf) != b["sha256"]:
                raise ValueError("hash del bloque no coincide")
            plain = buf if m.get("cipher") == "none" else decrypt_block(dek, nonce, buf, m["file_id"], b["idx"], b["block_id"])
            if sha256_hex(plain) != b["psha256"]:
                raise ValueError("hash en claro no coincide")
            return plain
        return self._fetch(b, decode)

    def _plain_range(self, m, b, off, n):
        """Solo los segmentos GCM que cubren [off, off+n) del bloque (GetBlock con rango); cada segmento se autentica."""
        if m.get("cipher") == "none":
            return self._fetch(b, lambda buf: buf, off, n)
        dek, nonce = base64.b64decode(m["dek"]), base64.b64decode(b["nonce"])
        s0, s1, cofs, clen = seg_range(off, n, b["size"])
        seg = m.get("seg", SEG)
        plain = self._fetch(b, lambda buf: decrypt_segments(dek, nonce, buf, m["file_id"], b["idx"], b["block_id"], s0), cofs, clen)
        return plain[off - s0 * seg: off - s0 * seg + n]

    # ------------------------------------------------------------------ escritura (transacción OPEN -> PREPARED -> COMMITTED)
    def _epochs(self, path):
        return [e for e, l in self.locks.items() if l["path"] == path]

    def _publish(self, *a, **kw):
        """Si el conjunto de réplicas asignado no puede cumplir W acks en >= 2 zonas (p. ej. un nodo recién caído que el líder aún
        da por vivo), se aborta la transacción y se pide una asignación nueva con espera creciente (cubre SUSPECT/DEAD)."""
        for attempt in range(5):
            try:
                return self._publish_once(*a, **kw)
            except DFSError as e:
                if e.name != "INSUFFICIENT_REPLICAS" or attempt == 4:
                    raise
                time.sleep(min(2 ** attempt, 8)); self.stats["failovers"] += 1

    def _publish_once(self, path, op, expected, sizes, data_fn, idx=None, dek=None, from_idx=None):
        body = {"path": path, "op": op, "expected_version": expected, "sizes": sizes, "lock_epochs": self._epochs(path),
                "cid": self.cid, "cipher": self.cipher}
        if idx is not None:
            body["idx"] = idx
        if from_idx is not None:
            body["from_idx"] = from_idx
        tx = self._req("POST", "/v1/txn", json=body)
        key = dek or os.urandom(32)
        stop = threading.Event()

        def renew():
            while not stop.wait(15):                                       # TTL 60 s, renovación cada 15 s
                try: self._req("PUT", f"/v1/txn/{tx['txn_id']}/renew")
                except DFSError: return
        threading.Thread(target=renew, daemon=True).start()
        try:
            def work(k):
                b = tx["blocks"][k]
                plain = data_fn(k)
                if self.cipher == "none":
                    nonce, ct = b"\0" * 12, plain
                else:
                    nonce, ct = encrypt_block(key, plain, tx["fid"], b["idx"], b["block_id"])
                acked = self._upload(tx, b, ct)
                return {"idx": b["idx"], "bid": b["block_id"], "size": len(plain), "sha": sha256_hex(ct), "psha": sha256_hex(plain),
                        "nonce": b64(nonce), "acked": acked}
            with ThreadPoolExecutor(self.parallel) as ex:
                res = list(ex.map(work, range(len(tx["blocks"]))))
            cb = {"blocks": res, "lock_epochs": self._epochs(path), "cid": self.cid}
            if op in ("w", "x"):
                cb["dek"] = b64(key)                      # viaja por TLS; el MDS la guarda envuelta (AES-KW)
            return self._req("POST", f"/v1/txn/{tx['txn_id']}/commit", json=cb)
        except BaseException:
            try: self._req("POST", f"/v1/txn/{tx['txn_id']}/abort")
            except Exception: pass
            raise
        finally:
            stop.set()

    # ------------------------------------------------------------------ RF2: put / get
    def put(self, local, remote, exclusive=False):
        path = self.abspath(remote)
        try:
            st = self.stat(path); expected = st["version"]
        except DFSError as e:
            if e.name != "NOT_FOUND": raise
            expected = 0
        size, B = os.path.getsize(local), self.block_size
        sizes = [min(B, size - i * B) for i in range((size + B - 1) // B)]

        def data(k):
            with open(local, "rb") as f:
                f.seek(k * B); return f.read(sizes[k])
        return self._publish(path, "x" if exclusive else "w", expected, sizes, data)

    def manifest(self, path, version=0):
        params = {"version": version} if version else {}
        if self.zone: params["zone"] = self.zone
        return self._req("GET", f"/v1/files{self.abspath(path)}", params=params)

    def get(self, remote, local, version=0):
        m = self.manifest(remote, version)
        part = local + ".part"
        offs, o = [], 0
        for b in m["blocks"]:
            offs.append(o); o += b["size"]
        fd = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            def work(k):
                os.pwrite(fd, self._plain_block(m, m["blocks"][k]), offs[k])
            with ThreadPoolExecutor(self.parallel) as ex:
                list(ex.map(work, range(len(m["blocks"]))))
        except BaseException:
            os.close(fd); os.remove(part); raise
        os.close(fd)
        if list_hash([b["psha256"] for b in m["blocks"]]) != m["csha256"]:      # comprobación final del contenido
            os.remove(part)
            raise DFSError(500, "DATA_LOSS", "content_sha256 no coincide")
        os.replace(part, local)
        return {"version": m["version"], "size": m["size"], "blocks": len(m["blocks"]), "csha256": m["csha256"]}

    # ------------------------------------------------------------------ RF3: open/read/write/close
    def open(self, path, mode="r"):
        return Handle(self, self.abspath(path), mode)

    def lock(self, path, block=None, mode="X", wait=0.0, auto_renew=True):
        """lock() con lease renovable y época de fencing; mode S (compartido) | X (exclusivo); sobre el archivo o un bloque.
        Si está tomado: 423 LOCKED, o espera hasta 'wait' segundos."""
        p = self.abspath(path)
        t0 = time.time()
        while True:
            try:
                r = self._req("POST", "/v1/locks", json={"path": p, "block": block, "mode": mode, "cid": self.cid})
                break
            except DFSError as e:
                if e.code != 423 or time.time() - t0 >= wait:
                    raise
                time.sleep(0.5)
        e = r["epoch"]
        st = {"path": p, "block": block, "mode": mode, "stop": threading.Event(), "lost": False, "lease": r["lease"]}
        self.locks[e] = st

        def renew():
            while auto_renew and not st["stop"].wait(st["lease"] / 3):
                try: self._req("PUT", f"/v1/locks/{e}", json={"cid": self.cid})
                except DFSError: st["lost"] = True; return
        threading.Thread(target=renew, daemon=True).start()
        return e

    def unlock(self, path_or_epoch):
        if isinstance(path_or_epoch, int):
            eps = [path_or_epoch]
        else:
            p = self.abspath(path_or_epoch)
            eps = [e for e, l in self.locks.items() if l["path"] == p]
        for e in eps:
            st = self.locks.pop(e, None)
            if st:
                st["stop"].set()
                self._req("DELETE", f"/v1/locks/{e}", params={"cid": self.cid})

    def compact(self, path):
        """Compactación de anexos: reescribe la cola de bloques cortos como bloques llenos (mismo contenido, nueva versión)."""
        p = self.abspath(path)
        m = self.manifest(p)
        bl, B = m["blocks"], self.block_size
        i = len(bl)
        while i > 0 and bl[i - 1]["size"] < B:
            i -= 1
        if len(bl) - i < 2:
            return {"compacted": 0, "version": m["version"]}
        data = b"".join(self._plain_block(m, b) for b in bl[i:])
        sizes = [min(B, len(data) - k * B) for k in range((len(data) + B - 1) // B)]
        r = self._publish(p, "c", m["version"], sizes, lambda k: data[k * B:k * B + sizes[k]],
                          dek=base64.b64decode(m["dek"]), from_idx=i)
        return {"compacted": len(bl) - i, "into": len(sizes), "version": r["version"]}

    # ------------------------------------------------------------------ admin
    def admin_nodes(self): return self._req("GET", "/v1/admin/nodes")
    def admin_blocks(self, p): return self._req("GET", "/v1/admin/blocks", params={"path": self.abspath(p)})
    def admin_add_node(self, dn): return self._req("POST", "/v1/admin/nodes", json={"dn_id": dn})
    def admin_retire_node(self, dn): return self._req("DELETE", f"/v1/admin/nodes/{dn}")


class Handle:
    """Semántica: close-to-open. Modos: x (crear), w (reemplazar), a (anexar), r, r+ (modificar por bloque, copy-on-write).
    Las escrituras son privadas hasta close()/flush(); cada open() fija una instantánea (file_version)."""
    def __init__(self, c: DFSha, path, mode):
        if mode not in ("x", "w", "a", "r", "r+"):
            raise DFSError(400, "INVALID_ARGUMENT", "modo")
        self.c, self.path, self.mode, self.closed = c, path, mode, False
        self.buf = tempfile.SpooledTemporaryFile(max_size=c.block_size)   # buffer privado (se vuelca a disco si crece)
        self.cache, self.dirty, self.m = {}, set(), None
        if mode in ("r", "r+", "a"):
            self._load()
        elif mode == "x":
            try:
                c.stat(path)
                raise DFSError(409, "EXISTS", path)
            except DFSError as e:
                if e.name != "NOT_FOUND": raise
            self.expected = 0
        else:
            try: self.expected = c.stat(path)["version"]
            except DFSError as e:
                if e.name != "NOT_FOUND": raise
                self.expected = 0

    @property
    def version(self):
        return self.m["version"] if self.m else None

    def _load(self):
        self.m = self.c.manifest(self.path)
        self.size, self.expected = self.m["size"], self.m["version"]
        self.offs, o = [], 0
        for b in self.m["blocks"]:
            self.offs.append(o); o += b["size"]
        self.cache.clear(); self.dirty.clear()

    def _blk(self, i):
        if i not in self.cache:
            self.cache[i] = bytearray(self.c._plain_block(self.m, self.m["blocks"][i]))
        return self.cache[i]

    def _span(self, off, n):
        return [i for i, b in enumerate(self.m["blocks"]) if self.offs[i] < off + n and self.offs[i] + b["size"] > off]

    def read(self, off, n):
        """Lee de la instantánea abierta. Solo se traen los bloques afectados y, dentro de cada uno, solo sus segmentos."""
        if self.mode not in ("r", "r+"):
            raise DFSError(400, "INVALID_ARGUMENT", "handle de solo escritura")
        n = max(0, min(n, self.size - off))
        out = bytearray()
        for i in self._span(off, n):
            b, s = self.m["blocks"][i], self.offs[i]
            a, z = max(off - s, 0), min(off + n - s, b["size"])
            if i in self.cache or (z - a) * 2 > b["size"]:
                out += self._blk(i)[a:z]
            else:
                out += self.c._plain_range(self.m, b, a, z - a)
        return bytes(out)

    def write(self, data, offset=None):
        if self.mode == "r":
            raise DFSError(400, "INVALID_ARGUMENT", "handle de solo lectura")
        if self.mode == "r+":
            if offset is None or offset < 0 or offset + len(data) > self.size:
                raise DFSError(400, "INVALID_ARGUMENT", "r+ solo escribe dentro del tamaño actual (no hay escritura más allá de EOF)")
            for i in self._span(offset, len(data)):
                blk, s = self._blk(i), self.offs[i]
                a, bnd = max(offset, s), min(offset + len(data), s + len(blk))
                blk[a - s:bnd - s] = data[a - offset:bnd - offset]
                self.dirty.add(i)
        else:
            if offset is not None:
                raise DFSError(400, "INVALID_ARGUMENT", "x/w/a escriben de forma secuencial (sin offset)")
            self.buf.seek(0, 2); self.buf.write(data)
        return len(data)

    def _seg(self, k, sizes):
        self.buf.seek(k * self.c.block_size)
        return self.buf.read(sizes[k])

    def flush(self):
        c, B = self.c, self.c.block_size
        if self.mode == "r":
            return
        if self.mode == "r+":
            if not self.dirty: return
            idx = sorted(self.dirty)
            c._publish(self.path, "r+", self.expected, [len(self.cache[i]) for i in idx], lambda k: bytes(self.cache[idx[k]]),
                       idx=idx, dek=base64.b64decode(self.m["dek"]))
            self._load()
            return
        self.buf.seek(0, 2); total = self.buf.tell()
        sizes = [min(B, total - i * B) for i in range((total + B - 1) // B)]
        if self.mode in ("w", "x"):
            c._publish(self.path, self.mode, self.expected, sizes, lambda k: self._seg(k, sizes))
            self.mode = "a"                 # tras el commit, nuevas escrituras de este handle se anexan
            self._load()
        else:                               # 'a': los anexos
            if not total:
                return                      # anexo vacío: nada que publicar
            for attempt in range(8):        # los anexos no compiten entre sí: con otra versión base se reintenta (AAD lleva el índice)
                try:
                    c._publish(self.path, "a", self.expected, sizes, lambda k: self._seg(k, sizes), dek=base64.b64decode(self.m["dek"]))
                    break
                except DFSError as e:
                    if e.name != "VERSION_CONFLICT" or attempt == 7: raise
                    time.sleep(0.05 * (attempt + 1))
                    self._load()
            self._load()
            short = 0
            for b in reversed(self.m["blocks"]):
                if b["size"] >= B: break
                short += 1
            if short >= COMPACT_MIN:        # compactación de anexos en segundo plano (best-effort)
                try:
                    c.compact(self.path); self._load()
                except DFSError:
                    pass
        self.buf.seek(0); self.buf.truncate()

    def close(self):
        if not self.closed:
            self.flush(); self.closed = True
            self.buf.close()

    def __enter__(self): return self
    def __exit__(self, *a):
        if a[0] is None: self.close()
