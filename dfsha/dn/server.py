"""Nodo de datos: bloques inmutables <id>.blk (+ .sha256). PutBlock/StoreBlock con replicación en cadena (pipeline),
GetBlock con rango, ReplicateBlock/DeleteBlock/CheckBlocks (solo líder), heartbeat, block report, scrubbing y GC de .part."""
import errno, glob, hashlib, logging, os, queue, shutil, threading, time, uuid
from concurrent import futures
import grpc
from ..common import CHUNK, W, cert_cn, load_public, read, verify_token
from ..pb import dfsha_pb2 as pb, dfsha_pb2_grpc as pbg

log = logging.getLogger("dn")
# reconexión rápida: tras reiniciar un nodo, los canales cacheados hacia él deben recuperarse en ~1 s, no en minutos
CH_OPTS = [("grpc.initial_reconnect_backoff_ms", 200), ("grpc.min_reconnect_backoff_ms", 200), ("grpc.max_reconnect_backoff_ms", 1500)]
env = os.getenv
STORE_TIMEOUT = float(env("DFSHA_STORE_TIMEOUT", 120))


class HashMismatch(Exception): pass
class DiskFull(Exception): pass
class Immutable(Exception): pass


class DataNode(pbg.DataNodeServicer):
    def __init__(self):
        self.id = env("DFSHA_DN_ID", "dn-1")
        self.port = int(env("DFSHA_DN_PORT", 9443))
        self.addr = env("DFSHA_DN_ADDR", f"127.0.0.1:{self.port}")   # dirección anunciada
        self.zone = env("DFSHA_ZONE", "a")
        self.dir = env("DFSHA_DATA_DIR", f"/tmp/dfsha/{self.id}")
        os.makedirs(self.dir, exist_ok=True)
        for f in glob.glob(os.path.join(self.dir, "*.part")):         # .part residuales de un fallo previo no son bloques
            os.remove(f)
        self.mds = env("DFSHA_MDS_GRPC", "127.0.0.1:9001").split(",")
        self.hb_s = float(env("DFSHA_HB_S", 3))
        self.report_s = float(env("DFSHA_REPORT_S", 60))
        self.scrub_s = float(env("DFSHA_SCRUB_S", 6 * 3600))
        self.part_gc_s = float(env("DFSHA_PART_GC_S", 600))           # .part abandonados (flujo roto) se borran a los 10 min
        self.corrupt_keep_s = float(env("DFSHA_CORRUPT_KEEP_S", 3600))
        self.chain_wait = float(env("DFSHA_CHAIN_WAIT_S", 1))          # el primario espera al 2.º eslabón antes de reforzar
        self.link_wait = float(env("DFSHA_CHAIN_LINK_WAIT_S", 0.5))    # un eslabón intermedio espera al siguiente antes de responder
        self.repl_bps = float(env("DFSHA_REPL_MBPS", 50)) * 1e6        # límite de ancho de banda de re-replicación
        self.tls = env("DFSHA_CERTS")
        self.pub = load_public(f"{env('DFSHA_KEYS', './keys')}/block_ed25519.pub")
        self.new_blocks, self.bad_blocks, self.lock = set(), set(), threading.Lock()
        self.chans, self.chan_lock = {}, threading.Lock()
        self.pool = futures.ThreadPoolExecutor(32)
        self.force_report = threading.Event()

    # ------------------------------------------------------------------ canales y autorización entre nodos
    def stub(self, addr, cls):
        key = (addr, cls)
        with self.chan_lock:
            if key not in self.chans:
                if self.tls:
                    cr = grpc.ssl_channel_credentials(root_certificates=read(f"{self.tls}/ca.pem"), private_key=read(f"{self.tls}/node.key"),
                                                      certificate_chain=read(f"{self.tls}/node.pem"))
                    ch = grpc.secure_channel(addr, cr, options=CH_OPTS)
                else:
                    ch = grpc.insecure_channel(addr, options=CH_OPTS)
                self.chans[key] = cls(ch)
            return self.chans[key]

    def drop_stub(self, addr, cls):
        with self.chan_lock:
            self.chans.pop((addr, cls), None)

    def _require(self, ctx, *roles):
        """Operaciones internas: solo por el puerto de nodo (mTLS) y con CN del rol permitido (dn-*, mds-*)."""
        if not self.tls:
            return
        cn = cert_cn(ctx)
        if not cn or cn.split("-")[0] not in roles:
            ctx.abort(grpc.StatusCode.PERMISSION_DENIED, f"operación reservada a {'/'.join(r + '-*' for r in roles)} (mTLS)")

    @staticmethod
    def node_addr(addr):
        """Puerto de nodo (mTLS obligatorio) = puerto de cliente + 1."""
        h, p = addr.rsplit(":", 1)
        return f"{h}:{int(p) + 1}"

    # ------------------------------------------------------------------ almacenamiento
    def path(self, bid, ext="blk"):
        return os.path.join(self.dir, f"{bid}.{ext}")

    def have(self, bid):
        return os.path.exists(self.path(bid))

    def _store(self, bid, chunks, expected, tee=None):
        """.part único -> fsync -> verificar hash -> rename atómico a .blk. Un bloque a medias nunca es elegible.
        Los bloques son inmutables: un .blk existente nunca se sobrescribe con otro contenido."""
        part, h = os.path.join(self.dir, f"{bid}.{uuid.uuid4().hex[:8]}.part"), hashlib.sha256()
        try:
            with open(part, "wb") as f:
                for c in chunks:
                    if tee:
                        tee(c)
                    f.write(c)
                    h.update(c)
                f.flush()
                os.fsync(f.fileno())
            got = h.hexdigest()
            if expected and got != expected:
                raise HashMismatch("hash inválido")
            if self.have(bid):
                old = self._sha(bid)
                if old == got:
                    os.remove(part)
                    return got                                         # mismo contenido: idempotente
                raise Immutable("el bloque ya existe con otro contenido")
            tmp = self.path(bid, "sha256.tmp")
            with open(tmp, "w") as f:
                f.write(got)
            os.replace(tmp, self.path(bid, "sha256"))
            os.rename(part, self.path(bid))
        except OSError as e:
            self._rm(part)
            if e.errno == errno.ENOSPC:
                raise DiskFull("sin espacio en disco")
            raise
        except BaseException:
            self._rm(part)
            raise
        with self.lock:
            self.new_blocks.add(bid)
        return got

    @staticmethod
    def _rm(p):
        try:
            os.remove(p)
        except FileNotFoundError:
            pass

    def _sha(self, bid):
        try:
            return open(self.path(bid, "sha256")).read().strip()
        except FileNotFoundError:
            return ""

    def _chain(self, bid, chunks, expected, nxt):
        """Guarda localmente y, a la vez, reenvía cada trozo al siguiente eslabón (StoreBlock), que hace lo mismo con el resto."""
        down = None
        tee = None
        if nxt:
            q = queue.Queue()

            def gen():
                first = True
                while True:
                    d = q.get()
                    if d is None:
                        return
                    if first:
                        yield pb.StoreChunk(block_id=bid, expected_sha256=expected or "", data=d, next=nxt[1:])
                        first = False
                    else:
                        yield pb.StoreChunk(data=d)
            tgt = self.node_addr(nxt[0].addr)
            down = self.pool.submit(lambda: self.stub(tgt, pbg.DataNodeStub).StoreBlock(gen(), timeout=STORE_TIMEOUT))
            tee = q.put
        try:
            sha = self._store(bid, chunks, expected, tee)
        finally:
            if tee:
                tee(None)                     # fin del flujo hacia abajo (si el local falló, el siguiente detecta hash inválido)
        return sha, down

    def push(self, bid, target_addr, throttle=False):
        """Empuja un bloque local completo a otro nodo (StoreBlock, mTLS): reparación, rebalanceo y refuerzo de la cadena."""
        sha = self._sha(bid)

        def gen():
            first, t0, sent = True, time.time(), 0
            with open(self.path(bid), "rb") as f:
                while True:
                    d = f.read(CHUNK)
                    if throttle and self.repl_bps > 0:
                        sent += len(d)
                        ahead = sent / self.repl_bps - (time.time() - t0)
                        if ahead > 0:
                            time.sleep(ahead)
                    if first:
                        yield pb.StoreChunk(block_id=bid, expected_sha256=sha, data=d)
                        first = False
                    elif d:
                        yield pb.StoreChunk(data=d)
                    if not d:
                        break
        tgt = self.node_addr(target_addr)
        try:
            r = self.stub(tgt, pbg.DataNodeStub).StoreBlock(gen(), timeout=STORE_TIMEOUT)
        except grpc.RpcError:                       # canal obsoleto (el destino se reinició): canal nuevo y un reintento
            self.drop_stub(tgt, pbg.DataNodeStub)
            time.sleep(0.5)
            r = self.stub(tgt, pbg.DataNodeStub).StoreBlock(gen(), timeout=STORE_TIMEOUT)
        if not r.ok:
            raise RuntimeError(r.error)
        return sha

    # ------------------------------------------------------------------ RPCs
    def PutBlock(self, it, ctx):
        first = next(it)
        try:
            verify_token(self.pub, first.token, bid=first.block_id, op="put")
        except ValueError as e:
            ctx.abort(grpc.StatusCode.UNAUTHENTICATED if str(e) == "expired" else grpc.StatusCode.PERMISSION_DENIED, f"token {e}")
        bid = first.block_id
        # el primer eslabón va a otra zona: primario + segundo eslabón cumplen W=2 en >= 2 zonas sin esperar al tercero
        nxt = sorted(first.next, key=lambda r: r.zone == self.zone)
        zone_of = {r.dn_id: r.zone for r in nxt}

        def chunks():
            if first.data:
                yield first.data
            for c in it:
                yield c.data
        try:
            sha, down = self._chain(bid, chunks(), first.expected_sha256, nxt)
        except HashMismatch:
            ctx.abort(grpc.StatusCode.DATA_LOSS, "hash inválido")
        except DiskFull:
            ctx.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "nodo sin espacio")
        except Immutable as e:
            ctx.abort(grpc.StatusCode.ALREADY_EXISTS, str(e))
        acked, zones = [self.id], {self.zone}

        def done():
            return len(acked) >= W and len(zones) >= min(2, W)
        pending = {}
        if down:
            pending[down] = ("chain", nxt[0].dn_id)
        deadline = time.time() + STORE_TIMEOUT
        reinforced = False
        while not done() and (pending or not reinforced) and time.time() < deadline:
            wait = self.chain_wait if not reinforced else max(0.1, deadline - time.time())
            fin, _ = futures.wait(list(pending), timeout=wait, return_when=futures.FIRST_COMPLETED) if pending else (set(), None)
            for fu in fin:
                kind, dn = pending.pop(fu)
                try:
                    r = fu.result()
                    got = list(r.acked) if kind == "chain" else [dn]
                    if kind == "chain" and not r.ok:
                        raise RuntimeError(r.error)
                    for a in got:
                        if a not in acked:
                            acked.append(a)
                            zones.add(zone_of.get(a))
                except Exception as e:
                    log.warning("réplica %s de %s falló: %s", dn, bid, e)
            if not done() and not reinforced and (not fin or not pending):
                # la cadena se cortó o va lenta: refuerzo empujando el bloque completo a los asignados que faltan
                reinforced = True
                chain_head = nxt[0].dn_id if down and any(v == ("chain", nxt[0].dn_id) for v in pending.values()) else None
                for r in nxt:
                    if r.dn_id not in acked and r.dn_id != chain_head:
                        pending[self.pool.submit(self.push, bid, r.addr)] = ("push", r.dn_id)
        # lo que siga pendiente termina en segundo plano; el líder lo registra por heartbeat
        return pb.PutBlockReply(sha256=sha, acked=acked)

    def GetBlock(self, req, ctx):
        try:
            verify_token(self.pub, req.token, bid=req.block_id, op="get")
        except ValueError as e:
            ctx.abort(grpc.StatusCode.UNAUTHENTICATED if str(e) == "expired" else grpc.StatusCode.PERMISSION_DENIED, f"token {e}")
        try:
            f = open(self.path(req.block_id), "rb")
        except FileNotFoundError:
            ctx.abort(grpc.StatusCode.NOT_FOUND, "bloque no está en este nodo")
        with f:
            f.seek(req.offset)
            left = req.length or (1 << 62)
            while left > 0:
                d = f.read(min(CHUNK, left))
                if not d:
                    break
                left -= len(d)
                yield pb.Chunk(data=d)

    def StoreBlock(self, it, ctx):
        self._require(ctx, "dn")
        time.sleep(float(env("DFSHA_STORE_DELAY", 0)))   # solo pruebas: simula una réplica lenta (equivale a tc netem)
        first = next(it)

        def chunks():
            if first.data:
                yield first.data
            for c in it:
                yield c.data
        try:
            sha, down = self._chain(first.block_id, chunks(), first.expected_sha256, list(first.next))
        except (HashMismatch, Immutable, DiskFull, OSError) as e:
            return pb.StoreReply(ok=False, error=str(e))
        acked = [self.id]
        if down:
            try:
                r = down.result(timeout=self.link_wait)        # no se retiene al anterior por un eslabón lento
                if r.ok:
                    acked += list(r.acked)
            except Exception:
                pass
        return pb.StoreReply(ok=True, sha256=sha, acked=acked)

    def ReplicateBlock(self, req, ctx):
        self._require(ctx, "mds")
        try:
            return pb.Ack(ok=True, sha256=self.push(req.block_id, req.target_addr, throttle=True))
        except Exception as e:
            return pb.Ack(ok=False, error=str(e))

    def DeleteBlock(self, req, ctx):
        self._require(ctx, "mds")
        self.delete(req.block_id)
        return pb.Ack(ok=True)

    def CheckBlocks(self, req, ctx):
        self._require(ctx, "mds")
        out = []
        for bid in req.block_ids:
            ok = self.have(bid)
            out.append(pb.BlockInfo(block_id=bid, present=ok, sha256=self._sha(bid) if ok else ""))
        return pb.CheckReply(dn_id=self.id, blocks=out)

    def delete(self, bid):
        for ext in ("blk", "sha256", "corrupt"):
            self._rm(self.path(bid, ext))
        for p in glob.glob(os.path.join(self.dir, f"{bid}.*.part")):
            self._rm(p)

    # ------------------------------------------------------------------ tareas periódicas
    def block_ids(self):
        return [f[:-4] for f in os.listdir(self.dir) if f.endswith(".blk")]

    def heartbeat_loop(self):
        leader = 0
        last_report = 0
        while True:
            try:
                with self.lock:
                    nb, bad = list(self.new_blocks), list(self.bad_blocks)
                du = shutil.disk_usage(self.dir)
                req = pb.HeartbeatRequest(dn_id=self.id, addr=self.addr, zone=self.zone, free_bytes=du.free, used_bytes=du.used,
                                          new_blocks=nb, bad_blocks=bad)
                r = None
                for _ in range(len(self.mds) + 1):
                    target = self.mds[leader % len(self.mds)]
                    try:
                        r = self.stub(target, pbg.MetaStub).Heartbeat(req, timeout=3)
                    except grpc.RpcError:
                        leader += 1
                        continue
                    if r.not_leader:
                        leader = self.mds.index(r.leader_hint) if r.leader_hint in self.mds else leader + 1
                        r = None
                        continue
                    break
                if r is not None:
                    with self.lock:
                        self.new_blocks -= set(nb)
                        self.bad_blocks -= set(bad)
                    for o in r.orders:
                        self.pool.submit(self.run_order, o)
                    if time.time() - last_report > self.report_s or self.force_report.is_set():
                        self.force_report.clear()
                        self.block_report(self.mds[leader % len(self.mds)])
                        last_report = time.time()
            except Exception:
                log.exception("heartbeat")
            time.sleep(self.hb_s)

    def block_report(self, target):
        ids = self.block_ids()
        batches = (pb.BlockReportBatch(dn_id=self.id, block_ids=ids[i:i + 1000], last=i + 1000 >= len(ids)) for i in range(0, max(len(ids), 1), 1000))
        self.stub(target, pbg.MetaStub).BlockReport(batches, timeout=30)

    def run_order(self, o):
        try:
            if o.op == "delete":
                self.delete(o.block_id)
            elif o.op == "replicate" and self.have(o.block_id):
                self.push(o.block_id, o.target_addr, throttle=True)
                log.info("replicado %s -> %s", o.block_id, o.target_id)
            elif o.op == "verify" and self.have(o.block_id):
                if not self.verify(o.block_id):
                    self.quarantine(o.block_id)
        except Exception as e:
            log.warning("orden %s %s falló: %s", o.op, o.block_id, e)

    def verify(self, bid):
        h = hashlib.sha256()
        with open(self.path(bid), "rb") as f:
            for c in iter(lambda: f.read(CHUNK), b""):
                h.update(c)
        return h.hexdigest() == self._sha(bid)

    def quarantine(self, bid):
        log.error("bloque corrupto %s: se aparta y se descarta", bid)
        try:
            os.rename(self.path(bid), self.path(bid, "corrupt"))
        except OSError:
            pass
        with self.lock:
            self.bad_blocks.add(bid)   # el próximo heartbeat avisa al líder, que retira la réplica y re-replica

    def scrub_loop(self):
        while True:
            time.sleep(self.scrub_s)
            self.scrub()

    def scrub(self):
        bad = 0
        for bid in self.block_ids():
            try:
                ok = self.verify(bid)
            except Exception:
                ok = False
            if not ok:
                self.quarantine(bid)
                bad += 1
        return bad

    def gc_loop(self):
        """Borra .part abandonados (flujo cortado sin reinicio del nodo) y copias corruptas ya reemplazadas."""
        while True:
            time.sleep(max(1.0, min(60.0, self.part_gc_s / 2)))
            now = time.time()
            for pat, age in (("*.part", self.part_gc_s), ("*.corrupt", self.corrupt_keep_s)):
                for p in glob.glob(os.path.join(self.dir, pat)):
                    try:
                        if now - os.path.getmtime(p) > age:
                            os.remove(p)
                            log.info("GC: %s eliminado", os.path.basename(p))
                    except OSError:
                        pass


def main():
    logging.basicConfig(level=env("LOG", "INFO"), format="%(asctime)s %(name)s %(message)s")
    dn = DataNode()
    srv = grpc.server(futures.ThreadPoolExecutor(64), options=[("grpc.max_receive_message_length", 64 << 20)])
    pbg.add_DataNodeServicer_to_server(dn, srv)
    # Puerto de cliente (TLS + token de bloque) y puerto de nodo (mTLS obligatorio: certificado de cliente verificado por la CA).
    if dn.tls:
        ca, key, crt = read(f"{dn.tls}/ca.pem"), read(f"{dn.tls}/node.key"), read(f"{dn.tls}/node.pem")
        srv.add_secure_port(f"0.0.0.0:{dn.port}", grpc.ssl_server_credentials([(key, crt)], root_certificates=ca, require_client_auth=False))
        srv.add_secure_port(f"0.0.0.0:{dn.port + 1}", grpc.ssl_server_credentials([(key, crt)], root_certificates=ca, require_client_auth=True))
    else:
        srv.add_insecure_port(f"0.0.0.0:{dn.port}")
        srv.add_insecure_port(f"0.0.0.0:{dn.port + 1}")
    srv.start()
    for loop in (dn.heartbeat_loop, dn.scrub_loop, dn.gc_loop):
        threading.Thread(target=loop, daemon=True).start()
    log.info("%s listo en %s (zona %s)", dn.id, dn.addr, dn.zone)
    srv.wait_for_termination()


if __name__ == "__main__":
    main()
