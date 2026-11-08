"""Nodo de metadatos (MDS): REST (FastAPI) + gRPC Meta + monitores del líder + Raft (PySyncObj sobre mTLS)."""
import base64, json, logging, os, random, ssl, threading, time
from collections import deque
from concurrent import futures
import grpc, jwt, uvicorn
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from fastapi import Body, Depends, FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse
from pysyncobj import SyncObj, SyncObjConf
from pysyncobj.syncobj import SyncObjException

from ..common import (BLOCK_SIZE, CIPHER, DFSError, R, SEG, TOKEN_TTL, W, err_from, load_private, new_id, norm_path,
                      read, sign_token, unwrap_dek, wrap_dek)
from ..pb import dfsha_pb2 as pb, dfsha_pb2_grpc as pbg
from .state import FS

log = logging.getLogger("mds")
env = os.getenv
HB_S = float(env("DFSHA_HB_S", 3))
SUSPECT_S = float(env("DFSHA_SUSPECT_S", 10))
DEAD_S = float(env("DFSHA_DEAD_S", 30))
GRACE_S = float(env("DFSHA_GRACE_S", 600))          # periodo de gracia de lecturas/borrado
ORPHAN_S = float(env("DFSHA_ORPHAN_S", 600))
REPORT_S = float(env("DFSHA_REPORT_S", 60))
TXN_TTL = float(env("DFSHA_TXN_TTL", 60))
LOCK_LEASE = float(env("DFSHA_LOCK_LEASE", 30))
JWT_TTL = int(env("DFSHA_JWT_TTL", 900))            # access token: 15 min
REFRESH_TTL = int(env("DFSHA_REFRESH_TTL", 8 * 3600))  # refresh token: 8 h
LOGIN_MAX = int(env("DFSHA_LOGIN_MAX", 5))           # intentos fallidos de login por minuto y usuario
INFLIGHT_S = float(env("DFSHA_INFLIGHT_S", 30))      # tiempo tras el cual una orden de re-replicación se reintenta
MAX_INFLIGHT = int(env("DFSHA_REREPL_MAX", 4))       # límite de reparaciones/movimientos simultáneos
REBALANCE = env("DFSHA_REBALANCE", "1") == "1"
ADMISSION = env("DFSHA_DN_ADMISSION", "auto")       # auto: los DN con certificado válido se registran solos | admin: alta explícita
ALLOW_PLAINTEXT = env("DFSHA_ALLOW_PLAINTEXT", "0") == "1"   # solo para el experimento E10 (cifrado apagado)
BAD_HOLD_S = float(env("DFSHA_BAD_HOLD_S", 600))
MAX_BLOCK = BLOCK_SIZE


def parse_cluster(s):
    out = {}
    for item in s.split(","):
        name, rest = item.split("=")
        host, rp, hp, gp = rest.split(":")
        out[name] = {"host": host, "raft": f"{host}:{rp}", "http": int(hp), "grpc": f"{host}:{gp}"}
    return out


class MDS:
    def __init__(self):
        self.id = env("DFSHA_MDS_ID", "mds-1")
        self.cluster = parse_cluster(env("DFSHA_MDS_CLUSTER", "mds-1=127.0.0.1:7001:8001:9001"))
        data = env("DFSHA_DATA_DIR", f"/tmp/dfsha/{self.id}")
        os.makedirs(data, exist_ok=True)
        keys = env("DFSHA_KEYS", "./keys")
        self.jwt_priv = read(f"{keys}/jwt_ed25519.pem")
        self.jwt_pub = read(f"{keys}/jwt_ed25519.pub")
        self.block_priv = load_private(f"{keys}/block_ed25519.pem")
        self.kek = bytes.fromhex(read(f"{keys}/kek.hex").decode().strip())
        self.tls = env("DFSHA_CERTS")
        self.fs = FS()
        conf_kw = dict(fullDumpFile=f"{data}/raft.dump", journalFile=f"{data}/raft.journal",
                       password=env("DFSHA_RAFT_PASSWORD") or None, dynamicMembershipChange=False,
                       raftMinTimeout=float(env("DFSHA_RAFT_MIN", 0.8)), raftMaxTimeout=float(env("DFSHA_RAFT_MAX", 1.6)),
                       # sin batch: cada comando despierta el bucle de Raft y se replica de inmediato (en batch espera al
                       # siguiente tick y cada operación de metadatos costaba ~100 ms)
                       appendEntriesUseBatch=env("DFSHA_RAFT_BATCH", "0") == "1", appendEntriesPeriod=float(env("DFSHA_RAFT_AE", 0.05)),
                       connectionTimeout=3.5, commandsWaitLeader=True)
        if self.tls:
            from . import raft_tls
            me, others, bind = raft_tls.setup(self.cluster, self.id, self.tls)
            conf_kw["bindAddress"] = bind
        else:
            me = self.cluster[self.id]["raft"]
            others = [c["raft"] for n, c in self.cluster.items() if n != self.id]
        self.raft = SyncObj(me, others, SyncObjConf(**conf_kw), consumers=[self.fs])
        from . import raft_safety
        raft_safety.install(self.raft)                       # nunca truncar entradas ya aplicadas (regla de Raft §5.3)
        self.dns = {}            # dn_id -> {addr, zone, free, used, last}
        self.dn_lock = threading.RLock()
        self.orders = {}         # dn_id -> [Order]
        self.orphans = {}        # (dn, bid) -> first_seen
        self.late = {}           # bid -> {dn}  réplicas anunciadas antes del commit
        self.inflight = {}       # (bid, target) -> ts
        self.moves = {}          # bid -> {src, dst, ts}  rebalanceo / retiro de nodos
        self.bad = {}            # (bid, dn) -> hasta cuándo no se re-admite (copia que el nodo verificó como corrupta)
        self.suspect = {}        # (bid, dn) -> hasta cuándo se ofrece al final (un cliente la reportó; el nodo la está verificando)
        self.purged = set()
        self.was_leader = False
        self.ph = PasswordHasher()
        self.users_seed = env("DFSHA_USERS", "alice:alicepw,bob:bobpw,admin:adminpw:admin")
        self.login_hits = {}     # clave -> deque de instantes de intento
        self.audit_fd = os.open(f"{data}/audit.log", os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)   # solo-anexar
        self.dn_stubs = {}
        self.pool = futures.ThreadPoolExecutor(16)

    # ---- auditoría de solo-anexar (en el líder)
    def audit(self, event, **kw):
        line = json.dumps({"ts": round(time.time(), 3), "mds": self.id, "event": event, **kw}, ensure_ascii=False, default=str) + "\n"
        os.write(self.audit_fd, line.encode())

    # ---- liderazgo
    def is_leader(self):
        lead = self.raft.isReady() and self.raft._isLeader()
        if lead and not getattr(self, "_lead", False):
            self.lead_since = time.time()
        self._lead = lead
        return lead

    def leader_info(self):
        n = self.raft._getLeader()
        if n is None:
            return None
        addr = getattr(n, "id", str(n))
        for name, c in self.cluster.items():
            if addr in (c["raft"], f"{name}:{c['raft'].rsplit(':', 1)[1]}"):
                scheme = "https" if self.tls else "http"
                return {"id": name, "http": f"{scheme}://{c['host']}:{c['http']}", "grpc": c["grpc"]}
        return None

    # ---- registro de nodos de datos (memoria del líder; los heartbeats NO pasan por Raft)
    def dn_state(self, dn, now=None):
        d = self.dns.get(dn)
        if not d:
            return "DEAD"
        age = (now or time.time()) - d["last"]
        return "ALIVE" if age < SUSPECT_S else ("SUSPECT" if age < DEAD_S else "DEAD")

    def admin_state(self, dn):
        st = self.fs.dn_admin.get(dn)
        if st is None:
            return "ACTIVE" if ADMISSION == "auto" else "PENDING"
        return st

    def alive(self, writable=False):
        now = time.time()
        with self.dn_lock:
            out = [dict(v, dn_id=k) for k, v in self.dns.items() if self.dn_state(k, now) == "ALIVE"]
        if writable:
            out = [c for c in out if self.admin_state(c["dn_id"]) == "ACTIVE" and c["free"] > 2 * MAX_BLOCK]
        return out

    def alloc(self, sizes):
        """Colocación: power of two choices por espacio libre; nodos distintos; >= 2 zonas; el primario rota."""
        if time.time() - getattr(self, "lead_since", 0) < 2 * HB_S + 1:       # el registro de nodos se reconstruye con heartbeats
            raise DFSError(503, "UNAVAILABLE", "nuevo líder: reconstruyendo el registro de nodos")
        cands = self.alive(writable=True)
        zones = {c["zone"] for c in cands}
        if len(cands) < W or len(zones) < min(2, W):
            raise DFSError(503, "INSUFFICIENT_REPLICAS", f"nodos vivos={len(cands)}, zonas={len(zones)}")
        out = []
        for _ in sizes:
            pool, chosen = list(cands), []
            while pool and len(chosen) < R:
                a = random.sample(pool, 2) if len(pool) > 1 else pool[:1]
                best = max(a, key=lambda c: c["free"])
                chosen.append(best)
                pool.remove(best)
            if len({c["zone"] for c in chosen}) < 2:
                other = [c for c in cands if c["zone"] != chosen[0]["zone"]]
                if other:
                    chosen[-1] = random.choice(other)
            # el segundo eslabón de la cadena va a otra zona: así primario + segundo cumplen W=2 en >= 2 zonas
            rest = sorted(chosen[1:], key=lambda c: c["zone"] == chosen[0]["zone"])
            out.append([{"dn_id": c["dn_id"], "addr": c["addr"], "zone": c["zone"]} for c in [chosen[0]] + rest])
        return out

    def replicas_for(self, reps, zone=None, bid=None):
        """Réplicas vivas ordenadas: estado, sospecha de corrupción, cercanía (zona del cliente) y espacio libre."""
        now = time.time()
        rank = {"ALIVE": 0, "SUSPECT": 1}
        items = []
        with self.dn_lock:
            for dn in reps:
                st = self.dn_state(dn, now)
                if st == "DEAD":
                    continue
                d = self.dns[dn]
                sus = 1 if self.suspect.get((bid, dn), 0) > now else 0
                items.append((rank[st], sus, 0 if zone and d["zone"] == zone else 1, -d["free"], {"dn_id": dn, "addr": d["addr"], "zone": d["zone"], "state": st}))
        return [i[4] for i in sorted(items, key=lambda x: x[:4])]

    def tok(self, bid, op, uid, txn=""):
        exp = time.time() + TOKEN_TTL
        return sign_token(self.block_priv, bid=bid, op=op, uid=uid, txn=txn, exp=exp), exp

    def queue(self, dn, **order):
        with self.dn_lock:
            self.orders.setdefault(dn, []).append(pb.Order(**order))

    # ---- verificación de réplicas por el líder (no se confía en lo que declara el cliente)
    def dn_stub(self, addr):
        h, p = addr.rsplit(":", 1)
        node = f"{h}:{int(p) + 1}"                       # puerto de nodo (mTLS)
        if node not in self.dn_stubs:
            if self.tls:
                cr = grpc.ssl_channel_credentials(root_certificates=read(f"{self.tls}/ca.pem"), private_key=read(f"{self.tls}/node.key"),
                                                  certificate_chain=read(f"{self.tls}/node.pem"))
                ch = grpc.secure_channel(node, cr)
            else:
                ch = grpc.insecure_channel(node)
            self.dn_stubs[node] = pbg.DataNodeStub(ch)
        return self.dn_stubs[node]

    def verify_blocks(self, allocs, declared):
        """Pregunta a cada nodo asignado si tiene el bloque con el SHA-256 declarado. Devuelve bid -> [dn verificados]."""
        by_dn = {}
        for a in allocs:
            for r in a["reps"]:
                by_dn.setdefault(r["dn_id"], (r["addr"], []))[1].append(a["bid"])

        def ask(dn, addr, bids):
            try:
                rep = self.dn_stub(addr).CheckBlocks(pb.CheckRequest(block_ids=bids), timeout=5)
                return dn, {b.block_id: b.sha256 for b in rep.blocks if b.present}
            except grpc.RpcError as e:
                log.warning("CheckBlocks %s: %s", dn, e.code())
                return dn, {}
        res = list(self.pool.map(lambda kv: ask(kv[0], *kv[1]), by_dn.items()))
        out = {a["bid"]: [] for a in allocs}
        for dn, have in res:
            for bid, sha in have.items():
                if bid in out and declared.get(bid) == sha:
                    out[bid].append(dn)
        return out

    # ---- procesamiento de heartbeats / block reports
    def reconcile(self, dn, ids, full):
        now = time.time()
        snap = self.fs.snapshot_blocks()
        for bid in ids:
            b = snap.get(bid)
            if b:
                mv = self.moves.get(bid)
                if mv and mv["dst"] == dn and dn not in b["reps"]:
                    self.moves.pop(bid, None)
                    r = self.fs.move_replica(bid, mv["src"], dn, timeout=10)            # rebalanceo: dst confirmada -> retirar src
                    if "err" not in r:
                        self.queue(mv["src"], op="delete", block_id=bid)
                        continue
                    b = self.fs.snapshot_blocks().get(bid) or b                         # src ya no figuraba (p. ej. DEAD): trato normal
                if dn not in b["reps"]:
                    if self.bad.get((bid, dn), 0) > now:
                        continue                                                       # réplica reportada corrupta: no se re-admite
                    if len(b["reps"]) < R:
                        self.fs.add_replica(bid, dn, timeout=10)
                    else:
                        self.queue(dn, op="delete", block_id=bid)      # réplica en exceso / versión vieja
            elif self.fs.block_known(bid):
                self.late.setdefault(bid, set()).add(dn)               # bloque staged: se suma tras el commit
            else:
                self.orphans.setdefault((dn, bid), now)
        if full:
            s = set(ids)
            for bid, b in snap.items():
                if dn in b["reps"] and bid not in s and now - b["ct"] > 2 * REPORT_S:
                    log.warning("réplica perdida %s en %s", bid, dn)
                    self.fs.remove_replica(bid, dn, timeout=10)

    # ---- monitores (solo el líder actúa)
    def monitor(self):
        last_maint = 0
        while True:
            time.sleep(1.0)
            try:
                lead = self.is_leader()
                now = time.time()
                if lead and not self.was_leader:
                    log.info("soy líder: extiendo leases y registro usuarios")
                    self.fs.locks_extend(now, LOCK_LEASE, timeout=10)
                    self.audit("leader_elected")
                if not lead:
                    self.was_leader = False
                    continue
                self.was_leader = True
                self.seed_users()
                with self.dn_lock:
                    items = list(self.dns)
                for dn in items:                                       # DEAD => quitar de ubicaciones
                    st = self.dn_state(dn, now)
                    if st == "DEAD" and dn not in self.purged:
                        r = self.fs.remove_node(dn, timeout=10)
                        self.purged.add(dn)
                        log.warning("DN %s DEAD; réplicas retiradas: %s", dn, r)
                    elif st != "DEAD":
                        self.purged.discard(dn)
                self.replication_cycle(now)
                self.rebalance_cycle(now)
                if now - last_maint > 5:
                    last_maint = now
                    r = self.fs.maintenance(now, GRACE_S, timeout=10)
                    for bid, reps in r["deleted"]:
                        for dn in reps:
                            self.queue(dn, op="delete", block_id=bid)
                    self.bad = {k: t for k, t in self.bad.items() if t > now}
                    self.suspect = {k: t for k, t in self.suspect.items() if t > now}
                for (dn, bid), t in list(self.orphans.items()):
                    if now - t > ORPHAN_S:
                        if not self.fs.block_known(bid):
                            self.queue(dn, op="delete", block_id=bid)
                        del self.orphans[(dn, bid)]
            except SyncObjException as e:
                log.warning("monitor: %s", e)
            except Exception:
                log.exception("monitor")

    def seed_users(self):
        if getattr(self, "_seeded", False):
            return
        for spec in self.users_seed.split(","):
            if not spec:
                continue
            p = spec.split(":")
            if p[0] not in self.fs.users:
                self.fs.add_user(p[0], self.ph.hash(p[1]), p[2] if len(p) > 2 else "user", time.time(), timeout=10)
        self._seeded = True

    def replication_cycle(self, now):
        snap = self.fs.snapshot_blocks()
        for bid in list(self.late):
            if bid in snap:
                for dn in self.late.pop(bid):
                    if dn not in snap[bid]["reps"] and len(snap[bid]["reps"]) < R and self.bad.get((bid, dn), 0) <= now:
                        self.fs.add_replica(bid, dn, timeout=10)
            elif not self.fs.block_known(bid):
                self.late.pop(bid)
        # una orden de re-replicación termina cuando el destino ya figura como réplica (o expira)
        self.inflight = {k: t for k, t in self.inflight.items()
                         if now - t < INFLIGHT_S and k[0] in snap and k[1] not in snap[k[0]]["reps"]}
        under = sorted((len(b["reps"]), bid) for bid, b in snap.items() if len(b["reps"]) < R and now - b["ct"] > 3 * HB_S + 2)
        alive = self.alive(writable=True)
        for n, bid in under:                                           # prioridad: bloques con menos réplicas primero
            if len(self.inflight) >= MAX_INFLIGHT:
                break                                                  # limita el ancho de banda de reparación
            b = snap[bid]
            if any(k[0] == bid for k in self.inflight):
                continue
            live = [d for d in b["reps"] if self.dn_state(d, now) == "ALIVE"]
            cands = [c for c in alive if c["dn_id"] not in b["reps"] and self.bad.get((bid, c["dn_id"]), 0) <= now]
            if not live or not cands:
                continue
            have = {self.dns[d]["zone"] for d in b["reps"] if d in self.dns}
            pref = [c for c in cands if c["zone"] not in have] or cands
            tgt = max(pref, key=lambda c: c["free"])
            self.queue(random.choice(live), op="replicate", block_id=bid, target_id=tgt["dn_id"], target_addr=tgt["addr"])
            self.inflight[(bid, tgt["dn_id"])] = now
            log.info("re-replicación %s (%d/%d réplicas) -> %s", bid, n, R, tgt["dn_id"])

    def rebalance_cycle(self, now):
        """Retiro de nodos (DECOMMISSIONING) y rebalanceo gradual hacia nodos nuevos o poco cargados (UC9)."""
        self.moves = {b: m for b, m in self.moves.items() if now - m["ts"] < 3 * INFLIGHT_S}
        if not REBALANCE or self.inflight:
            return
        snap = self.fs.snapshot_blocks()
        if any(len(b["reps"]) < R for b in snap.values()):
            return                                                     # primero la durabilidad, luego el equilibrio
        alive = {c["dn_id"]: c for c in self.alive()}
        active = [d for d in alive if self.admin_state(d) == "ACTIVE" and alive[d]["free"] > 2 * MAX_BLOCK]
        decom = [d for d in alive if self.admin_state(d) == "DECOMMISSIONING"]
        count = {d: 0 for d in alive}
        for b in snap.values():
            for d in b["reps"]:
                if d in count:
                    count[d] += 1
        for d in decom:
            if count[d] == 0 and not any(m["src"] == d for m in self.moves.values()):
                self.fs.dn_set_admin(d, "DECOMMISSIONED", timeout=10)
                self.audit("node_decommissioned", dn=d)
        if not active:
            return

        def zone(d):
            return self.dns[d]["zone"] if d in self.dns else None

        def plan(bid, b, src):
            others = [d for d in b["reps"] if d != src]
            for dst in sorted(active, key=lambda d: count[d]):
                if dst in b["reps"] or bid in self.moves:
                    continue
                if len({zone(d) for d in others + [dst]}) < min(2, len(others) + 1):
                    continue
                if self.bad.get((bid, dst), 0) > now:
                    continue
                origin = src if self.dn_state(src, now) == "ALIVE" else next((d for d in others if self.dn_state(d, now) == "ALIVE"), None)
                if origin is None:
                    return False
                self.queue(origin, op="replicate", block_id=bid, target_id=dst, target_addr=self.dns[dst]["addr"])
                self.moves[bid] = {"src": src, "dst": dst, "ts": now}
                count[dst] += 1
                count[src] -= 1
                log.info("rebalanceo %s: %s -> %s", bid, src, dst)
                return True
            return False
        for d in decom:                                               # vaciar nodos en retiro
            for bid, b in snap.items():
                if len(self.moves) >= MAX_INFLIGHT:
                    return
                if d in b["reps"] and bid not in self.moves:
                    plan(bid, b, d)
        if len(active) < 2 or self.moves:
            return
        avg = sum(count[d] for d in active) / len(active)
        hi, lo = max(active, key=lambda d: count[d]), min(active, key=lambda d: count[d])
        if count[hi] - count[lo] <= max(2, 0.1 * avg):
            return
        for bid, b in snap.items():                                   # gradual: a lo sumo MAX_INFLIGHT movimientos por ciclo
            if len(self.moves) >= MAX_INFLIGHT or count[hi] - count[lo] <= max(2, 0.1 * avg):
                break
            if hi in b["reps"] and lo not in b["reps"] and bid not in self.moves:
                plan(bid, b, hi)


# =================================================================== gRPC Meta
class MetaServicer(pbg.MetaServicer):
    def __init__(self, m): self.m = m

    def _node_only(self, ctx):
        cn = ctx.auth_context().get("x509_common_name", []) if self.m.tls else [b"dn-dev"]
        if not cn or not cn[0].decode().startswith("dn-"):
            ctx.abort(grpc.StatusCode.PERMISSION_DENIED, "se requiere certificado de nodo de datos (mTLS)")
        return cn[0].decode()

    def Heartbeat(self, req, ctx):
        cn = self._node_only(ctx)
        m = self.m
        if self.m.tls and cn != req.dn_id:
            ctx.abort(grpc.StatusCode.PERMISSION_DENIED, "el dn_id no coincide con el certificado")
        if not m.is_leader():
            li = m.leader_info()
            return pb.HeartbeatReply(not_leader=True, leader_hint=li["grpc"] if li else "")
        with m.dn_lock:
            m.dns[req.dn_id] = {"addr": req.addr, "zone": req.zone, "free": req.free_bytes, "used": req.used_bytes, "last": time.time()}
            q = m.orders.get(req.dn_id, [])
            orders, m.orders[req.dn_id] = q[:20], q[20:]              # las órdenes no entregadas quedan para el próximo latido
        for bid in req.bad_blocks:                                      # copia verificada como corrupta por el propio nodo
            m.bad[(bid, req.dn_id)] = time.time() + BAD_HOLD_S           # no se re-admite durante BAD_HOLD_S
            m.suspect.pop((bid, req.dn_id), None)
            m.fs.remove_replica(bid, req.dn_id, timeout=10)
            m.audit("replica_corrupt", dn=req.dn_id, block=bid)
        if req.new_blocks:
            m.reconcile(req.dn_id, list(req.new_blocks), False)
        return pb.HeartbeatReply(orders=orders)

    def BlockReport(self, it, ctx):
        cn = self._node_only(ctx)
        m = self.m
        ids, dn = [], None
        for batch in it:
            dn = batch.dn_id
            ids += list(batch.block_ids)
        if self.m.tls and cn != dn:
            ctx.abort(grpc.StatusCode.PERMISSION_DENIED, "el dn_id no coincide con el certificado")
        if not m.is_leader():
            li = m.leader_info()
            return pb.BlockReportReply(not_leader=True, leader_hint=li["grpc"] if li else "")
        m.reconcile(dn, ids, True)
        return pb.BlockReportReply(known=len(ids))


def start_grpc(m):
    srv = grpc.server(futures.ThreadPoolExecutor(16))
    pbg.add_MetaServicer_to_server(MetaServicer(m), srv)
    addr = "0.0.0.0:" + m.cluster[m.id]["grpc"].split(":")[1]
    if m.tls:
        creds = grpc.ssl_server_credentials([(read(f"{m.tls}/node.key"), read(f"{m.tls}/node.pem"))],
                                            root_certificates=read(f"{m.tls}/ca.pem"), require_client_auth=True)
        srv.add_secure_port(addr, creds)
    else:
        srv.add_insecure_port(addr)
    srv.start()
    return srv


# =================================================================== REST
def create_app(m: MDS) -> FastAPI:
    app = FastAPI(title="DFSha metadata service")
    ph = m.ph

    @app.exception_handler(DFSError)
    async def _h(request: Request, e: DFSError):
        headers = {}
        if e.name == "NOT_LEADER" and e.extra.get("leader"):
            loc = e.extra["leader"] + request.url.path + (("?" + request.url.query) if request.url.query else "")
            headers.update({"Location": loc, "X-Leader": e.extra["leader"]})
        if e.code == 429:
            headers["Retry-After"] = "60"
        return JSONResponse({"error": e.name, "code": e.code, "message": e.msg, **e.extra}, status_code=e.code, headers=headers)

    @app.exception_handler(SyncObjException)
    async def _s(_, e):
        return JSONResponse({"error": "UNAVAILABLE", "code": 503, "message": "sin quórum Raft"}, status_code=503)

    def leader():
        if not m.is_leader():
            li = m.leader_info()
            if li:
                raise DFSError(307, "NOT_LEADER", "este nodo no es líder", leader=li["http"])   # 307 conserva método y cuerpo
            raise DFSError(503, "UNAVAILABLE", "sin líder: elección en curso")

    def auth(request: Request, authorization: str = Header(None)):
        leader()
        if not authorization or not authorization.startswith("Bearer "):
            raise DFSError(401, "UNAUTHENTICATED", "falta JWT")
        try:
            c = jwt.decode(authorization[7:], m.jwt_pub, algorithms=["EdDSA"])
        except Exception:
            raise DFSError(401, "UNAUTHENTICATED", "JWT inválido o expirado")
        if c.get("typ") != "access":
            raise DFSError(401, "UNAUTHENTICATED", "se esperaba un access token")
        return c

    def own(c, path, allow_home=True):
        path = norm_path(path)
        home = "/users/" + c["sub"]
        if c.get("role") == "admin" or not (path == home or path.startswith(home + "/")):
            raise DFSError(403, "FORBIDDEN", "ruta fuera de su espacio")
        if not allow_home and path == home:
            raise DFSError(403, "FORBIDDEN", "no se puede borrar ni mover el directorio raíz del usuario")
        return path

    def admin(c):
        if c.get("role") != "admin":
            raise DFSError(403, "FORBIDDEN", "se requiere rol admin")

    def holder(c, cid):
        return f'{c["sub"]}:{cid or ""}'

    def tokens(sub, role):
        now = int(time.time())
        acc = jwt.encode({"sub": sub, "role": role, "typ": "access", "iat": now, "exp": now + JWT_TTL, "jti": new_id()}, m.jwt_priv, algorithm="EdDSA")
        ref = jwt.encode({"sub": sub, "role": role, "typ": "refresh", "iat": now, "exp": now + REFRESH_TTL, "jti": new_id()}, m.jwt_priv, algorithm="EdDSA")
        return {"access_token": acc, "refresh_token": ref, "expires_in": JWT_TTL, "refresh_expires_in": REFRESH_TTL}

    def blocked(user, ip):
        """Límite de intentos de login: LOGIN_MAX fallidos por minuto por usuario (y 4x por IP). Bloqueado => 429
        aunque la contraseña sea correcta, para que la fuerza bruta no pueda seguir probando."""
        now = time.time()
        for k, lim in (("u:" + user, LOGIN_MAX), ("ip:" + ip, 4 * LOGIN_MAX)):
            q = m.login_hits.setdefault(k, deque())
            while q and q[0] < now - 60:
                q.popleft()
            if len(q) >= lim:
                raise DFSError(429, "TOO_MANY_REQUESTS", f"demasiados intentos fallidos de login ({lim}/min)")

    def failed(user, ip):
        now = time.time()
        for k in ("u:" + user, "ip:" + ip):
            m.login_hits.setdefault(k, deque()).append(now)

    # ---- público
    @app.get("/healthz")
    def healthz():
        return {"id": m.id, "leader": m.is_leader(), "ready": m.raft.isReady()}

    @app.post("/v1/auth/login")
    def login(request: Request, body: dict):
        leader()
        user = str(body.get("username", ""))
        ip = request.client.host if request.client else "?"
        blocked(user, ip)
        u = m.fs.users.get(user)
        try:
            if not u:
                raise VerifyMismatchError()
            ph.verify(u["pw"], body.get("password", ""))
        except VerifyMismatchError:
            failed(user, ip)
            m.audit("login_failed", user=user, ip=ip)
            raise DFSError(401, "UNAUTHENTICATED", "credenciales inválidas")
        m.audit("login", user=user)
        return tokens(user, u["role"])

    @app.post("/v1/auth/refresh")
    def refresh(body: dict):
        leader()
        try:
            c = jwt.decode(body.get("refresh_token", ""), m.jwt_pub, algorithms=["EdDSA"])
        except Exception:
            raise DFSError(401, "UNAUTHENTICATED", "refresh token inválido o expirado")
        if c.get("typ") != "refresh" or c["sub"] not in m.fs.users:
            raise DFSError(401, "UNAUTHENTICATED", "refresh token inválido")
        r = m.fs.use_jti(c["jti"], c["exp"], time.time(), timeout=10)
        if "err" in r:
            m.audit("refresh_reuse", user=c["sub"], jti=c["jti"])
        err_from(r)
        return tokens(c["sub"], m.fs.users[c["sub"]]["role"])

    @app.post("/v1/auth/logout")
    def logout(body: dict):
        leader()
        try:
            c = jwt.decode(body.get("refresh_token", ""), m.jwt_pub, algorithms=["EdDSA"])
            m.fs.use_jti(c["jti"], c["exp"], time.time(), timeout=10)        # el refresh token queda inutilizable
        except Exception:
            pass
        return {"ok": True}

    # ---- RF1
    @app.get("/v1/fs/{path:path}")
    def fs_get(path: str, op: str = "ls", c=Depends(auth)):
        p = own(c, path)
        m.fs.barrier(timeout=10)                                       # lectura linealizable: pasa por el log Raft
        return err_from(m.fs.stat(p) if op == "stat" else m.fs.ls(p))

    @app.post("/v1/fs/{path:path}")
    def fs_post(path: str, body: dict = Body(default=None), op: str = Query(...), c=Depends(auth)):
        body = body or {}
        now = time.time()
        if op == "mkdir":
            p = own(c, path)
            r = err_from(m.fs.mkdir(p, c["sub"], now, bool(body.get("parents")), timeout=10))
            m.audit("mkdir", user=c["sub"], path=p)
            return r
        if op == "mv":
            p, d = own(c, path, allow_home=False), own(c, body["dest"], allow_home=False)
            r = err_from(m.fs.mv(p, d, holder(c, body.get("cid")), now, timeout=10))
            m.audit("mv", user=c["sub"], path=p, dest=d)
            return r
        raise DFSError(400, "INVALID_ARGUMENT", "op")

    @app.delete("/v1/fs/{path:path}")
    def fs_del(path: str, recursive: bool = False, cid: str = "", c=Depends(auth)):
        p = own(c, path, allow_home=False)
        r = err_from(m.fs.rm(p, recursive, holder(c, cid), time.time(), timeout=10))
        m.audit("rm", user=c["sub"], path=p, recursive=recursive)
        return r

    # ---- transacciones de escritura (RF2/RF3)
    def block_view(a, uid, tid):
        t, exp = m.tok(a["bid"], "put", uid, tid)
        return {"block_id": a["bid"], "idx": a["idx"], "replicas": a["reps"], "token": t, "token_exp": exp}

    @app.post("/v1/txn")
    def txn_open(body: dict, c=Depends(auth)):
        p, op = own(c, body["path"]), body.get("op", "w")
        if op not in ("x", "w", "a", "r+", "c"):
            raise DFSError(400, "INVALID_ARGUMENT", "op")
        cipher = body.get("cipher", CIPHER)
        if cipher not in (CIPHER, "none") or (cipher == "none" and not ALLOW_PLAINTEXT):
            raise DFSError(400, "INVALID_ARGUMENT", "cifrado no permitido")
        sizes = [int(s) for s in body.get("sizes", [])]
        if any(s <= 0 or s > MAX_BLOCK for s in sizes):
            raise DFSError(400, "INVALID_ARGUMENT", f"tamaño de bloque fuera de (0, {MAX_BLOCK}]")
        if op in ("w", "x"):
            idx = list(range(len(sizes)))
        elif op == "a":
            m.fs.barrier(timeout=10)
            st = err_from(m.fs.stat(p))
            idx = list(range(st["nblocks"], st["nblocks"] + len(sizes)))
        elif op == "c":
            i = int(body["from_idx"])
            idx = list(range(i, i + len(sizes)))
        else:
            idx = [int(i) for i in body["idx"]]
            if len(idx) != len(sizes) or len(set(idx)) != len(idx):
                raise DFSError(400, "INVALID_ARGUMENT", "idx y sizes no coinciden")
        reps = m.alloc(sizes) if sizes else []
        allocs = [{"bid": new_id("b-"), "idx": i, "reps": r} for i, r in zip(idx, reps)]
        tid = new_id("tx-")
        res = err_from(m.fs.txn_begin(tid, c["sub"], holder(c, body.get("cid")), p, op, int(body.get("expected_version", 0)), new_id("f-"), idx, allocs,
                                      time.time(), TXN_TTL, [int(e) for e in body.get("lock_epochs", [])], cipher, timeout=10))
        return {"txn_id": tid, "fid": res["fid"], "version": res["version"], "ttl": TXN_TTL, "state": "OPEN", "r": R, "w": W,
                "blocks": [block_view(a, c["sub"], tid) for a in allocs]}

    @app.get("/v1/txn/{tid}")
    def txn_status(tid: str, c=Depends(auth)):
        t = err_from(m.fs.txn_get(tid, c["sub"]))
        return {"txn_id": tid, "state": t["state"], "path": t["path"], "op": t["op"], "expires_in": round(t["exp"] - time.time(), 1)}

    @app.post("/v1/txn/{tid}/tokens")
    def txn_tokens(tid: str, body: dict = Body(default=None), c=Depends(auth)):
        """Renueva los block tokens de escritura (vigencia corta) de una transacción abierta: subidas largas no caducan."""
        t = err_from(m.fs.txn_get(tid, c["sub"]))
        if t["state"] != "OPEN" or t["exp"] < time.time():
            raise DFSError(410, "LEASE_EXPIRED", "transacción no abierta")
        want = set((body or {}).get("block_ids") or [a["bid"] for a in t["alloc"]])
        return {"blocks": [block_view(a, c["sub"], tid) for a in t["alloc"] if a["bid"] in want]}

    @app.put("/v1/txn/{tid}/renew")
    def txn_renew(tid: str, c=Depends(auth)):
        return err_from(m.fs.txn_renew(tid, c["sub"], time.time(), TXN_TTL, timeout=10))

    @app.post("/v1/txn/{tid}/abort")
    def txn_abort(tid: str, c=Depends(auth)):
        r = err_from(m.fs.txn_abort(tid, c["sub"], time.time(), timeout=10))
        m.audit("abort", user=c["sub"], txn=tid)
        return r

    @app.post("/v1/txn/{tid}/commit")
    def txn_commit(tid: str, body: dict, c=Depends(auth)):
        t = err_from(m.fs.txn_get(tid, c["sub"]))
        if t["state"] == "COMMITTED":
            return t["result"]                                             # idempotente por txn_id
        body = dict(body)
        if t["state"] == "OPEN":
            declared = {b["bid"]: b["sha"] for b in body.get("blocks", [])}
            idx = {a["bid"]: a["idx"] for a in t["alloc"]}
            if set(declared) != set(idx):
                raise DFSError(400, "INVALID_ARGUMENT", "los bloques no coinciden con la asignación")
            ver = m.verify_blocks(t["alloc"], declared)                        # el líder comprueba cada réplica (hash) en su nodo
            r = m.fs.txn_prepare(tid, c["sub"], [{"bid": b, "idx": idx[b], "acked": ver[b]} for b in declared], time.time(), timeout=10)
            if "err" in r:
                m.audit("commit_failed", user=c["sub"], txn=tid, path=t["path"], error=r["err"][1])
            err_from(r)
        if body.get("dek"):                      # la DEK llega por TLS y se guarda envuelta con la KEK (AES-KW)
            body["dek"] = wrap_dek(m.kek, base64.b64decode(body["dek"]))
        r = m.fs.txn_commit(tid, c["sub"], body, time.time(), timeout=10)
        m.audit("commit" if "err" not in r else "commit_failed", user=c["sub"], txn=tid, path=t["path"], op=t["op"],
                version=r.get("version"), error=r["err"][1] if "err" in r else None)
        return err_from(r)

    # ---- lectura: manifiesto + ubicaciones + tokens
    @app.get("/v1/files/{path:path}")
    def files_get(path: str, version: int = 0, zone: str = None, c=Depends(auth)):
        p = own(c, path)
        m.fs.barrier(timeout=10)   # (G2) lectura linealizable: cualquier open() posterior a un 200 debe ver esa versión o una más nueva
        r = err_from(m.fs.open_file(new_id("pin-"), c["sub"], p, version, time.time(), GRACE_S, timeout=10))
        m.audit("open", user=c["sub"], path=p, version=r["version"])
        blocks = []
        for b in r["blocks"]:
            t, exp = m.tok(b["bid"], "get", c["sub"])
            blocks.append({"idx": b["idx"], "block_id": b["bid"], "size": b["size"], "sha256": b["sha"], "psha256": b["psha"], "nonce": b["nonce"],
                           "replicas": m.replicas_for(b["reps"], zone, b["bid"]), "token": t, "token_exp": exp})
        dek = r["dek"]
        return {"version": r["version"], "current": r["current"], "size": r["size"], "csha256": r["csha"], "file_id": r["fid"],
                "cipher": r["cipher"], "seg": SEG, "dek": base64.b64encode(unwrap_dek(m.kek, dek)).decode() if dek else None,
                "pin_ttl": GRACE_S, "blocks": blocks}

    @app.get("/v1/blocks/{bid}/locations")
    def block_locations(bid: str, zone: str = None, c=Depends(auth)):
        snap = m.fs.snapshot_blocks().get(bid)
        if not snap or m.fs.block_owner(bid) != c["sub"]:
            raise DFSError(404, "NOT_FOUND", bid)
        t, exp = m.tok(bid, "get", c["sub"])
        return {"block_id": bid, "replicas": m.replicas_for(snap["reps"], zone, bid), "token": t, "token_exp": exp}

    @app.post("/v1/blocks/{bid}/bad")
    def block_bad(bid: str, body: dict, c=Depends(auth)):
        if m.fs.block_owner(bid) != c["sub"]:
            raise DFSError(404, "NOT_FOUND", bid)
        dn = body["dn_id"]
        # el reporte de un cliente no basta para retirar una réplica (un cliente con errores dejaría el archivo sin copias):
        # se ofrece al final mientras el nodo recalcula el hash; si está corrupta, el nodo la aparta y lo avisa por heartbeat
        m.suspect[(bid, dn)] = time.time() + 60
        m.queue(dn, op="verify", block_id=bid)
        m.audit("bad_replica", user=c["sub"], block=bid, dn=dn)
        return {"ok": True}

    # ---- locks (lease + fencing; S/X; archivo o bloque)
    @app.post("/v1/locks")
    def lock_post(body: dict, c=Depends(auth)):
        p = own(c, body["path"])
        mode = body.get("mode", "X")
        blk = body.get("block")
        if mode not in ("S", "X") or (blk is not None and int(blk) < 0):
            raise DFSError(400, "INVALID_ARGUMENT", "mode S|X, block >= 0")
        r = m.fs.lock_acquire(p, None if blk is None else int(blk), mode, holder(c, body.get("cid")), time.time(), LOCK_LEASE, timeout=10)
        m.audit("lock" if "err" not in r else "lock_denied", user=c["sub"], path=p, mode=mode, block=blk, epoch=r.get("epoch"))
        return err_from(r)

    @app.get("/v1/locks")
    def lock_list(path: str, c=Depends(auth)):
        return {"locks": m.fs.lock_list(own(c, path), time.time())}

    @app.put("/v1/locks/{epoch}")
    def lock_put(epoch: int, body: dict = Body(default=None), c=Depends(auth)):
        return err_from(m.fs.lock_renew(epoch, holder(c, (body or {}).get("cid")), time.time(), LOCK_LEASE, timeout=10))

    @app.delete("/v1/locks/{epoch}")
    def lock_del(epoch: int, cid: str = "", c=Depends(auth)):
        r = err_from(m.fs.lock_release(epoch, holder(c, cid), timeout=10))
        m.audit("unlock", user=c["sub"], epoch=epoch)
        return r

    # ---- administración
    @app.get("/v1/admin/nodes")
    def admin_nodes(c=Depends(auth)):
        admin(c)
        now = time.time()
        count = {}
        for b in m.fs.snapshot_blocks().values():
            for d in b["reps"]:
                count[d] = count.get(d, 0) + 1
        with m.dn_lock:
            known = set(m.dns) | set(m.fs.dn_admin)
            nodes = [{"dn_id": k, "addr": m.dns.get(k, {}).get("addr"), "zone": m.dns.get(k, {}).get("zone"), "free": m.dns.get(k, {}).get("free"),
                      "state": m.dn_state(k, now), "admin": m.admin_state(k), "blocks": count.get(k, 0),
                      "age_s": round(now - m.dns[k]["last"], 1) if k in m.dns else None} for k in known]
        return {"leader": m.id, "mds": list(m.cluster), "admission": ADMISSION, "moves_in_progress": len(m.moves), "nodes": sorted(nodes, key=lambda n: n["dn_id"])}

    @app.post("/v1/admin/nodes")
    def admin_add_node(body: dict, c=Depends(auth)):
        """Alta de un nodo de datos: queda ACTIVE y el rebalanceo gradual le traslada bloques."""
        admin(c)
        dn = body["dn_id"]
        m.fs.dn_set_admin(dn, "ACTIVE", timeout=10)
        m.audit("node_added", user=c["sub"], dn=dn)
        return {"dn_id": dn, "admin": "ACTIVE"}

    @app.delete("/v1/admin/nodes/{dn}")
    def admin_retire_node(dn: str, c=Depends(auth)):
        """Retiro de un nodo: deja de recibir escrituras y sus bloques se trasladan a otros nodos antes de liberarlo."""
        admin(c)
        m.fs.dn_set_admin(dn, "DECOMMISSIONING", timeout=10)
        m.audit("node_retiring", user=c["sub"], dn=dn)
        return {"dn_id": dn, "admin": "DECOMMISSIONING"}

    @app.get("/v1/admin/blocks")
    def admin_blocks(path: str, c=Depends(auth)):
        admin(c)
        bl = m.fs.file_blocks(norm_path(path))
        if bl is None:
            raise DFSError(404, "NOT_FOUND", path)
        out = []
        for b in bl:
            reps = [{"dn_id": d, "zone": m.dns.get(d, {}).get("zone"), "state": m.dn_state(d)} for d in b["reps"]]
            out.append({"idx": b["idx"], "block_id": b["bid"], "size": b["size"], "n_replicas": len(reps),
                        "status": "COMMITTED" if len(reps) >= R else "UNDER_REPLICATED", "replicas": reps})
        return {"path": path, "blocks": out}

    return app


def main():
    logging.basicConfig(level=env("LOG", "INFO"), format="%(asctime)s %(name)s %(message)s")
    m = MDS()
    m.grpc_server = start_grpc(m)   # mantener referencia: si no, el recolector detiene el servidor
    threading.Thread(target=m.monitor, daemon=True).start()
    app = create_app(m)
    cfg = uvicorn.Config(app, host="0.0.0.0", port=m.cluster[m.id]["http"], log_level="warning",
                         **(dict(ssl_keyfile=f"{m.tls}/node.key", ssl_certfile=f"{m.tls}/node.pem") if m.tls else {}))
    cfg.load()
    if cfg.ssl is not None:
        cfg.ssl.minimum_version = ssl.TLSVersion.TLSv1_3          # el REST de metadatos solo acepta TLS 1.3
    uvicorn.Server(cfg).run()


if __name__ == "__main__":
    main()
