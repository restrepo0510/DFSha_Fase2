"""Máquina de estados replicada con Raft (PySyncObj).
Regla: los métodos @replicated son DETERMINISTAS: nunca llaman a time/uuid/random;
el líder pasa 'now', ids y asignaciones como argumentos. Devuelven dicts; errores como {'err': [...]}.
"""
import threading
from pysyncobj import SyncObjConsumer, replicated_sync
from ..common import R, W, parent

OK = {"ok": True}


def E(code, name, msg="", **extra):
    return {"err": [code, name, msg, extra]}


def under(path, root):
    return path == root or path.startswith(root.rstrip("/") + "/")


class FS(SyncObjConsumer):
    def __init__(self):
        self.lock = threading.RLock()   # antes de super().__init__ => no se serializa
        super().__init__()
        self.users = {}      # name -> {pw, role}
        self.nodes = {"/": {"t": "d"}, "/users": {"t": "d"}}   # path -> {t, fid?, uid?}
        self.files = {}      # fid -> {uid, path, cur, dek, cipher, mans:{ver:{...}}, deleted_at}
        self.blocks = {}     # bid -> {reps:[dn], size, sha, fid, ct}
        self.txns = {}       # tid -> {..., state: OPEN|PREPARED|COMMITTED|ABORTED}
        self.staged = {}     # bid -> tid
        self.locks = {}      # epoch -> {path, blk (None = archivo), mode S|X, holder, exp}
        self.pins = {}       # pid -> {fid, ver, exp}
        self.lock_epoch = 0
        self.used_jti = {}   # jti de refresh tokens ya usados -> exp (contra reutilización)
        self.dn_admin = {}   # dn_id -> ACTIVE | DECOMMISSIONING (alta/baja administrativa)

    # ------------------------------------------------------------------ barrera de lectura linealizable
    @replicated_sync
    def barrier(self):
        """Comando vacío: cuando se aplica, el líder sabe que su estado incluye todo lo confirmado antes (ls/stat linealizables)."""
        return OK

    # ------------------------------------------------------------------ usuarios y tokens
    @replicated_sync
    def add_user(self, name, pw, role, now):
        with self.lock:
            if name in self.users:
                return OK
            self.users[name] = {"pw": pw, "role": role}
            if role != "admin":
                self.nodes["/users/" + name] = {"t": "d", "uid": name, "mtime": now}
            return OK

    @replicated_sync
    def use_jti(self, jti, exp, now):
        """Un refresh token sólo puede usarse una vez (rotación); reutilizarlo es señal de robo."""
        with self.lock:
            for k in [k for k, e in self.used_jti.items() if e < now]:
                del self.used_jti[k]
            if jti in self.used_jti:
                return E(401, "UNAUTHENTICATED", "refresh token reutilizado")
            self.used_jti[jti] = exp
            return OK

    # ------------------------------------------------------------------ RF1
    @replicated_sync
    def mkdir(self, path, uid, now, parents):
        with self.lock:
            if path in self.nodes:
                if parents and self.nodes[path]["t"] == "d":
                    return OK                                      # mkdir -p sobre un directorio existente no es error
                return E(409, "EXISTS", path)
            p = parent(path)
            if p not in self.nodes and not parents:
                return E(404, "NOT_FOUND", p)
            cur, todo = "", []
            for part in path.strip("/").split("/"):
                cur += "/" + part
                n = self.nodes.get(cur)
                if n is None:
                    todo.append(cur)
                elif n["t"] != "d":
                    return E(400, "INVALID_PATH", f"{cur} no es directorio")
            for d in todo:
                self.nodes[d] = {"t": "d", "uid": uid, "mtime": now}
            return OK

    def ls(self, path):
        with self.lock:
            n = self.nodes.get(path)
            if n is None:
                return E(404, "NOT_FOUND", path)
            if n["t"] != "d":
                return {"entries": [self._stat(path, n)]}
            pre = path.rstrip("/") + "/"
            out = [self._stat(p, v) for p, v in self.nodes.items()
                   if p != path and p.startswith(pre) and "/" not in p[len(pre):]]
            return {"entries": sorted(out, key=lambda e: e["name"])}

    def _stat(self, path, n):
        e = {"name": path.rsplit("/", 1)[-1] or "/", "path": path, "type": n["t"], "mtime": n.get("mtime")}
        if n["t"] == "f":
            f = self.files[n["fid"]]
            m = f["mans"][f["cur"]]
            e.update(size=m["size"], version=f["cur"], nblocks=len(m["blocks"]), fid=n["fid"])
        return e

    def stat(self, path):
        with self.lock:
            n = self.nodes.get(path)
            return E(404, "NOT_FOUND", path) if n is None else self._stat(path, n)

    def _locked_by_other(self, root, holder, now):
        for l in self.locks.values():
            if l["exp"] > now and l["holder"] != holder and under(l["path"], root):
                return l
        return None

    @replicated_sync
    def rm(self, path, recursive, holder, now):
        with self.lock:
            n = self.nodes.get(path)
            if n is None:
                return E(404, "NOT_FOUND", path)
            l = self._locked_by_other(path, holder, now)
            if l:
                return E(423, "LOCKED", l["path"], holder=l["holder"])
            if n["t"] == "f":
                self._tomb(path, n, now)
                return {"removed": 1}
            pre = path.rstrip("/") + "/"
            kids = [p for p in self.nodes if p.startswith(pre)]
            if kids and not recursive:
                return E(409, "NOT_EMPTY", path)
            for p in kids:
                if self.nodes[p]["t"] == "f":
                    self._tomb(p, self.nodes[p], now)
                else:
                    del self.nodes[p]
            del self.nodes[path]
            return {"removed": len(kids) + 1}

    def _tomb(self, path, n, now):
        """Lápida atómica: la ruta desaparece; los bloques se liberan por GC tras la gracia."""
        self.files[n["fid"]]["deleted_at"] = now
        del self.nodes[path]
        for k in [k for k, l in self.locks.items() if l["path"] == path]:
            del self.locks[k]

    @replicated_sync
    def mv(self, src, dst, holder, now):
        with self.lock:
            if src not in self.nodes:
                return E(404, "NOT_FOUND", src)
            if dst in self.nodes:
                return E(409, "EXISTS", dst)
            if parent(dst) not in self.nodes or self.nodes[parent(dst)]["t"] != "d":
                return E(404, "NOT_FOUND", parent(dst))
            if under(dst, src):
                return E(400, "INVALID_PATH", "mover dentro de sí mismo")
            l = self._locked_by_other(src, holder, now)
            if l:
                return E(423, "LOCKED", l["path"], holder=l["holder"])
            moves = [src] + [p for p in self.nodes if p.startswith(src.rstrip("/") + "/")]
            for p in moves:
                np_ = dst + p[len(src):]
                self.nodes[np_] = self.nodes.pop(p)
                if self.nodes[np_]["t"] == "f":
                    self.files[self.nodes[np_]["fid"]]["path"] = np_
            for l in self.locks.values():                         # los locks propios viajan con el archivo
                if under(l["path"], src):
                    l["path"] = dst + l["path"][len(src):]
            return OK

    # ------------------------------------------------------------------ locks: lease + fencing; S/X; archivo o bloque
    @staticmethod
    def _overlap(l, path, blk):
        return l["path"] == path and (l["blk"] is None or blk is None or l["blk"] == blk)

    @replicated_sync
    def lock_acquire(self, path, blk, mode, holder, now, lease):
        with self.lock:
            n = self.nodes.get(path)
            if n is None or n["t"] != "f":
                return E(404, "NOT_FOUND", path)
            for e, l in self.locks.items():
                if l["exp"] <= now or not self._overlap(l, path, blk):
                    continue
                if l["holder"] == holder and l["blk"] == blk and l["mode"] == mode:
                    l["exp"] = now + lease                              # mismo titular y recurso: renueva sin cambiar época
                    return {"epoch": e, "lease": lease, "mode": mode, "block": blk}
                if l["holder"] != holder and not (l["mode"] == "S" and mode == "S"):
                    return E(423, "LOCKED", path, holder=l["holder"], mode=l["mode"], block=l["blk"])
            self.lock_epoch += 1
            self.locks[self.lock_epoch] = {"path": path, "blk": blk, "mode": mode, "holder": holder, "exp": now + lease}
            for k in [k for k, l in self.locks.items() if l["exp"] <= now]:
                del self.locks[k]
            return {"epoch": self.lock_epoch, "lease": lease, "mode": mode, "block": blk}

    @replicated_sync
    def lock_renew(self, epoch, holder, now, lease):
        with self.lock:
            l = self.locks.get(epoch)
            if not l or l["exp"] <= now or l["holder"] != holder:
                return E(410, "LEASE_EXPIRED", f"lock {epoch}")
            l["exp"] = now + lease
            return {"epoch": epoch, "lease": lease}

    @replicated_sync
    def lock_release(self, epoch, holder):
        with self.lock:
            l = self.locks.get(epoch)
            if l and l["holder"] == holder:
                del self.locks[epoch]
            return OK

    @replicated_sync
    def locks_extend(self, now, lease):
        """Tras un cambio de líder: reinicia leases vigentes (no depende de relojes sincronizados)."""
        with self.lock:
            for l in self.locks.values():
                l["exp"] = now + lease
            return OK

    def _check_write(self, path, blks, holder, epochs, now):
        """Fencing: cada época presentada debe seguir vigente y ser del titular. Exclusión: ningún lock ajeno (S o X)
        puede solaparse con lo que se escribe (blks = None => archivo completo)."""
        for e in epochs or []:
            l = self.locks.get(e)
            if not l or l["exp"] <= now or l["holder"] != holder or l["path"] != path:
                return E(409, "STALE_FENCING", f"época {e} obsoleta o expirada")
        for l in self.locks.values():
            if l["exp"] <= now or l["holder"] == holder or l["path"] != path:
                continue
            if blks is None or l["blk"] is None or l["blk"] in blks:
                return E(423, "LOCKED", path, holder=l["holder"], mode=l["mode"], block=l["blk"])
        return None

    def lock_list(self, path, now):
        with self.lock:
            return [dict(l, epoch=e) for e, l in self.locks.items() if l["path"] == path and l["exp"] > now]

    # ------------------------------------------------------------------ transacciones de escritura
    @staticmethod
    def _write_blks(op, idx):
        # w/x reemplazan todo y c reescribe la cola: chocan con cualquier lock ajeno del archivo; a/r+ solo con los de sus bloques
        return None if op in ("w", "x", "c") else set(idx)

    @replicated_sync
    def txn_begin(self, tid, uid, holder, path, op, expected, fid_new, idx, allocs, now, ttl, epochs, cipher):
        with self.lock:
            n = self.nodes.get(path)
            if parent(path) not in self.nodes or self.nodes[parent(path)]["t"] != "d":
                return E(404, "NOT_FOUND", parent(path))
            if n is not None and n["t"] != "f":
                return E(400, "INVALID_PATH", "es un directorio")
            if op == "x" and n is not None:
                return E(409, "EXISTS", path)
            if op in ("a", "r+", "c") and n is None:
                return E(404, "NOT_FOUND", path)
            e = self._check_write(path, self._write_blks(op, idx), holder, epochs, now)
            if e:
                return e
            cur = self.files[n["fid"]]["cur"] if n else 0
            if expected != cur and not (op == "r+" and expected < cur):
                # r+ puede llegar con versión base vieja: el commit intenta el rebase (bloques disjuntos)
                return E(409, "VERSION_CONFLICT", "versión cambiada", current=cur)
            fid = n["fid"] if n else fid_new
            if n and op in ("a", "r+", "c") and self.files[fid].get("cipher") != cipher:
                return E(400, "INVALID_ARGUMENT", "cifrado distinto al del archivo")
            self.txns[tid] = {"uid": uid, "path": path, "op": op, "expected": expected, "fid": fid, "idx": idx,
                              "alloc": allocs, "state": "OPEN", "exp": now + ttl, "ended": None, "epochs": epochs,
                              "holder": holder, "cipher": cipher, "prepared": None}
            for a in allocs:
                self.staged[a["bid"]] = tid
            return {"fid": fid, "version": cur}

    def txn_get(self, tid, uid):
        with self.lock:
            t = self.txns.get(tid)
            if not t or t["uid"] != uid:
                return E(404, "NOT_FOUND", "transacción")
            return t

    @replicated_sync
    def txn_renew(self, tid, uid, now, ttl):
        with self.lock:
            t = self.txns.get(tid)
            if not t or t["uid"] != uid or t["state"] not in ("OPEN", "PREPARED") or t["exp"] < now:
                return E(410, "LEASE_EXPIRED", "transacción vencida")
            t["exp"] = now + ttl
            return OK

    def _end_txn(self, t, state, now):
        t["state"], t["ended"] = state, now
        for a in t["alloc"]:
            self.staged.pop(a["bid"], None)

    @replicated_sync
    def txn_abort(self, tid, uid, now):
        with self.lock:
            t = self.txns.get(tid)
            if t and t["uid"] == uid and t["state"] in ("OPEN", "PREPARED"):
                self._end_txn(t, "ABORTED", now)
            return OK

    @replicated_sync
    def txn_prepare(self, tid, uid, blocks, now):
        """OPEN -> PREPARED: todos los bloques tienen >= W réplicas VERIFICADAS por el líder (hash consultado a cada nodo),
        de nodos asignados y en >= 2 zonas (garantía G1). Si no, ABORTED y 503."""
        with self.lock:
            t = self.txns.get(tid)
            if not t or t["uid"] != uid:
                return E(404, "NOT_FOUND", "transacción")
            if t["state"] in ("PREPARED", "COMMITTED"):
                return {"state": t["state"]}
            if t["state"] != "OPEN" or t["exp"] < now:
                if t["state"] == "OPEN":
                    self._end_txn(t, "ABORTED", now)
                return E(410, "LEASE_EXPIRED", "transacción vencida")
            alloc = {a["bid"]: a for a in t["alloc"]}
            if len(blocks) != len(alloc) or {b["bid"] for b in blocks} != set(alloc):
                return E(400, "INVALID_ARGUMENT", "los bloques no coinciden con la asignación")
            for b in blocks:
                a = alloc[b["bid"]]
                if b["idx"] != a["idx"]:
                    return E(400, "INVALID_ARGUMENT", "índice de bloque")
                reps = {r["dn_id"]: r["zone"] for r in a["reps"]}
                good = [d for d in b["acked"] if d in reps]
                if len(good) < W or len({reps[d] for d in good}) < min(2, W):
                    self._end_txn(t, "ABORTED", now)
                    return E(503, "INSUFFICIENT_REPLICAS", f"bloque {b['bid']}: réplicas verificadas={len(good)}, zonas={len({reps[d] for d in good})}")
                b["acked"] = good
            t["state"], t["prepared"] = "PREPARED", blocks
            return {"state": "PREPARED"}

    @replicated_sync
    def txn_commit(self, tid, uid, body, now):
        """Commit atómico: único punto de publicación de una versión (PREPARED -> COMMITTED)."""
        with self.lock:
            t = self.txns.get(tid)
            if not t or t["uid"] != uid:
                return E(404, "NOT_FOUND", "transacción")
            if t["state"] == "COMMITTED":            # idempotente por txn_id
                return t["result"]
            if t["state"] != "PREPARED" or t["exp"] < now:
                if t["state"] in ("OPEN", "PREPARED"):
                    self._end_txn(t, "ABORTED", now)
                return E(410, "LEASE_EXPIRED", "transacción no preparada o vencida")
            path, op, fid = t["path"], t["op"], t["fid"]
            nb = t["prepared"]

            def abort(*a, **k):
                self._end_txn(t, "ABORTED", now)
                return E(*a, **k)
            e = self._check_write(path, self._write_blks(op, t["idx"]), t["holder"], body.get("lock_epochs", t["epochs"]), now)
            if e:
                return abort(*e["err"][:3], **e["err"][3])
            n = self.nodes.get(path)
            if n is not None and n["t"] != "f":
                return abort(400, "INVALID_PATH", "es un directorio")
            if n is not None and n["fid"] != fid:
                # ABA: el archivo se borró y se recreó durante la escritura; los bloques se cifraron para otro file_id
                return abort(409, "VERSION_CONFLICT", "el archivo fue reemplazado por otro durante la escritura", current=self.files[n["fid"]]["cur"])
            if n is None and op in ("a", "r+", "c"):
                return abort(409, "VERSION_CONFLICT", "el archivo fue borrado durante la escritura", current=0)
            f = self.files.get(fid) if n else None
            cur = f["cur"] if f else 0
            base = f["mans"][cur]["blocks"] if f else []
            if cur != t["expected"]:
                # rebase optimista: solo r+ con bloques disjuntos de los cambiados desde la versión base
                idxs = {b["idx"] for b in nb}
                okr = op == "r+" and f is not None
                for v in range(t["expected"] + 1, cur + 1):
                    ch = f["mans"].get(v, {}).get("changed") if f else None
                    if not (okr and isinstance(ch, list) and ch and isinstance(ch[0], int) and not (set(ch) & idxs)):
                        okr = False
                if not okr:
                    return abort(409, "VERSION_CONFLICT", "otro cliente publicó antes", current=cur)
            if n is not None and op == "x":
                return abort(409, "EXISTS", path)
            meta = {b["bid"]: b for b in body.get("blocks", [])}
            if set(meta) != {b["bid"] for b in nb}:
                return abort(400, "INVALID_ARGUMENT", "metadatos de bloques incompletos")

            def ent(b):
                m = meta[b["bid"]]
                return {"idx": b["idx"], "bid": b["bid"], "size": m["size"], "sha": m["sha"], "nonce": m["nonce"], "psha": m["psha"]}
            new = [ent(b) for b in sorted(nb, key=lambda b: b["idx"])]
            if op in ("w", "x"):
                blocks, changed = new, ["all"]
            elif op == "a":
                blocks = [dict(b) for b in base] + new
                if [b["idx"] for b in blocks] != list(range(len(blocks))):
                    return abort(409, "VERSION_CONFLICT", "índices de anexo", current=cur)
                changed = ["append"]
            elif op == "c":
                # compactación de la cola: reemplaza los bloques [i..fin] por bloques llenos con el mismo contenido
                i = t["idx"][0] if t["idx"] else len(base)
                if sum(b["size"] for b in base[i:]) != sum(b["size"] for b in new) or [b["idx"] for b in new] != list(range(i, i + len(new))):
                    return abort(400, "INVALID_ARGUMENT", "la compactación debe conservar el contenido de la cola")
                blocks, changed = [dict(b) for b in base[:i]] + new, ["compact"]
            else:  # r+
                blocks = [dict(b) for b in base]
                for b in new:
                    if b["idx"] >= len(blocks) or blocks[b["idx"]]["size"] != b["size"]:
                        return abort(400, "INVALID_ARGUMENT", "r+ no puede cambiar el tamaño ni escribir más allá de EOF")
                    blocks[b["idx"]] = b
                changed = sorted(b["idx"] for b in new)
            nv = cur + 1
            if f is None:
                f = self.files[fid] = {"uid": uid, "path": path, "cur": 0, "dek": None, "cipher": t["cipher"], "mans": {}, "deleted_at": None}
                self.nodes[path] = {"t": "f", "fid": fid, "uid": uid}
            if op in ("w", "x"):
                f["dek"], f["cipher"] = body["dek"], t["cipher"]
            if cur in f["mans"]:
                f["mans"][cur]["superseded"] = now
            from ..common import list_hash
            f["mans"][nv] = {"blocks": blocks, "size": sum(b["size"] for b in blocks), "csha": list_hash([b["psha"] for b in blocks]),
                             "created": now, "changed": changed, "superseded": None}
            f["cur"] = nv
            self.nodes[path]["mtime"] = now
            for b in nb:
                self.blocks[b["bid"]] = {"reps": list(b["acked"]), "size": meta[b["bid"]]["size"], "sha": meta[b["bid"]]["sha"], "fid": fid, "ct": now}
            self._end_txn(t, "COMMITTED", now)
            t["result"] = {"version": nv, "fid": fid}
            return t["result"]

    # ------------------------------------------------------------------ lectura: open() fija instantánea
    @replicated_sync
    def open_file(self, pid, uid, path, ver, now, pin_ttl):
        with self.lock:
            n = self.nodes.get(path)
            if n is None or n["t"] != "f":
                return E(404, "NOT_FOUND", path)
            f = self.files[n["fid"]]
            v = ver or f["cur"]
            if v not in f["mans"]:
                return E(410, "SNAPSHOT_EXPIRED", f"versión {v}")
            self.pins[pid] = {"fid": n["fid"], "ver": v, "exp": now + pin_ttl}
            m = f["mans"][v]
            return {"fid": n["fid"], "version": v, "current": f["cur"], "size": m["size"], "csha": m["csha"], "cipher": f.get("cipher"),
                    "dek": f["dek"], "blocks": [dict(b, reps=list(self.blocks[b["bid"]]["reps"])) for b in m["blocks"]]}

    # ------------------------------------------------------------------ ubicación de réplicas
    @replicated_sync
    def add_replica(self, bid, dn):
        with self.lock:
            b = self.blocks.get(bid)
            if b and dn not in b["reps"]:
                b["reps"].append(dn)
            return OK

    @replicated_sync
    def remove_replica(self, bid, dn):
        with self.lock:
            b = self.blocks.get(bid)
            if b and dn in b["reps"]:
                b["reps"].remove(dn)
            return OK

    @replicated_sync
    def move_replica(self, bid, src, dst):
        """Rebalanceo/retiro: dst ya tiene la copia verificada; se agrega y se retira src en un solo paso."""
        with self.lock:
            b = self.blocks.get(bid)
            if not b or src not in b["reps"]:
                return E(404, "NOT_FOUND", bid)
            if dst not in b["reps"]:
                b["reps"].append(dst)
            b["reps"].remove(src)
            return OK

    @replicated_sync
    def remove_node(self, dn):
        with self.lock:
            n = 0
            for b in self.blocks.values():
                if dn in b["reps"]:
                    b["reps"].remove(dn)
                    n += 1
            return {"blocks": n}

    @replicated_sync
    def dn_set_admin(self, dn, state):
        with self.lock:
            if state is None:
                self.dn_admin.pop(dn, None)
            else:
                self.dn_admin[dn] = state
            return OK

    # ------------------------------------------------------------------ mantenimiento: TTL de txn, versiones viejas, GC
    @replicated_sync
    def maintenance(self, now, grace):
        with self.lock:
            aborted = 0
            for t in self.txns.values():
                if t["state"] in ("OPEN", "PREPARED") and t["exp"] < now:
                    self._end_txn(t, "ABORTED", now)
                    aborted += 1
            for tid in [k for k, t in self.txns.items() if t["ended"] and t["ended"] < now - 3600]:
                del self.txns[tid]
            for pid in [k for k, p in self.pins.items() if p["exp"] < now]:
                del self.pins[pid]
            for k in [k for k, l in self.locks.items() if l["exp"] < now - 60]:
                del self.locks[k]
            pinned = {(p["fid"], p["ver"]) for p in self.pins.values()}
            for fid in list(self.files):
                f = self.files[fid]
                if f["deleted_at"] is not None:
                    if f["deleted_at"] + grace < now and not any(k[0] == fid for k in pinned):
                        del self.files[fid]
                    continue
                for v in [v for v, m in f["mans"].items()
                          if v != f["cur"] and m["superseded"] is not None and m["superseded"] + grace < now and (fid, v) not in pinned]:
                    del f["mans"][v]
            ref = set(self.staged)
            for f in self.files.values():
                for m in f["mans"].values():
                    ref.update(b["bid"] for b in m["blocks"])
            dead = [(bid, list(b["reps"])) for bid, b in self.blocks.items() if bid not in ref]
            for bid, _ in dead:
                del self.blocks[bid]
            return {"aborted": aborted, "deleted": dead}

    # ------------------------------------------------------------------ lecturas locales auxiliares (en el líder)
    def snapshot_blocks(self):
        with self.lock:
            return {bid: dict(b, reps=list(b["reps"])) for bid, b in self.blocks.items()}

    def block_known(self, bid):
        with self.lock:
            return bid in self.blocks or bid in self.staged

    def block_owner(self, bid):
        with self.lock:
            b = self.blocks.get(bid)
            if b:
                return self.files[b["fid"]]["uid"] if b["fid"] in self.files else None
            t = self.staged.get(bid)
            return self.txns[t]["uid"] if t else None

    def file_blocks(self, path):
        with self.lock:
            n = self.nodes.get(path)
            if not n or n["t"] != "f":
                return None
            f = self.files[n["fid"]]
            return [dict(b, reps=list(self.blocks[b["bid"]]["reps"]), idx=b["idx"]) for b in f["mans"][f["cur"]]["blocks"]]
