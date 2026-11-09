"""Clúster local de pruebas: 3 MDS + N DN como procesos separados que se hablan por TCP real (TLS/mTLS).
Uso:  python -m scripts.local_cluster up   (bloquea; Ctrl-C para detener)"""
import os, shutil, signal, subprocess, sys, time
import httpx
from . import gen_certs

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ZONES = ["a", "b", "c", "a", "b", "c", "a"]


class Cluster:
    def __init__(self, base="/tmp/dfsha_local", n_mds=3, n_dn=5, base_port=18000, tls=True, **env):
        self.base, self.n_mds, self.n_dn, self.bp, self.tls = base, n_mds, n_dn, base_port, tls
        self.procs, self.logs = {}, {}
        self.env = {"DFSHA_BLOCK_SIZE": str(256 * 1024), "DFSHA_HB_S": "1", "DFSHA_SUSPECT_S": "3", "DFSHA_DEAD_S": "6", "DFSHA_REPORT_S": "5",
                    "DFSHA_GRACE_S": "20", "DFSHA_ORPHAN_S": "15", "DFSHA_LOCK_LEASE": "6", "DFSHA_TXN_TTL": "20", "DFSHA_RAFT_PASSWORD": "dfsha-raft",
                    "DFSHA_SCRUB_S": "8", "DFSHA_INFLIGHT_S": "10", "DFSHA_PART_GC_S": "20", "DFSHA_CORRUPT_KEEP_S": "60", "DFSHA_BAD_HOLD_S": "60", "DFSHA_TOKEN_TTL": "12",
                    "DFSHA_USERS": "alice:alicepw,bob:bobpw,carol:carolpw,admin:adminpw:admin"}
        self.env.update({k: str(v) for k, v in env.items()})
        self.mds = {f"mds-{i+1}": dict(raft=base_port + i * 10 + 1, http=base_port + i * 10 + 2, grpc=base_port + i * 10 + 3) for i in range(n_mds)}
        self.cluster = ",".join(f"{n}=127.0.0.1:{p['raft']}:{p['http']}:{p['grpc']}" for n, p in self.mds.items())
        self.dn_port = {f"dn-{i+1}": base_port + 500 + i * 10 for i in range(n_dn + 3)}     # + puertos libres para nodos agregados
        self.certs = f"{base}/certs"
        self.ca = f"{self.certs}/ca.pem"
        self.urls = [f"{'https' if tls else 'http'}://127.0.0.1:{p['http']}" for p in self.mds.values()]

    def start(self):
        shutil.rmtree(self.base, ignore_errors=True)
        os.makedirs(self.base)
        gen_certs.main(self.certs, [f"{n}:127.0.0.1,localhost,mds.dfsha.internal" for n in self.mds] + list(self.dn_port)
                       + ["gateway:127.0.0.1,localhost,meta.dfsha.internal"])
        for n in self.mds: self.start_mds(n)
        for n in list(self.dn_port)[:self.n_dn]: self.start_dn(n)
        try:
            self.wait_ready()
        except BaseException:
            self.stop(); raise
        return self

    def _env(self, name, extra):
        e = dict(os.environ, PYTHONPATH=ROOT, DFSHA_MDS_CLUSTER=self.cluster, DFSHA_MDS_GRPC=",".join(f"127.0.0.1:{p['grpc']}" for p in self.mds.values()),
                 DFSHA_KEYS=f"{self.certs}/keys" if name.startswith("mds") else f"{self.certs}/keys-dn", **self.env, **{k: str(v) for k, v in extra.items()})
        if self.tls: e["DFSHA_CERTS"] = f"{self.certs}/{name}"
        return e

    def _spawn(self, name, module, extra):
        log = open(f"{self.base}/{name}.log", "ab")
        self.procs[name] = subprocess.Popen([sys.executable, "-m", module], env=self._env(name, extra), stdout=log, stderr=log, cwd=ROOT)

    def start_mds(self, n): self._spawn(n, "dfsha.mds.server", {"DFSHA_MDS_ID": n, "DFSHA_DATA_DIR": f"{self.base}/data/{n}"})

    def start_dn(self, n, **extra):
        i = int(n.split("-")[1])
        self._spawn(n, "dfsha.dn.server", {"DFSHA_DN_ID": n, "DFSHA_DN_PORT": self.dn_port[n], "DFSHA_DN_ADDR": f"127.0.0.1:{self.dn_port[n]}",
                                           "DFSHA_ZONE": ZONES[(i - 1) % len(ZONES)], "DFSHA_DATA_DIR": f"{self.base}/data/{n}", **extra})

    def kill(self, n, sig=signal.SIGKILL):
        p = self.procs.get(n)
        if p and p.poll() is None:
            p.send_signal(sig); p.wait() if sig == signal.SIGKILL else None

    def pause(self, n): self.procs[n].send_signal(signal.SIGSTOP)
    def resume(self, n): self.procs[n].send_signal(signal.SIGCONT)

    def leader(self):
        for n, p in self.mds.items():
            if self.procs[n].poll() is not None: continue
            try:
                import ssl
                r = httpx.get(f"{self.urls[list(self.mds).index(n)]}/healthz", verify=ssl.create_default_context(cafile=self.ca) if self.tls else False, timeout=2).json()
                if r["leader"]: return n
            except Exception: pass
        return None

    def wait_ready(self, timeout=60):
        t0 = time.time()
        while time.time() - t0 < timeout:
            try:
                if self.leader():
                    from dfsha.client.sdk import DFSha
                    d = DFSha(self.urls, self.ca if self.tls else None, "admin", "adminpw"); d.login()
                    if len([x for x in d.admin_nodes()["nodes"] if x["state"] == "ALIVE"]) >= self.n_dn: return
            except Exception: pass
            time.sleep(0.5)
        raise RuntimeError("clúster no listo; revise " + self.base + "/*.log")

    def stop(self):
        for p in self.procs.values():
            if p.poll() is None: p.kill()
        for p in self.procs.values(): p.wait()

    def client(self, user="alice", pw=None, **kw):
        from dfsha.client.sdk import DFSha
        d = DFSha(self.urls, self.ca if self.tls else None, user, pw or {"alice": "alicepw", "bob": "bobpw", "carol": "carolpw", "admin": "adminpw"}[user], **kw)
        d.login()
        return d


if __name__ == "__main__":
    sys.path.insert(0, ROOT)
    c = Cluster().start()
    print("clúster listo. DFSHA_META=" + ",".join(c.urls) + " DFSHA_CA=" + c.ca)
    try: signal.pause()
    except KeyboardInterrupt: c.stop()
