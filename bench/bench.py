#!/usr/bin/env python3
"""Experimentos E1–E10 del diseño (§10). Cada medición se repite --reps veces (≥ 5 en el diseño); se reporta media,
desviación y p95, verificando integridad por hash. Resultados en CSV (evidencias/bench.csv) para graficar (bench/plot.py).

  Local (levanta sus propios clústeres de procesos reales con TLS/mTLS):
      python bench/bench.py --local --exp all --reps 5
  Contra un clúster existente (AWS):
      DFSHA_META=https://meta.dfsha.internal DFSHA_CA=/opt/dfsha/ca.pem \\
      python bench/bench.py --exp e1,e2,e3,e5 --cond aws --sizes 1,100,1024
      # E6/E7/E8 en AWS necesitan cómo inyectar el fallo y repararlo (se ejecutan con el shell):
      python bench/bench.py --exp e6 --cond aws --fault-cmd "ssh dn-1 sudo /opt/dfsha/repo/deploy/aws/chaos.sh kill" \\
                            --heal-cmd "ssh dn-1 sudo /opt/dfsha/repo/deploy/aws/chaos.sh start"
      # E4 (R/W), E9 (número de nodos) y E10 (TLS/AES apagados) se miden reconfigurando el clúster entre corridas (--cond).

  E1 tamaño de archivo · E2 flujos paralelos · E3 tamaño de bloque · E4 R=1/W=1 vs R=3/W=2 · E5 clientes concurrentes
  (lecturas; escrituras en bloques distintos y en el mismo) · E6 caída de un DN durante lectura/escritura · E7 caída del líder
  · E8 re-replicación según volumen · E9 escalabilidad 3→5→7 DN · E10 sobrecosto de TLS/mTLS y AES-GCM."""
import argparse, csv, hashlib, httpx, os, resource, statistics as st, subprocess, sys, tempfile, threading, time
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
MB = 1024 * 1024
FIELDS = ["experiment", "condition", "op", "size_mb", "block_mb", "parallel", "clients", "reps", "mean_s", "std_s", "p95_s",
          "throughput_mb_s", "cpu_s", "extra"]


def p95(v):
    v = sorted(v); return v[min(len(v) - 1, int(round(0.95 * (len(v) - 1))))]


def summarize(rows, base, xs, size_mb=0, cpu=None, **extra):
    m = st.mean(xs)
    r = dict(base, reps=len(xs), mean_s=round(m, 4), std_s=round(st.pstdev(xs), 4), p95_s=round(p95(xs), 4),
             throughput_mb_s=round(size_mb / m, 2) if size_mb and m > 0 else "", cpu_s=round(cpu, 4) if cpu is not None else "",
             extra=";".join(f"{k}={v}" for k, v in extra.items()))
    rows.append(r)
    print({k: r.get(k) for k in ("experiment", "condition", "op", "size_mb", "block_mb", "parallel", "clients", "mean_s", "p95_s", "throughput_mb_s", "extra")}, flush=True)


def cpu_now():
    u = resource.getrusage(resource.RUSAGE_SELF)
    return u.ru_utime + u.ru_stime


def mkfile(tmp, size_mb):
    p = os.path.join(tmp, f"src_{size_mb}")
    if not os.path.exists(p):
        with open(p, "wb") as f:
            for _ in range(int(size_mb)):
                f.write(os.urandom(MB))
            rest = int((size_mb - int(size_mb)) * MB)
            if rest: f.write(os.urandom(rest))
    return p, hashlib.sha256(open(p, "rb").read()).hexdigest()


def put_get(c, rows, exp, cond, tmp, size_mb, par, reps, block_mb=None):
    from dfsha.client.sdk import DFSha  # noqa
    c.parallel = par
    if block_mb: c.block_size = int(block_mb * MB)
    src, h = mkfile(tmp, size_mb)
    c.mkdir("/bench", parents=True)
    name = f"/bench/{exp}_{size_mb}_{par}_{block_mb}"
    c.put(src, name)                                    # calentamiento descartado (handshakes TLS, canales gRPC)
    xs, cpu0 = [], cpu_now()
    for _ in range(reps):
        t = time.perf_counter(); c.put(src, name); xs.append(time.perf_counter() - t)
    base = dict(experiment=exp, condition=cond, size_mb=size_mb, block_mb=round(c.block_size / MB, 2), parallel=par, clients=1)
    summarize(rows, dict(base, op="put"), xs, size_mb, (cpu_now() - cpu0) / reps, blocks=len(c.manifest(name)["blocks"]))
    dst = src + ".out"; xs, cpu0 = [], cpu_now()
    for _ in range(reps):
        t = time.perf_counter(); c.get(name, dst); xs.append(time.perf_counter() - t)
        assert hashlib.sha256(open(dst, "rb").read()).hexdigest() == h            # integridad en cada medición
    summarize(rows, dict(base, op="get"), xs, size_mb, (cpu_now() - cpu0) / reps)


# ------------------------------------------------------------------ experimentos
def e1(ctx):
    for s in ctx.sizes:
        put_get(ctx.client(), ctx.rows, "E1", ctx.cond, ctx.tmp, s, 4, ctx.reps)


def e2(ctx):
    for p in ctx.parallel:
        put_get(ctx.client(), ctx.rows, "E2", ctx.cond, ctx.tmp, ctx.big, p, ctx.reps)


def e3(ctx):
    for b in ctx.block_sizes:
        put_get(ctx.client(), ctx.rows, "E3", ctx.cond, ctx.tmp, ctx.big, 4, ctx.reps, block_mb=b)


def e4(ctx):
    put_get(ctx.client(), ctx.rows, "E4", ctx.cond, ctx.tmp, ctx.mid, 4, ctx.reps)


def e5(ctx):
    """Clientes concurrentes: lecturas del mismo archivo; escrituras r+ en bloques distintos y en el mismo bloque."""
    from dfsha.common import DFSError
    c0 = ctx.client(); src, h = mkfile(ctx.tmp, ctx.mid)
    c0.mkdir("/bench", parents=True); c0.put(src, "/bench/e5.bin")
    nblocks = len(c0.manifest("/bench/e5.bin")["blocks"])
    for n in ctx.clients:
        cs = [ctx.client() for _ in range(n)]
        # lecturas concurrentes (throughput agregado)
        xs = []
        for r in range(ctx.reps + 1):                       # la primera repetición es calentamiento y se descarta
            def rd(c): c.get("/bench/e5.bin", os.path.join(ctx.tmp, f"e5_{id(c)}"))
            ts = [threading.Thread(target=rd, args=(c,)) for c in cs]
            t = time.perf_counter(); [x.start() for x in ts]; [x.join() for x in ts]
            if r: xs.append(time.perf_counter() - t)
        summarize(ctx.rows, dict(experiment="E5", condition=ctx.cond, op="get_concurrente", size_mb=ctx.mid * n, block_mb=round(c0.block_size / MB, 2),
                                 parallel=4, clients=n), xs, ctx.mid * n)
        # escrituras concurrentes: bloques distintos (rebase) y mismo bloque (CAS)
        for kind in ("bloques_distintos", "mismo_bloque"):
            xs, lat, conflicts, total = [], [], 0, 0
            for _ in range(ctx.reps):
                hs = [c.open("/bench/e5.bin", "r+") for c in cs]
                for i, hd in enumerate(hs):
                    off = (i % nblocks) * c0.block_size + 10 if kind == "bloques_distintos" else 10
                    hd.write(b"W%02d" % i, off)
                res = []

                def wr(hd):
                    t = time.perf_counter()
                    try: hd.close(); res.append(("ok", time.perf_counter() - t))
                    except DFSError as e: res.append((e.name, time.perf_counter() - t))
                ts = [threading.Thread(target=wr, args=(hd,)) for hd in hs]
                t = time.perf_counter(); [x.start() for x in ts]; [x.join() for x in ts]; xs.append(time.perf_counter() - t)
                conflicts += sum(1 for r in res if r[0] == "VERSION_CONFLICT"); total += len(res); lat += [r[1] for r in res]
            summarize(ctx.rows, dict(experiment="E5", condition=ctx.cond, op=f"write_{kind}", size_mb=0, block_mb=round(c0.block_size / MB, 2),
                                     parallel=4, clients=n), xs, 0, None, conflictos_pct=round(100 * conflicts / total, 1),
                      commit_p95_s=round(p95(lat), 4), commits_por_s=round(total / sum(xs), 2))


def e6(ctx):
    """Caída de un DN durante una lectura y durante una escritura: éxito, tiempo y ausencia de archivos parciales."""
    c = ctx.client(); src, h = mkfile(ctx.tmp, ctx.mid)
    c.mkdir("/bench", parents=True); c.put(src, "/bench/e6.bin")
    for phase in ("lectura", "escritura"):
        xs, ok = [], 0
        for i in range(ctx.reps):
            victim = ctx.pick_victim("/bench/e6.bin")
            timer = threading.Timer(0.05, ctx.fault, args=(victim,))
            t = time.perf_counter(); timer.start()
            try:
                if phase == "lectura":
                    c.get("/bench/e6.bin", src + ".e6"); good = hashlib.sha256(open(src + ".e6", "rb").read()).hexdigest() == h
                else:
                    c.put(src, f"/bench/e6_w{i}.bin"); good = c.stat(f"/bench/e6_w{i}.bin")["size"] == os.path.getsize(src)
            except Exception:
                good = False
            xs.append(time.perf_counter() - t); timer.join(); ok += good
            ctx.heal(victim)
        summarize(ctx.rows, dict(experiment="E6", condition=ctx.cond, op=f"caida_dn_{phase}", size_mb=ctx.mid, block_mb=round(c.block_size / MB, 2),
                                 parallel=4, clients=1), xs, ctx.mid, None, exito_pct=round(100 * ok / ctx.reps, 1))


def e7(ctx):
    """Caída del líder de metadatos: ventana de indisponibilidad (desde el kill hasta la primera operación exitosa)."""
    c = ctx.client(); c.mkdir("/bench", parents=True)
    xs = []
    for i in range(ctx.reps):
        c.stat("/bench")
        t = time.perf_counter(); old = ctx.kill_leader()
        try:
            c.mkdir(f"/bench/e7_{i}_{int(time.time())}"); xs.append(time.perf_counter() - t)
        finally:
            ctx.restart_leader(old)   # siempre se reincorpora antes de la próxima repetición, incluso si esta falló
    summarize(ctx.rows, dict(experiment="E7", condition=ctx.cond, op="caida_lider", size_mb=0, block_mb=0, parallel=1, clients=1), xs)


def e8(ctx):
    """Re-replicación tras la caída de un DN: tiempo hasta volver a R=3 según el volumen de datos que tenía el nodo.
    Se mide desde que el líder lo declara DEAD hasta que todos los bloques del archivo vuelven a tener 3 réplicas."""
    adm = ctx.client("admin")
    for vol in ctx.volumes:
        xs = []
        c = ctx.client(); src, _ = mkfile(ctx.tmp, vol)
        for i in range(ctx.reps):
            name = f"/bench/e8_{vol}_{i}.bin"; c.mkdir("/bench", parents=True); c.put(src, name)
            path = c.abspath(name)
            ctx.wait(lambda: all(b["n_replicas"] >= 3 for b in adm.admin_blocks(path)["blocks"]), 120)
            victim = ctx.pick_victim(name)
            ctx.fault(victim)
            ctx.wait(lambda: next(n for n in adm.admin_nodes()["nodes"] if n["dn_id"] == victim)["state"] == "DEAD", 120)
            t = time.perf_counter()
            ctx.wait(lambda: all(b["n_replicas"] >= 3 and victim not in [r["dn_id"] for r in b["replicas"]] for b in adm.admin_blocks(path)["blocks"]), 600)
            xs.append(time.perf_counter() - t)
            ctx.heal(victim)
            c.rm(name)
        summarize(ctx.rows, dict(experiment="E8", condition=ctx.cond, op="re_replicacion", size_mb=vol, block_mb=round(c.block_size / MB, 2),
                                 parallel=1, clients=1), xs, vol)


def e9(ctx):
    """Escalabilidad: throughput agregado de N clientes con el número de DN de la condición (3, 5 o 7)."""
    n = max(ctx.clients); src, h = mkfile(ctx.tmp, ctx.mid)
    cs = [ctx.client() for _ in range(n)]
    cs[0].mkdir("/bench", parents=True)
    for op in ("put", "get"):
        xs = []
        for r in range(ctx.reps + 1):                       # la primera repetición es calentamiento y se descarta
            def job(i, c):
                if op == "put": c.put(src, f"/bench/e9_{i}.bin")
                else: c.get(f"/bench/e9_{i}.bin", src + f".e9_{i}")
            ts = [threading.Thread(target=job, args=(i, c)) for i, c in enumerate(cs)]
            t = time.perf_counter(); [x.start() for x in ts]; [x.join() for x in ts]
            if r: xs.append(time.perf_counter() - t)
        summarize(ctx.rows, dict(experiment="E9", condition=ctx.cond, op=f"{op}_agregado", size_mb=ctx.mid * n, block_mb=round(cs[0].block_size / MB, 2),
                                 parallel=4, clients=n), xs, ctx.mid * n, None, nodos_datos=ctx.n_dn)


def e10(ctx):
    put_get(ctx.client(), ctx.rows, "E10", ctx.cond, ctx.tmp, ctx.mid, 4, ctx.reps)


EXPS = {"e1": e1, "e2": e2, "e3": e3, "e4": e4, "e5": e5, "e6": e6, "e7": e7, "e8": e8, "e9": e9, "e10": e10}


# ------------------------------------------------------------------ contexto: local (clúster propio) o remoto (AWS)
class Ctx:
    def __init__(self, a, rows, tmp, cluster=None, cond=None, n_dn=None):
        self.a, self.rows, self.tmp, self.cluster = a, rows, tmp, cluster
        self.cond = cond or a.cond
        self.reps, self.sizes = a.reps, [float(x) for x in a.sizes.split(",")]
        self.parallel = [int(x) for x in a.parallel.split(",")]
        self.block_sizes = [float(x) for x in a.block_sizes.split(",")]
        self.clients = [int(x) for x in a.clients.split(",")]
        self.volumes = [float(x) for x in a.volumes.split(",")]
        self.big, self.mid = float(a.big), float(a.mid)
        self.n_dn = n_dn or a.n_dn
        self.block_mb = a.block_mb

    def client(self, user=None):
        from dfsha.client.sdk import DFSha
        user = user or self.a.user
        pw = {"admin": self.a.admin_password}.get(user, self.a.password)
        if self.cluster:
            c = self.cluster.client(user, pw)
        else:
            c = DFSha(user=user, password=pw); c.login()
        if self.block_mb: c.block_size = int(self.block_mb * MB)
        return c

    def warm(self):
        """Calentamiento descartado: espera a que el líder acepte escrituras (tras arrancar reconstruye el registro de nodos)."""
        c = self.client(); c.mkdir("/bench", parents=True)
        p = os.path.join(self.tmp, "warm"); open(p, "wb").write(os.urandom(4096))
        self.wait(lambda: c.put(p, "/bench/warm.bin"), 60)

    def wait(self, cond, timeout):
        t0 = time.time()
        while time.time() - t0 < timeout:
            try:
                if cond(): return
            except Exception: pass
            time.sleep(0.5)
        raise RuntimeError("timeout")

    def pick_victim(self, name):
        adm = self.client("admin")
        reps = [r["dn_id"] for r in adm.admin_blocks(self.client().abspath(name))["blocks"][0]["replicas"]]
        return sorted(reps)[0]

    def fault(self, victim):
        if self.cluster: self.cluster.kill(victim)
        else: subprocess.run(self.a.fault_cmd.replace("{dn}", victim), shell=True, check=False)

    def heal(self, victim):
        if self.cluster:
            self.cluster.start_dn(victim)
            adm = self.client("admin")
            self.wait(lambda: any(n["dn_id"] == victim and n["state"] == "ALIVE" for n in adm.admin_nodes()["nodes"]), 60)
        else:
            subprocess.run(self.a.heal_cmd.replace("{dn}", victim), shell=True, check=False); time.sleep(10)

    def kill_leader(self):
        if self.cluster:
            old = self.cluster.leader(); self.cluster.kill(old); return old
        subprocess.run(self.a.leader_fault_cmd, shell=True, check=False); return None

    def restart_leader(self, old):
        if self.cluster:
            import ssl
            self.cluster.start_mds(old)
            # no basta con dormir: hay que esperar a que haya líder estable Y a que 'old' vuelva a responder,
            # si no, una repetición siguiente podría matar al nuevo líder mientras 'old' aún no se reincorporó
            # al quórum (quedarían < 2/3 vivos y la siguiente operación se quedaría sin respuesta)
            self.wait(self.cluster.leader, 30)
            url = self.cluster.urls[list(self.cluster.mds).index(old)]
            ctxs = ssl.create_default_context(cafile=self.cluster.ca) if self.cluster.tls else False
            self.wait(lambda: httpx.get(f"{url}/healthz", verify=ctxs, timeout=2).status_code == 200, 15)
        else:
            subprocess.run(self.a.leader_heal_cmd, shell=True, check=False); time.sleep(10)


LOCAL_PLAN = [   # (experimentos, condición, opciones del clúster local)
    (["e1", "e2", "e3", "e5", "e6", "e7"], "local_R3W2_tls_aes", dict()),
    (["e8"], "local_R3W2_tls_aes", dict()),          # clúster propio: el nodo caído solo tiene los datos del experimento
    (["e4"], "local_R3W2", dict()),
    (["e4"], "local_R1W1", dict(DFSHA_R=1, DFSHA_W=1)),
    (["e9"], "local_3dn", dict(n_dn=3)),
    (["e9"], "local_5dn", dict(n_dn=5)),
    (["e9"], "local_7dn", dict(n_dn=7)),
    (["e10"], "tls_on_aes_on", dict()),
    (["e10"], "tls_on_aes_off", dict(plain=True)),
    (["e10"], "tls_off_aes_on", dict(tls=False)),
    (["e10"], "tls_off_aes_off", dict(tls=False, plain=True)),
]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--local", action="store_true"); ap.add_argument("--exp", default="all"); ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--user", default="alice"); ap.add_argument("--password", default="alicepw"); ap.add_argument("--admin-password", default="adminpw")
    ap.add_argument("--sizes", default="1,10,50"); ap.add_argument("--parallel", default="1,2,4,8"); ap.add_argument("--block-sizes", default="4,16,64")
    ap.add_argument("--clients", default="1,2,4,8"); ap.add_argument("--volumes", default="16,48,96")
    ap.add_argument("--big", default="64"); ap.add_argument("--mid", default="16"); ap.add_argument("--block-mb", type=float, default=0)
    ap.add_argument("--n-dn", type=int, default=0, help="DN de la condición (solo informativo en E9 remoto)")
    ap.add_argument("--cond", default="aws"); ap.add_argument("--out", default=os.path.join(ROOT, "evidencias", "bench.csv"))
    ap.add_argument("--fault-cmd", default=""); ap.add_argument("--heal-cmd", default="")
    ap.add_argument("--leader-fault-cmd", default=""); ap.add_argument("--leader-heal-cmd", default="")
    a = ap.parse_args()
    want = list(EXPS) if a.exp == "all" else a.exp.split(",")
    tmp = tempfile.mkdtemp(); rows = []
    try:
        if a.local:
            from scripts.local_cluster import Cluster
            for exps, cond, opts in LOCAL_PLAN:
                exps = [e for e in exps if e in want]
                if not exps: continue
                env = dict(DFSHA_BLOCK_SIZE=64 * MB, DFSHA_REPORT_S=10, DFSHA_REBALANCE=0, DFSHA_REPL_MBPS=200, DFSHA_GRACE_S=5, DFSHA_ORPHAN_S=10)
                env.update({k: v for k, v in opts.items() if k.startswith("DFSHA_")})
                if opts.get("plain"): env["DFSHA_ALLOW_PLAINTEXT"] = 1
                for k in ("DFSHA_R", "DFSHA_W"):           # el cliente toma W del servidor; R/W solo afectan a los procesos del clúster
                    os.environ.pop(k, None)
                os.environ["DFSHA_BENCH_PLAINTEXT"] = "1" if opts.get("plain") else "0"
                n_dn = opts.get("n_dn", 5)
                c = Cluster(base="/tmp/dfsha_bench", n_dn=n_dn, tls=opts.get("tls", True), **env).start()
                try:
                    ctx = Ctx(a, rows, tmp, c, cond, n_dn)
                    if not a.block_mb: ctx.block_mb = 4             # bloques de 4 MiB en local (muchos bloques con archivos chicos)
                    ctx.warm()
                    for e in exps:
                        print(f"=== {e.upper()} [{cond}]", flush=True)
                        EXPS[e](ctx)
                finally:
                    c.stop()
                os.environ["DFSHA_BENCH_PLAINTEXT"] = "0"
        else:
            ctx = Ctx(a, rows, tmp)
            for e in want:
                print(f"=== {e.upper()} [{a.cond}]", flush=True)
                EXPS[e](ctx)
    finally:
        if rows:
            new = not os.path.exists(a.out)
            with open(a.out, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=FIELDS); new and w.writeheader(); w.writerows(rows)
            print("CSV:", a.out)


if __name__ == "__main__":
    main()
