import hashlib, os, sys, time, json, pytest
os.environ.setdefault("DFSHA_BLOCK_SIZE", str(256 * 1024))      # 256 KiB en pruebas => muchos bloques con archivos pequeños
os.environ.setdefault("DFSHA_RPC_TIMEOUT", "4")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from scripts.local_cluster import Cluster          # noqa: E402

EVID = os.path.join(ROOT, "evidencias")
os.makedirs(EVID, exist_ok=True)
# se limpia aquí (antes de que corra cualquier prueba, incluidas las que no usan el clúster, como T0) y no al
# primer uso del fixture 'cluster': si no, T0 corre primero (no necesita clúster) y su evidencia se borraría
# en cuanto otra prueba levantara el clúster por primera vez.
for _f in os.listdir(EVID):
    if _f.endswith(".txt"):
        os.remove(os.path.join(EVID, _f))


def sha(b): return hashlib.sha256(b).hexdigest()


def evid(name, text):
    """Guarda evidencia legible de cada prueba en evidencias/<name>.txt"""
    with open(os.path.join(EVID, name + ".txt"), "a") as f:
        f.write(text.rstrip() + "\n")
    print("EVIDENCIA", name, ":", text)


def wait_for(cond, timeout=40, step=0.5, msg="condición"):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            v = cond()
            if v: return v
        except Exception: pass
        time.sleep(step)
    raise AssertionError(f"timeout esperando {msg}")


@pytest.fixture(scope="session")
def cluster():
    c = Cluster().start()
    yield c
    c.stop()


@pytest.fixture()
def alice(cluster): return cluster.client("alice")
@pytest.fixture()
def alice2(cluster): return cluster.client("alice")        # segundo cliente del MISMO usuario (otro cid)
@pytest.fixture()
def bob(cluster): return cluster.client("bob")
@pytest.fixture()
def admin(cluster): return cluster.client("admin")


def full_replication(admin, c, path, n=3, timeout=40):
    def ok():
        bl = admin.admin_blocks(path)["blocks"]
        return all(b["n_replicas"] >= n for b in bl)
    wait_for(ok, timeout, msg=f"R={n} en {path}")
    return admin.admin_blocks(path)["blocks"]


def alive_nodes(admin):
    return [n["dn_id"] for n in admin.admin_nodes()["nodes"] if n["state"] == "ALIVE"]
