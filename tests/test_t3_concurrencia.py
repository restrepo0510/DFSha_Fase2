"""T3: dos clientes sobre el mismo archivo/bloque; el resultado coincide con la política (CAS + lock con fencing). [HA 15][RF 15]"""
import os, threading, time, pytest
from dfsha.common import DFSError
from conftest import evid

B = 256 * 1024


def mk(alice, path, nblocks=3):
    with alice.open(path, "x") as h: h.write(b"\x00" * (nblocks * B))


def test_cas_mismo_bloque_gana_uno(alice, alice2):
    alice.mkdir("/t3"); p = "/t3/mismo.bin"; mk(alice, p)
    h1, h2 = alice.open(p, "r+"), alice2.open(p, "r+")           # misma versión base v1
    h1.write(b"AAAA", 10); h2.write(b"BBBB", 10)                  # mismo bloque
    res = {}
    def close(n, h):
        try: h.close(); res[n] = "200"
        except DFSError as e: res[n] = f"{e.code} {e.name}"
    ts = [threading.Thread(target=close, args=(n, h)) for n, h in (("c1", h1), ("c2", h2))]
    [t.start() for t in ts]; [t.join() for t in ts]
    oks = [k for k, v in res.items() if v == "200"]
    assert len(oks) == 1 and any("VERSION_CONFLICT" in v for v in res.values()), res
    final = alice.open(p, "r").read(10, 4)
    assert final == (b"AAAA" if oks[0] == "c1" else b"BBBB")        # sin pérdida silenciosa ni bloque mezclado
    assert alice.stat(p)["version"] == 2
    evid("T3_concurrencia", f"[sin lock, mismo bloque] resultados={res}; contenido final={final!r}; versión={alice.stat(p)['version']}")


def test_rebase_bloques_distintos_ambos_confirman(alice, alice2):
    p = "/t3/distintos.bin"; mk(alice, p)
    h1, h2 = alice.open(p, "r+"), alice2.open(p, "r+")
    h1.write(b"UNO", 5); h2.write(b"DOS", 2 * B + 5)               # bloques 0 y 2
    h1.close(); h2.close()                                         # el líder hace rebase: bloques disjuntos
    r = alice.open(p, "r")
    assert r.read(5, 3) == b"UNO" and r.read(2 * B + 5, 3) == b"DOS" and alice.stat(p)["version"] == 3
    evid("T3_concurrencia", "[sin lock, bloques distintos] ambos commits OK por rebase: versión 3 con cambios de los dos clientes")


def test_anexos_concurrentes_no_conflictuan(cluster):
    c0 = cluster.client("alice"); p = "/t3/log.txt"
    with c0.open(p, "x") as h: h.write(b"inicio\n")
    def ap(i):
        c = cluster.client("alice")
        with c.open(p, "a") as h: h.write(f"linea-{i}\n".encode())
    ts = [threading.Thread(target=ap, args=(i,)) for i in range(4)]
    [t.start() for t in ts]; [t.join() for t in ts]
    h = c0.open(p, "r"); txt = h.read(0, h.size).decode()
    assert all(f"linea-{i}\n" in txt for i in range(4)) and txt.startswith("inicio")
    evid("T3_concurrencia", f"[4 appends concurrentes] los 4 se aplican en orden de commit, size={h.size}")


def test_lock_exclusivo_lease_y_fencing(cluster, alice, alice2):
    p = "/t3/lock.bin"; mk(alice, p)
    e1 = alice.lock(p, auto_renew=False)
    with pytest.raises(DFSError) as e: alice2.lock(p)
    assert e.value.code == 423 and e.value.name == "LOCKED"
    h = alice2.open(p, "r+"); h.write(b"x", 1)                    # escritor sin lock no puede pisar al titular
    with pytest.raises(DFSError) as e: h.close()
    assert e.value.name == "LOCKED"
    w = alice.open(p, "r+"); w.write(b"CON-LOCK", 0); w.close()   # el titular sí
    w2 = alice.open(p, "r+"); w2.write(b"TARDE", 0)               # el titular se "congela" más allá del lease (6 s en pruebas)
    time.sleep(7.5)
    e2 = alice2.lock(p)                                           # otro cliente toma el lock: época mayor
    assert e2 > e1
    with pytest.raises(DFSError) as e: w2.close()                 # titular expirado reaparece
    assert e.value.name == "STALE_FENCING" and e.value.code == 409
    alice2.unlock(p)
    assert alice.open(p, "r").read(0, 8) == b"CON-LOCK"
    evid("T3_concurrencia", f"[lock] 2.º lock -> 423 LOCKED; escritor sin lock -> 423; titular expirado -> 409 STALE_FENCING (épocas {e1} -> {e2}); contenido intacto")
