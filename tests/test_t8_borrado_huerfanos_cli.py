"""Borrado con lápida + GC, instantáneas expiradas (410), escritura interrumpida sin publicar, y CLI. [HA 15][RF 15]"""
import glob, os, subprocess, sys, time
import pytest
from dfsha.common import DFSError
from conftest import evid, sha, wait_for, full_replication, ROOT


def on_disk(cluster, bid): return glob.glob(f"{cluster.base}/data/dn-*/{bid}.blk")


def test_borrado_gc_instantanea_y_escritura_interrumpida(cluster, alice, admin, tmp_path):
    alice.mkdir("/t8"); (tmp_path / "f").write_bytes(os.urandom(600_000))
    alice.put(str(tmp_path / "f"), "/t8/borrar.bin"); alice.put(str(tmp_path / "f"), "/t8/versiones.bin")
    p = alice.abspath("/t8/borrar.bin"); full_replication(admin, cluster, p)
    bids = [b["block_id"] for b in alice.manifest("/t8/borrar.bin")["blocks"]]
    snap = alice.open("/t8/borrar.bin", "r"); content = snap.read(0, snap.size)
    # --- borrado: lápida atómica; el lector con instantánea previa puede terminar dentro de la gracia
    alice.rm("/t8/borrar.bin")
    with pytest.raises(DFSError) as e: alice.stat("/t8/borrar.bin")
    assert e.value.name == "NOT_FOUND"
    h2 = alice.open("/t8/versiones.bin", "r"); v1 = h2.m["version"]
    assert snap.read(0, snap.size) == content and all(on_disk(cluster, b) for b in bids)
    # --- versión antigua: tras reemplazar, v1 sigue legible en la gracia y luego expira (410)
    old_m = alice.manifest("/t8/versiones.bin", version=1)
    with alice.open("/t8/versiones.bin", "w") as w: w.write(b"nueva")
    assert alice.manifest("/t8/versiones.bin", version=1)["version"] == 1
    # --- escritura interrumpida: el cliente "muere" tras subir bloques y no hace commit
    tx = alice._req("POST", "/v1/txn", json={"path": alice.abspath("/t8/fantasma.bin"), "op": "w", "expected_version": 0, "sizes": [1000, 1000], "cid": alice.cid})
    from dfsha.common import encrypt_block, os as _os
    orphan = []
    for b in tx["blocks"]:
        nonce, ct = encrypt_block(os.urandom(32), os.urandom(1000), tx["fid"], b["idx"], b["block_id"]); alice._upload(tx, b, ct); orphan.append(b["block_id"])
    with pytest.raises(DFSError): alice.stat("/t8/fantasma.bin")             # nada publicado
    assert all(on_disk(cluster, b) for b in orphan)
    t0 = time.time()
    # --- GC: tras la gracia (20 s) los bloques del archivo borrado y de la versión vieja se liberan; los huérfanos se eliminan
    wait_for(lambda: not any(on_disk(cluster, b) for b in bids), 90, msg="GC de archivo borrado")
    t_gc = time.time() - t0
    old_bids = [b["block_id"] for b in old_m["blocks"]]
    # (cada open() de v1 la fija otra vez durante la gracia; se espera sin tocarla a que expire y se libere)
    wait_for(lambda: not any(on_disk(cluster, b) for b in old_bids), 90, msg="GC de versión vieja")
    with pytest.raises(DFSError) as e: alice.manifest("/t8/versiones.bin", version=1)
    assert e.value.code == 410 and e.value.name == "SNAPSHOT_EXPIRED"
    wait_for(lambda: not any(on_disk(cluster, b) for b in orphan), 90, msg="GC de huérfanos")
    with pytest.raises(DFSError): alice.stat("/t8/fantasma.bin")
    evid("T8_borrado_huerfanos", f"rm -> lápida (stat 404) y el lector con instantánea siguió leyendo en la gracia; GC liberó {len(bids)} bloques a los {t_gc:.0f}s; "
         f"versión 1 reemplazada -> 410 SNAPSHOT_EXPIRED tras la gracia y sus bloques liberados; cliente muerto sin commit: archivo nunca publicado, "
         f"{len(orphan)} bloques huérfanos eliminados por GC (TTL de la transacción + gracia)")


def test_cli_end_to_end(cluster, tmp_path):
    env = dict(os.environ, PYTHONPATH=ROOT, DFSHA_HOME=str(tmp_path / "h"), DFSHA_META=",".join(cluster.urls), DFSHA_CA=cluster.ca, DFSHA_PASSWORD="alicepw")
    def run(*a, inp=None):
        r = subprocess.run([sys.executable, "-m", "dfsha.client", *a], env=env, capture_output=True, input=inp, timeout=60, cwd=ROOT)
        assert r.returncode == 0, (a, r.stderr.decode())
        return r.stdout
    run("login", "alice"); run("mkdir", "cli"); run("cd", "cli")
    (tmp_path / "x").write_bytes(os.urandom(500_000)); run("put", str(tmp_path / "x"), "x.bin")
    assert b"x.bin" in run("ls"); run("get", "x.bin", str(tmp_path / "y")); assert (tmp_path / "y").read_bytes() == (tmp_path / "x").read_bytes()
    run("write", "x.bin", "--off", "10", inp=b"PARCHE"); assert run("cat", "x.bin", "--off", "10", "--len", "6") == b"PARCHE"
    run("append", "x.bin", inp=b"+fin"); run("mv", "x.bin", "z.bin"); run("lock", "z.bin", "--hold", "1")
    run("rm", "z.bin"); run("cd", ".."); run("rmdir", "cli")
    evid("CLI", "login, mkdir, cd, put, ls, get (hash idéntico), write r+ por offset, cat con rango, append, mv, lock, rm, rmdir: OK")
