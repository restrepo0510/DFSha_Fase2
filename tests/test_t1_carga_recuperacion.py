"""T1: archivo grande particionado en varios nodos y recuperado idéntico (hash). [DIST 12][RF 15]"""
import os, time
from conftest import evid, sha, full_replication


def test_put_get_hash_y_distribucion(cluster, alice, admin, tmp_path):
    data = os.urandom(3 * 1024 * 1024 + 12345)           # 13 bloques de 256 KiB
    src, dst = tmp_path / "in.bin", tmp_path / "out.bin"
    src.write_bytes(data)
    alice.mkdir("/t1")
    t0 = time.time(); r = alice.put(str(src), "/t1/grande.bin"); t_put = time.time() - t0
    t0 = time.time(); g = alice.get("/t1/grande.bin", str(dst)); t_get = time.time() - t0
    assert sha(dst.read_bytes()) == sha(data)
    assert g["blocks"] >= 12
    bl = full_replication(admin, cluster, alice.abspath("/t1/grande.bin"))
    nodes = {r["dn_id"] for b in bl for r in b["replicas"]}
    assert len(nodes) >= 4, "los bloques deben repartirse entre varios nodos"
    for b in bl:
        assert b["n_replicas"] == 3
        assert len({r["dn_id"] for r in b["replicas"]}) == 3           # nodos distintos
        assert len({r["zone"] for r in b["replicas"]}) >= 2            # >= 2 zonas
    lines = [f"archivo {len(data)} B -> {g['blocks']} bloques, versión {r['version']}  put={t_put:.2f}s get={t_get:.2f}s",
             f"sha256 origen   {sha(data)}", f"sha256 recuperado {sha(dst.read_bytes())}  (IDÉNTICO)", f"nodos usados: {sorted(nodes)}"]
    lines += [f"  bloque {b['idx']:>2} {b['block_id'][:12]} -> " + ", ".join(f"{x['dn_id']}({x['zone']})" for x in b["replicas"]) for b in bl]
    evid("T1_carga_recuperacion", "\n".join(lines))


def test_archivo_vacio_y_un_byte(alice, tmp_path):
    for n, d in (("vacio", b""), ("uno", b"x")):
        p = tmp_path / n; p.write_bytes(d)
        alice.put(str(p), f"/{n}.bin"); alice.get(f"/{n}.bin", str(tmp_path / (n + ".out")))
        assert (tmp_path / (n + ".out")).read_bytes() == d
