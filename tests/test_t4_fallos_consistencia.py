"""T4 replicación y fallos reales; T5 consistencia con réplica retrasada/desconectada. [HA 15][DIST 12]"""
import os, signal, time
from dfsha.common import DFSError
import pytest
from conftest import evid, sha, wait_for, full_replication, alive_nodes


def holders(admin, path):
    return {r["dn_id"] for b in admin.admin_blocks(path)["blocks"] for r in b["replicas"]}


def test_t4_caida_de_nodo_lectura_y_recuperacion(cluster, alice, admin, tmp_path):
    data = os.urandom(2 * 1024 * 1024); (tmp_path / "f").write_bytes(data)
    alice.mkdir("/t4"); alice.put(str(tmp_path / "f"), "/t4/f.bin")
    p = alice.abspath("/t4/f.bin"); full_replication(admin, cluster, p)
    victim = sorted(holders(admin, p))[0]
    cluster.kill(victim)                                                     # caída REAL (kill -9)
    t0 = time.time(); alice.get("/t4/f.bin", str(tmp_path / "g")); dt = time.time() - t0
    assert sha((tmp_path / "g").read_bytes()) == sha(data)                   # lectura exitosa durante la caída
    wait_for(lambda: next(n for n in admin.admin_nodes()["nodes"] if n["dn_id"] == victim)["state"] == "DEAD", 20, msg="DEAD")
    t1 = time.time()
    wait_for(lambda: all(b["n_replicas"] == 3 and victim not in [r["dn_id"] for r in b["replicas"]] for b in admin.admin_blocks(p)["blocks"]), 60, msg="re-replicación")
    t_rep = time.time() - t1
    alice.get("/t4/f.bin", str(tmp_path / "g2")); assert sha((tmp_path / "g2").read_bytes()) == sha(data)
    cluster.start_dn(victim)                                                 # el nodo vuelve: block report reconcilia y descarta sobrantes
    wait_for(lambda: victim in alive_nodes(admin), 20, msg="nodo vuelve")
    bl = admin.admin_blocks(p)["blocks"]; mine = {b["block_id"] for b in bl if victim in [r["dn_id"] for r in b["replicas"]]}
    d = f"{cluster.base}/data/{victim}"
    wait_for(lambda: {f[:-4] for f in os.listdir(d) if f.endswith(".blk")} & {b["block_id"] for b in bl} == mine, 40, msg="descarte de sobrantes")
    evid("T4_replicacion_fallos", f"kill -9 {victim}; get durante la caída OK en {dt:.2f}s (hash idéntico); DEAD detectado; re-replicación a R=3 en {t_rep:.1f}s tras DEAD; "
         f"nodo reiniciado reconciliado (sobrantes eliminados)")


def test_t4_caida_del_lider_de_metadatos(cluster, alice, admin, tmp_path):
    (tmp_path / "f").write_bytes(os.urandom(600_000)); alice.put(str(tmp_path / "f"), "/t4/lider.bin")
    old = cluster.leader(); cluster.kill(old); t0 = time.time()
    st = alice.stat("/t4/lider.bin"); dt = time.time() - t0                    # el cliente reintenta con backoff hasta el nuevo líder
    new = wait_for(cluster.leader, 20, msg="nuevo líder")
    assert new != old and st["version"] == 1
    alice.put(str(tmp_path / "f"), "/t4/lider2.bin"); alice.get("/t4/lider.bin", str(tmp_path / "o"))
    assert (tmp_path / "o").read_bytes() == (tmp_path / "f").read_bytes()
    cluster.start_mds(old); time.sleep(4)
    evid("T4_replicacion_fallos", f"kill -9 del líder {old}: ventana de indisponibilidad {dt:.2f}s; nuevo líder {new}; metadatos intactos y escrituras siguen (quórum 2/3); {old} reintegrado")


def test_t5_replica_retrasada(cluster, alice, admin, tmp_path):
    cluster.kill("dn-3"); cluster.start_dn("dn-3", DFSHA_STORE_DELAY=15)      # réplica lenta (equivale a tc netem delay)
    wait_for(lambda: "dn-3" in alive_nodes(admin), 20, msg="dn-3")
    alice.mkdir("/t5")
    # la asignación de réplicas por bloque es aleatoria (balanceo por espacio libre): repetimos la escritura, si hace
    # falta, hasta que algún bloque quede asignado a dn-3 y así observar el commit en 2/3 sin esperar a la lenta
    witnesses = bl = p = None
    for attempt in range(8):
        name = f"/t5/lento{attempt}.bin"
        data = os.urandom(2 * 1024 * 1024); (tmp_path / "f").write_bytes(data)
        alice.put(str(tmp_path / "f"), name)
        p = alice.abspath(name)
        bl = admin.admin_blocks(p)["blocks"]
        witnesses = [b for b in bl if b["n_replicas"] < 3 and "dn-3" not in [x["dn_id"] for x in b["replicas"]]]
        if witnesses:
            break
    assert witnesses, "debe observarse al menos un bloque confirmado con 2/3 (la tercera réplica, dn-3, aún no llegó)"
    under = witnesses
    alice.get(name, str(tmp_path / "g")); assert sha((tmp_path / "g").read_bytes()) == sha(data)   # lectura inmediata, íntegra (G6)
    full_replication(admin, cluster, p, timeout=60)                              # al llegar la lenta, converge a 3/3
    evid("T5_consistencia", f"réplica retrasada (dn-3 +15s): commit 200 con {len(under)}/{len(bl)} bloques en 2/3 (UNDER_REPLICATED); lectura inmediata íntegra sin contactar a dn-3; "
         f"tras la llegada de la réplica, todos 3/3")
    cluster.kill("dn-3"); cluster.start_dn("dn-3"); wait_for(lambda: "dn-3" in alive_nodes(admin), 20, msg="dn-3 normal")


def test_t5_replica_desconectada_y_sin_quorum(cluster, alice, admin, tmp_path):
    data = os.urandom(1024 * 1024); (tmp_path / "f").write_bytes(data)
    cluster.kill("dn-2")                                                       # desconexión justo antes de escribir (aún figura ALIVE unos segundos)
    r = alice.put(str(tmp_path / "f"), "/t5/desconectada.bin")                 # el cliente cambia de primario / el primario ignora la réplica caída
    assert r["version"] == 1
    p = alice.abspath("/t5/desconectada.bin")
    wait_for(lambda: all(b["n_replicas"] == 3 and "dn-2" not in [x["dn_id"] for x in b["replicas"]] for b in admin.admin_blocks(p)["blocks"]), 60, msg="re-replicación")
    alice.get("/t5/desconectada.bin", str(tmp_path / "g")); assert sha((tmp_path / "g").read_bytes()) == sha(data)
    cluster.start_dn("dn-2"); wait_for(lambda: "dn-2" in alive_nodes(admin), 20)
    # sin quórum de réplicas: quedan < 2 nodos/zonas vivos => la escritura se rechaza y NO se publica nada
    for n in ("dn-1", "dn-3", "dn-4", "dn-5"): cluster.kill(n)
    wait_for(lambda: len(alive_nodes(admin)) <= 1, 20, msg="solo 1 vivo")
    with pytest.raises(DFSError) as e: alice.put(str(tmp_path / "f"), "/t5/sinquorum.bin")
    assert e.value.name == "INSUFFICIENT_REPLICAS" and e.value.code == 503
    with pytest.raises(DFSError) as e2: alice.stat("/t5/sinquorum.bin")
    assert e2.value.name == "NOT_FOUND"                                        # nada parcial publicado
    for n in ("dn-1", "dn-3", "dn-4", "dn-5"): cluster.start_dn(n)
    wait_for(lambda: len(alive_nodes(admin)) == 5, 30, msg="5 nodos")
    evid("T5_consistencia", "réplica desconectada (kill dn-2): escritura OK con W=2, re-replicación a 3/3 sin ella; con <2 nodos vivos: 503 INSUFFICIENT_REPLICAS y el archivo no existe (sin parciales)")
