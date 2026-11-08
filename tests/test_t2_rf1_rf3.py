"""RF1 (directorios) y T2: RF3 read()/write() con la semántica declarada. [RF 15]"""
import os, pytest
from dfsha.common import DFSError
from conftest import evid, sha


def test_rf1_directorios(alice):
    alice.mkdir("/rf1"); alice.mkdir("/rf1/a/b", parents=True)
    assert {e["name"] for e in alice.ls("/rf1")} == {"a"}
    alice.cd("/rf1/a"); assert alice.cwd.endswith("/rf1/a")
    assert [e["name"] for e in alice.ls(".")] == ["b"]
    with pytest.raises(DFSError) as e: alice.rmdir("/rf1/a")                  # no vacío
    assert e.value.name == "NOT_EMPTY"
    alice.mv("/rf1/a/b", "/rf1/c"); assert {e["name"] for e in alice.ls("/rf1")} == {"a", "c"}
    with pytest.raises(DFSError) as e: alice.mkdir("/rf1/c")
    assert e.value.name == "EXISTS"
    alice.rm("/rf1", recursive=True)
    with pytest.raises(DFSError) as e: alice.ls("/rf1")
    assert e.value.name == "NOT_FOUND"
    alice.cwd = "/users/alice"
    evid("RF1_directorios", "mkdir -p, ls, cd, rmdir (409 NOT_EMPTY), mv, mkdir existente (409 EXISTS), rm -r, ls inexistente (404): OK")


def test_t2_semantica_rf3(cluster, alice, alice2, admin):
    B = 256 * 1024
    alice.mkdir("/rf3")
    p = "/rf3/doc.bin"
    # x: crear; repetir falla
    with alice.open(p, "x") as h: h.write(os.urandom(3 * B))
    with pytest.raises(DFSError) as e: alice.open(p, "x")
    assert e.value.name == "EXISTS"
    m1 = alice.manifest(p); assert m1["version"] == 1 and len(m1["blocks"]) == 3
    orig = alice.open(p, "r").read(0, 3 * B)
    # r+: modifica por bloque, copy-on-write: cambia exactamente UN block_id
    with alice.open(p, "r+") as h:
        h.write(b"MODIFICADO" * 100, B + 5)
    m2 = alice.manifest(p)
    changed = [i for i in range(3) if m1["blocks"][i]["block_id"] != m2["blocks"][i]["block_id"]]
    assert m2["version"] == 2 and changed == [1]
    new = alice.open(p, "r").read(0, 3 * B)
    assert new[B + 5:B + 1005] == b"MODIFICADO" * 100 and new[:B] == orig[:B] and new[2 * B:] == orig[2 * B:]
    # r+ no escribe más allá de EOF
    with pytest.raises(DFSError) as e: alice.open(p, "r+").write(b"zz", 3 * B - 1)
    assert e.value.name == "INVALID_ARGUMENT"
    # read(off,n) solo trae los bloques afectados
    h = alice2.open(p, "r"); f0 = alice2.stats["blocks_fetched"]
    chunk = h.read(B - 10, 20)                       # cruza el borde bloque0/bloque1 => 2 bloques
    assert chunk == new[B - 10:B + 10] and alice2.stats["blocks_fetched"] - f0 == 2
    # a: anexar
    with alice.open(p, "a") as h: h.write(b"FIN-DEL-ARCHIVO")
    m3 = alice.manifest(p); assert m3["version"] == 3 and m3["size"] == 3 * B + 15 and len(m3["blocks"]) == 4
    assert alice.open(p, "r").read(3 * B, 100) == b"FIN-DEL-ARCHIVO"
    # close-to-open: el lector con instantánea previa no ve a medias; tras close, un open nuevo ve la versión nueva
    reader = alice2.open(p, "r"); snap_v = reader.m["version"]
    w = alice.open(p, "w"); w.write(b"REEMPLAZO-COMPLETO")
    assert alice2.open(p, "r").read(0, 50)[:5] != b"REEMP"           # escrituras privadas hasta close
    w.close()
    assert alice2.open(p, "r").read(0, 50) == b"REEMPLAZO-COMPLETO"   # open posterior ve la versión nueva (G2)
    assert reader.read(B - 10, 20) == new[B - 10:B + 10]              # la instantánea abierta sigue leyendo la versión vieja (G4)
    st = alice.stat(p)
    evid("T2_rf3", f"x (EXISTS al repetir), r+ (solo bloque 1 cambió: {m1['blocks'][1]['block_id'][:10]}->{m2['blocks'][1]['block_id'][:10]}), "
         f"r+ fuera de EOF (400), read(off,n) cruzando 2 bloques = 2 GetBlock, a (v{m3['version']}, {len(m3['blocks'])} bloques), "
         f"w (v{st['version']}), close-to-open e instantánea: OK")
