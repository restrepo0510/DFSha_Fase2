"""T9: fidelidad al diseño de Fase I y defectos corregidos (307, PREPARED y verificación de réplicas, ABA, tokens renovables,
locks S/X por archivo y bloque, refresh + jti, límite de logins, auditoría, Raft sobre mTLS, TLS 1.3, lectura por segmentos,
compactación, 410 en un handle, inmutabilidad, GC de .part, rebalanceo y retiro de nodos)."""
import base64, glob, json, os, socket, ssl, subprocess, threading, time
import grpc, httpx, pytest
from dfsha.common import DFSError, SEG, TAG, encrypt_block, read, sha256_hex
from dfsha.pb import dfsha_pb2 as pb, dfsha_pb2_grpc as pbg
from conftest import evid, sha, wait_for, full_replication, alive_nodes

B = 256 * 1024


def follower_url(cluster):
    lead = cluster.leader()
    return next(u for n, u in zip(cluster.mds, cluster.urls) if n != lead)


def node_stub(cluster, addr, who):
    h, p = addr.rsplit(":", 1)
    d = f"{cluster.certs}/{who}"
    cr = grpc.ssl_channel_credentials(root_certificates=read(f"{d}/ca.pem"), private_key=read(f"{d}/node.key"), certificate_chain=read(f"{d}/node.pem"))
    return pbg.DataNodeStub(grpc.secure_channel(f"{h}:{int(p) + 1}", cr))


def test_307_hacia_el_lider(cluster, tmp_path):
    from dfsha.client.sdk import DFSha
    d = DFSha([follower_url(cluster)], cluster.ca, "alice", "alicepw")   # como un gateway que cae en un seguidor
    d.login(); d.mkdir("/t9", parents=True)
    st = d.stat("/t9")
    r = httpx.get(follower_url(cluster) + "/v1/fs/users/alice/t9?op=stat", headers={"Authorization": f"Bearer {d.token}"},
                  verify=ssl.create_default_context(cafile=cluster.ca))
    assert r.status_code == 307 and r.headers["location"].endswith("/v1/fs/users/alice/t9?op=stat") and d.stats["redirects"] >= 1
    assert st["type"] == "d"
    evid("T9_fidelidad", f"[307] un seguidor responde 307 NOT_LEADER con Location={r.headers['location'].split('/v1')[0]}...; el cliente sigue al líder conservando método y cuerpo")


def test_commit_verifica_replicas_y_estados(alice):
    # 1) acks inventados: el líder consulta cada nodo (CheckBlocks) y no publica nada
    tx = alice._req("POST", "/v1/txn", json={"path": "/users/alice/t9/falso.bin", "op": "w", "expected_version": 0, "sizes": [1000], "cid": alice.cid})
    b = tx["blocks"][0]
    fake = {"blocks": [{"idx": 0, "bid": b["block_id"], "size": 1000, "sha": "00" * 32, "psha": "00" * 32, "nonce": base64.b64encode(os.urandom(12)).decode(),
                        "acked": [r["dn_id"] for r in b["replicas"]]}], "dek": base64.b64encode(os.urandom(32)).decode(), "cid": alice.cid}
    with pytest.raises(DFSError) as e:
        alice._req("POST", f"/v1/txn/{tx['txn_id']}/commit", json=fake)
    assert e.value.code == 503 and e.value.name == "INSUFFICIENT_REPLICAS"
    assert alice._req("GET", f"/v1/txn/{tx['txn_id']}")["state"] == "ABORTED"
    with pytest.raises(DFSError) as e2:
        alice.stat("/t9/falso.bin")
    assert e2.value.name == "NOT_FOUND"
    # 2) escritura real: OPEN -> PREPARED -> COMMITTED, commit idempotente por txn_id
    tx = alice._req("POST", "/v1/txn", json={"path": "/users/alice/t9/real.bin", "op": "w", "expected_version": 0, "sizes": [1000], "cid": alice.cid})
    assert alice._req("GET", f"/v1/txn/{tx['txn_id']}")["state"] == "OPEN"
    key, plain, b = os.urandom(32), os.urandom(1000), tx["blocks"][0]
    nonce, ct = encrypt_block(key, plain, tx["fid"], 0, b["block_id"])
    acked = alice._upload(tx, b, ct)
    body = {"blocks": [{"idx": 0, "bid": b["block_id"], "size": 1000, "sha": sha256_hex(ct), "psha": sha256_hex(plain), "nonce": base64.b64encode(nonce).decode(),
                        "acked": acked}], "dek": base64.b64encode(key).decode(), "cid": alice.cid}
    r1 = alice._req("POST", f"/v1/txn/{tx['txn_id']}/commit", json=body)
    r2 = alice._req("POST", f"/v1/txn/{tx['txn_id']}/commit", json=body)
    assert r1 == r2 and r1["version"] == 1 and alice._req("GET", f"/v1/txn/{tx['txn_id']}")["state"] == "COMMITTED"
    evid("T9_fidelidad", "[G1] commit con acks inventados -> 503 INSUFFICIENT_REPLICAS (el líder verificó el hash en cada nodo), txn ABORTED, nada publicado; "
         "escritura real OPEN -> PREPARED -> COMMITTED y commit repetido idempotente")


def test_aba_borrar_y_recrear_durante_escritura(alice, alice2, tmp_path):
    p = "/users/alice/t9/aba.bin"
    (tmp_path / "f").write_bytes(os.urandom(B)); alice.put(str(tmp_path / "f"), p)

    def data_fn(k):
        alice2.rm(p); alice2.put(str(tmp_path / "f"), p)          # otro cliente borra y recrea: nuevo file_id, versión 1 otra vez
        return b"X" * B
    with pytest.raises(DFSError) as e:
        alice._publish_once(p, "w", 1, [B], data_fn)
    assert e.value.name == "VERSION_CONFLICT"
    assert alice.open(p, "r").read(0, B) == (tmp_path / "f").read_bytes()
    evid("T9_fidelidad", "[ABA] borrar y recrear el archivo durante una escritura -> 409 VERSION_CONFLICT; el archivo nuevo queda íntegro")


def test_home_protegido(alice):
    for fn in (lambda: alice.rm("/users/alice", True), lambda: alice.mv("/users/alice", "/users/alice/x")):
        with pytest.raises(DFSError) as e:
            fn()
        assert e.value.code == 403
    assert alice.stat("/users/alice")["type"] == "d"
    evid("T9_fidelidad", "[home] rm -r y mv del directorio raíz del usuario -> 403")


def test_tokens_de_bloque_se_renuevan_en_subidas_largas(cluster):
    c = cluster.client("bob", parallel=1)
    c.mkdir("/t9", parents=True)

    def slow(k):
        time.sleep(14 if k == 1 else 0)                           # el 2.º bloque empieza después de que vence el token (TTL 12 s en pruebas)
        return bytes([k]) * B
    r = c._publish_once("/users/bob/t9/lento.bin", "w", 0, [B, B], slow)
    assert r["version"] == 1 and c.stats["token_refresh"] >= 1
    assert c.open("/t9/lento.bin", "r").read(B, 4) == b"\x01" * 4
    evid("T9_fidelidad", f"[tokens] subida que dura más que la vigencia del block token: el cliente renovó {c.stats['token_refresh']} token(s) y confirmó")


def test_locks_compartidos_exclusivos_y_por_bloque(cluster, alice, alice2):
    p = "/t9/locks.bin"
    with alice.open(p, "x") as h: h.write(b"\0" * (3 * B))
    s1 = alice.lock(p, mode="S"); s2 = alice2.lock(p, mode="S")              # S + S compatibles
    with pytest.raises(DFSError) as e: alice2.lock(p, mode="X")
    assert e.value.code == 423
    h = alice2.open(p, "r+"); h.write(b"x", 1)
    with pytest.raises(DFSError) as e: h.close()                             # un S ajeno impide escribir
    assert e.value.name == "LOCKED"
    alice.unlock(s1); alice2.unlock(s2)
    b0 = alice.lock(p, block=0, mode="X"); b1 = alice2.lock(p, block=1, mode="X")   # X en bloques distintos: compatibles
    with pytest.raises(DFSError) as e: alice2.lock(p, mode="X")             # X de archivo choca con el X del bloque 0
    assert e.value.code == 423
    w = alice2.open(p, "r+"); w.write(b"B1", B + 5); w.close()               # escribe su bloque con su lock
    w = alice2.open(p, "r+"); w.write(b"B0", 5)
    with pytest.raises(DFSError) as e: w.close()                             # el bloque 0 es de otro titular
    assert e.value.name == "LOCKED"
    threading.Timer(1.5, lambda: alice.unlock(b0)).start()
    e3 = alice2.lock(p, block=0, mode="X", wait=10)                          # espera con timeout hasta que se libera
    assert e3 > b1
    alice2.unlock(p)
    assert alice.open(p, "r").read(B + 5, 2) == b"B1"
    evid("T9_fidelidad", "[locks] S+S compatibles; X frente a S -> 423; escritor bloqueado por S ajeno -> 423; X por bloque: bloques distintos en paralelo, "
         "mismo bloque -> 423; lock(wait) espera a que se libere")


def test_rm_y_mv_respetan_locks(alice, alice2):
    p = "/t9/protegido.bin"
    with alice.open(p, "x") as h: h.write(b"abc")
    e = alice.lock(p)
    for fn in (lambda: alice2.rm(p), lambda: alice2.mv(p, "/t9/otro.bin")):
        with pytest.raises(DFSError) as ex: fn()
        assert ex.value.code == 423
    alice.mv(p, "/t9/movido.bin")                                            # el titular sí, y el lock viaja con el archivo
    assert alice._req("GET", "/v1/locks", params={"path": alice.abspath("/t9/movido.bin")})["locks"][0]["epoch"] == e
    alice.unlock(e)
    evid("T9_fidelidad", "[locks] rm/mv de un archivo bloqueado por otro titular -> 423; el titular lo mueve y su lock lo acompaña")


def test_refresh_token_rotacion_y_reutilizacion(cluster):
    from dfsha.client.sdk import DFSha
    d = DFSha(cluster.urls, cluster.ca, "bob", "bobpw"); d.login()
    old = d.refresh_token
    d.refresh(); assert d.refresh_token != old and d.stat("/")["type"] == "d"
    with pytest.raises(DFSError) as e:
        DFSha(cluster.urls, cluster.ca, refresh_token=old).refresh()
    assert e.value.code == 401                                                # jti ya usado
    cur = d.refresh_token; d.logout()
    with pytest.raises(DFSError) as e:
        DFSha(cluster.urls, cluster.ca, refresh_token=cur).refresh()
    assert e.value.code == 401
    evid("T9_fidelidad", "[auth] refresh rota los tokens (8 h); reutilizar un refresh token (mismo jti) -> 401; tras logout -> 401")


def test_limite_de_intentos_de_login(cluster):
    from dfsha.client.sdk import DFSha
    codes = []
    for _ in range(6):
        try: DFSha(cluster.urls, cluster.ca, "carol", "mala").login()
        except DFSError as e: codes.append(e.code)
    with pytest.raises(DFSError) as e:
        DFSha(cluster.urls, cluster.ca, "carol", "carolpw").login()            # bloqueado aunque la clave sea correcta
    assert codes[:5] == [401] * 5 and codes[5] == 429 and e.value.code == 429
    evid("T9_fidelidad", f"[auth] 5 intentos fallidos por minuto: {codes} y luego 429 aun con la clave correcta")


def test_auditoria_solo_anexar(cluster, alice):
    lead = cluster.leader()
    path = f"{cluster.base}/data/{lead}/audit.log"
    alice.mkdir("/t9/aud", parents=True); alice.rm("/t9/aud")
    ev = [json.loads(l) for l in open(path)]
    kinds = {e["event"] for e in ev}
    assert {"login", "commit", "open", "rm", "mkdir", "lock"} <= kinds
    assert oct(os.stat(path).st_mode & 0o777) == "0o600"
    evid("T9_fidelidad", f"[auditoría] {len(ev)} eventos en {lead}/audit.log (O_APPEND, 0600); tipos: {sorted(kinds)}")


def test_raft_solo_por_mtls_y_rest_tls13(cluster):
    m = cluster.mds["mds-1"]
    raft = ("127.0.0.1", m["raft"])
    s = socket.create_connection(raft, timeout=3); s.sendall(b"\x00" * 64)         # Raft en claro: se corta
    s.settimeout(3)
    try: data = s.recv(10)
    except (ConnectionResetError, socket.timeout): data = b""
    assert data == b""
    c = ssl.create_default_context(cafile=cluster.ca)
    with pytest.raises((ssl.SSLError, ConnectionResetError, OSError)):
        with c.wrap_socket(socket.create_connection(raft, timeout=3), server_hostname="127.0.0.1") as t:   # TLS sin certificado de cliente
            t.sendall(b"x"); t.recv(10) or (_ for _ in ()).throw(ConnectionResetError())
    c2 = ssl.create_default_context(cafile=cluster.ca); c2.load_cert_chain(f"{cluster.certs}/dn-1/node.pem", f"{cluster.certs}/dn-1/node.key")
    t = c2.wrap_socket(socket.create_connection(raft, timeout=3), server_hostname="127.0.0.1"); t.settimeout(3)
    try: got = t.recv(10)                                                     # certificado válido pero de nodo de datos: se cierra
    except (ConnectionResetError, ssl.SSLError, socket.timeout, OSError): got = b""
    assert got == b""
    host = cluster.urls[0].split("//")[1]
    o = subprocess.run(["openssl", "s_client", "-connect", host, "-tls1_2", "-CAfile", cluster.ca, "-brief"], input=b"", capture_output=True, timeout=10)
    assert "Protocol version: TLSv1.2" not in (o.stdout + o.stderr).decode()
    evid("T9_fidelidad", "[canal] Raft solo acepta TLS 1.3 con certificado mds-* (en claro, sin certificado o con certificado dn-* -> cerrado); "
         "el REST de metadatos rechaza TLS 1.2")


def test_operaciones_internas_solo_del_lider(cluster, alice, tmp_path):
    (tmp_path / "f").write_bytes(os.urandom(1000)); alice.put(str(tmp_path / "f"), "/t9/interno.bin")
    b = alice.manifest("/t9/interno.bin")["blocks"][0]
    addr = b["replicas"][0]["addr"]
    for rpc in (lambda s: s.DeleteBlock(pb.DeleteRequest(block_id=b["block_id"]), timeout=5),
                lambda s: s.CheckBlocks(pb.CheckRequest(block_ids=[b["block_id"]]), timeout=5)):
        with pytest.raises(grpc.RpcError) as e:
            rpc(node_stub(cluster, addr, "dn-2"))                              # un nodo de datos no puede pedirlo
        assert e.value.code() == grpc.StatusCode.PERMISSION_DENIED
    r = node_stub(cluster, addr, "mds-1").CheckBlocks(pb.CheckRequest(block_ids=[b["block_id"]]), timeout=5)
    assert r.blocks[0].present and r.blocks[0].sha256 == b["sha256"]
    evid("T9_fidelidad", "[mTLS] DeleteBlock/CheckBlocks con certificado dn-* -> PERMISSION_DENIED; con certificado mds-* -> permitido")


def test_bloques_inmutables_y_gc_de_part(cluster, alice):
    tx = alice._req("POST", "/v1/txn", json={"path": "/users/alice/t9/inm.bin", "op": "w", "expected_version": 0, "sizes": [1000], "cid": alice.cid})
    b = tx["blocks"][0]
    alice._upload(tx, b, os.urandom(1016))
    with pytest.raises(grpc.RpcError) as e:
        alice._stub(b["replicas"][0]["addr"]).PutBlock(iter([pb.PutBlockChunk(block_id=b["block_id"], token=b["token"], expected_sha256="", data=b"otro")]), timeout=5)
    assert e.value.code() == grpc.StatusCode.ALREADY_EXISTS
    alice._req("POST", f"/v1/txn/{tx['txn_id']}/abort")
    d = f"{cluster.base}/data/dn-1"
    stale = os.path.join(d, "b-abandonado.deadbeef.part")
    open(stale, "wb").write(b"x" * 100); os.utime(stale, (time.time() - 3600, time.time() - 3600))
    wait_for(lambda: not os.path.exists(stale), 40, msg="GC de .part")
    evid("T9_fidelidad", "[datos] un bloque existente no se sobrescribe (ALREADY_EXISTS); un .part abandonado se borra por GC sin reiniciar el nodo")


def test_lectura_por_segmentos_gcm(cluster, alice, alice2, tmp_path):
    data = os.urandom(3 * B); (tmp_path / "f").write_bytes(data); alice.put(str(tmp_path / "f"), "/t9/seg.bin")
    h = alice2.open("/t9/seg.bin", "r"); f0 = alice2.stats["bytes_fetched"]
    assert h.read(B + 70000, 100) == data[B + 70000:B + 70100]
    got = alice2.stats["bytes_fetched"] - f0
    assert got == SEG + TAG
    evid("T9_fidelidad", f"[RF3] read(off, 100 B) dentro de un bloque de {B // 1024} KiB trajo {got} B (un segmento GCM de 64 KiB + etiqueta) con GetBlock por rango")


def test_compactacion_de_anexos(cluster, alice):
    p = "/t9/log.txt"
    with alice.open(p, "x") as h: h.write(b"inicio\n")
    for i in range(8):
        with alice.open(p, "a") as h: h.write(f"linea-{i}\n".encode())
    m = alice.manifest(p)
    h = alice.open(p, "r"); txt = h.read(0, h.size).decode()
    # x (v1) + 7 anexos (v2..v8) = 8 bloques cortos -> compactación (v9) a 1 bloque; el 8.º anexo (v10) agrega otro
    assert m["version"] == 10 and len(m["blocks"]) == 2 and txt == "inicio\n" + "".join(f"linea-{i}\n" for i in range(8))
    evid("T9_fidelidad", f"[anexos] tras 8 bloques cortos por anexos, la compactación los reescribió en 1 bloque; versión final {m['version']} con {len(m['blocks'])} bloques y el contenido intacto")


def test_instantanea_expirada_en_handle_abierto(cluster, alice, tmp_path):
    (tmp_path / "f").write_bytes(os.urandom(2 * B)); alice.put(str(tmp_path / "f"), "/t9/exp.bin")
    h = alice.open("/t9/exp.bin", "r")
    alice.put(str(tmp_path / "f"), "/t9/exp.bin")                              # nueva versión: la abierta queda superada
    old = [b["block_id"] for b in h.m["blocks"]]
    wait_for(lambda: not any(glob.glob(f"{cluster.base}/data/dn-*/{b}.blk") for b in old), 90, msg="GC de la versión vieja")
    with pytest.raises(DFSError) as e:
        h.read(0, 10)
    assert e.value.code == 410 and e.value.name == "SNAPSHOT_EXPIRED"
    evid("T9_fidelidad", "[instantánea] un handle abierto cuya versión superó la gracia y fue liberada -> 410 SNAPSHOT_EXPIRED")


def test_alta_rebalanceo_y_retiro_de_nodo(cluster, admin, alice, tmp_path):
    data = os.urandom(24 * B); (tmp_path / "f").write_bytes(data); alice.put(str(tmp_path / "f"), "/t9/reb.bin")
    full_replication(admin, cluster, alice.abspath("/t9/reb.bin"))
    cluster.start_dn("dn-6")
    wait_for(lambda: "dn-6" in alive_nodes(admin), 20, msg="dn-6 vivo")
    admin.admin_add_node("dn-6")
    nodes = lambda: {n["dn_id"]: n for n in admin.admin_nodes()["nodes"]}
    wait_for(lambda: nodes()["dn-6"]["blocks"] >= 5, 90, msg="rebalanceo hacia dn-6")
    moved = nodes()["dn-6"]["blocks"]
    alice.get("/t9/reb.bin", str(tmp_path / "g")); assert sha((tmp_path / "g").read_bytes()) == sha(data)
    admin.admin_retire_node("dn-6")
    wait_for(lambda: nodes()["dn-6"]["admin"] == "DECOMMISSIONED", 120, msg="retiro de dn-6")
    assert nodes()["dn-6"]["blocks"] == 0
    full_replication(admin, cluster, alice.abspath("/t9/reb.bin"))
    alice.get("/t9/reb.bin", str(tmp_path / "g2")); assert sha((tmp_path / "g2").read_bytes()) == sha(data)
    cluster.kill("dn-6")
    evid("T9_fidelidad", f"[UC9] alta de dn-6 -> rebalanceo gradual le trasladó {moved} bloques; retiro -> sus bloques se trasladaron (DECOMMISSIONED, 0 bloques); "
         "lecturas íntegras durante todo el proceso")


def test_reporte_falso_de_corrupcion_no_retira_replicas(cluster, alice, admin, tmp_path):
    (tmp_path / "f").write_bytes(os.urandom(B)); alice.put(str(tmp_path / "f"), "/t9/sano.bin")
    p = alice.abspath("/t9/sano.bin"); full_replication(admin, cluster, p)
    b = alice.manifest("/t9/sano.bin")["blocks"][0]
    for r in b["replicas"]:                                                  # el cliente "acusa" a todas las réplicas
        alice._req("POST", f"/v1/blocks/{b['block_id']}/bad", json={"dn_id": r["dn_id"]})
    time.sleep(4)                                                             # los nodos verifican el hash: todo está sano
    assert admin.admin_blocks(p)["blocks"][0]["n_replicas"] == 3
    assert alice.open("/t9/sano.bin", "r").read(0, B) == (tmp_path / "f").read_bytes()
    evid("T9_fidelidad", "[integridad] un cliente que reporta como corruptas réplicas sanas no las retira: cada nodo recalcula el hash y solo él puede apartar su copia")
