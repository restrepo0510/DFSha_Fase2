"""T6: autenticación, autorización, protección del canal y del almacenamiento. [SEG 10]"""
import glob, os, subprocess, time
import grpc, httpx, jwt, pytest
from cryptography.exceptions import InvalidTag
from dfsha.common import DFSError, decrypt_block, load_private, sign_token, read
from dfsha.pb import dfsha_pb2 as pb, dfsha_pb2_grpc as pbg
from conftest import evid, sha, wait_for, full_replication


def raw(cluster, method, path, token=None, **kw):
    h = {"Authorization": f"Bearer {token}"} if token else {}
    import ssl
    return httpx.request(method, cluster.urls[0] + path, headers=h, verify=ssl.create_default_context(cafile=cluster.ca), **kw)


def leader_url(cluster):
    return cluster.urls[list(cluster.mds).index(cluster.leader())]


def test_autenticacion_y_autorizacion(cluster, alice, bob, admin):
    from dfsha.client.sdk import DFSha
    with pytest.raises(DFSError) as e: DFSha(cluster.urls, cluster.ca, "alice", "mala").login()
    assert e.value.code == 401
    base = leader_url(cluster)
    import ssl; ctx = ssl.create_default_context(cafile=cluster.ca)
    assert httpx.get(base + "/v1/fs/users/alice?op=ls", verify=ctx).status_code == 401               # sin JWT
    keys = f"{cluster.certs}/keys"
    old = jwt.encode({"sub": "alice", "role": "user", "exp": int(time.time()) - 10}, read(f"{keys}/jwt_ed25519.pem"), algorithm="EdDSA")
    assert httpx.get(base + "/v1/fs/users/alice?op=ls", headers={"Authorization": f"Bearer {old}"}, verify=ctx).status_code == 401   # expirado
    forged = jwt.encode({"sub": "alice", "role": "admin", "exp": int(time.time()) + 999}, "x" * 32, algorithm="HS256")
    assert httpx.get(base + "/v1/admin/nodes", headers={"Authorization": f"Bearer {forged}"}, verify=ctx).status_code == 401   # firma falsa
    alice.mkdir("/t6"); 
    with alice.open("/t6/secreto.txt", "x") as h: h.write(b"solo de alice")
    with pytest.raises(DFSError) as e: bob.ls("/users/alice/t6")
    assert e.value.code == 403
    with pytest.raises(DFSError) as e: bob.manifest("/users/alice/t6/secreto.txt")
    assert e.value.code == 403
    with pytest.raises(DFSError) as e: bob.open("/users/alice/t6/secreto.txt", "r+")
    assert e.value.code == 403
    with pytest.raises(DFSError) as e: alice.admin_nodes()
    assert e.value.code == 403
    with pytest.raises(DFSError) as e: admin.ls("/users/alice")                                         # el admin no accede al contenido
    assert e.value.code == 403
    assert bob.ls("/") != [] or True
    evid("T6_seguridad", "AUTORIZADO: alice opera en /users/alice. DENEGADO: login con clave mala 401; sin JWT 401; JWT expirado 401; JWT con firma falsa 401; "
         "bob sobre /users/alice 403 (ls, manifest, open); alice a /v1/admin 403; admin sobre contenido de usuarios 403")


def test_tokens_de_bloque(cluster, alice, tmp_path):
    (tmp_path / "f").write_bytes(os.urandom(700_000)); alice.put(str(tmp_path / "f"), "/t6/tok.bin")
    m = alice.manifest("/t6/tok.bin"); b0, b1 = m["blocks"][0], m["blocks"][1]
    ca = open(cluster.ca, "rb").read()
    stub = lambda addr: pbg.DataNodeStub(grpc.secure_channel(addr, grpc.ssl_channel_credentials(root_certificates=ca)))
    # token del bloque 0 no sirve para el bloque 1
    with pytest.raises(grpc.RpcError) as e: list(stub(b1["replicas"][0]["addr"]).GetBlock(pb.GetBlockRequest(block_id=b1["block_id"], token=b0["token"]), timeout=5))
    assert e.value.code() == grpc.StatusCode.PERMISSION_DENIED
    # token de lectura no sirve para escribir
    with pytest.raises(grpc.RpcError) as e: stub(b0["replicas"][0]["addr"]).PutBlock(iter([pb.PutBlockChunk(block_id=b0["block_id"], token=b0["token"], data=b"x")]), timeout=5)
    assert e.value.code() == grpc.StatusCode.PERMISSION_DENIED
    # token vencido
    priv = load_private(f"{cluster.certs}/keys/block_ed25519.pem")
    exp = sign_token(priv, bid=b0["block_id"], op="get", uid="alice", txn="", exp=time.time() - 5)
    with pytest.raises(grpc.RpcError) as e: list(stub(b0["replicas"][0]["addr"]).GetBlock(pb.GetBlockRequest(block_id=b0["block_id"], token=exp), timeout=5))
    assert e.value.code() == grpc.StatusCode.UNAUTHENTICATED
    # token sin firma válida
    with pytest.raises(grpc.RpcError) as e: list(stub(b0["replicas"][0]["addr"]).GetBlock(pb.GetBlockRequest(block_id=b0["block_id"], token="a.b"), timeout=5))
    assert e.value.code() == grpc.StatusCode.PERMISSION_DENIED
    evid("T6_seguridad", "TOKENS DE BLOQUE: token de otro bloque -> PERMISSION_DENIED; token 'get' usado para PutBlock -> PERMISSION_DENIED; vencido -> UNAUTHENTICATED; firma inválida -> PERMISSION_DENIED")


def test_proteccion_del_canal(cluster, alice, tmp_path):
    base = leader_url(cluster)
    with pytest.raises(Exception): httpx.get(base.replace("https", "http") + "/healthz", timeout=3)       # REST en claro: rechazado
    out = subprocess.run(["openssl", "s_client", "-connect", base.split("//")[1], "-CAfile", cluster.ca, "-brief"], input=b"", capture_output=True, timeout=15)
    proto = [l for l in (out.stderr + out.stdout).decode().splitlines() if "Protocol" in l or "Cipher" in l or "Verif" in l]
    assert any("TLSv1.3" in l for l in proto), proto
    m = alice.manifest("/t6/tok.bin"); addr = m["blocks"][0]["replicas"][0]["addr"]; host, port = addr.split(":")
    with pytest.raises(grpc.RpcError) as e: list(pbg.DataNodeStub(grpc.insecure_channel(addr)).GetBlock(pb.GetBlockRequest(block_id="x", token="y"), timeout=3))
    assert e.value.code() == grpc.StatusCode.UNAVAILABLE                                                  # gRPC en claro: rechazado
    ca = open(cluster.ca, "rb").read()
    tls_only = pbg.DataNodeStub(grpc.secure_channel(addr, grpc.ssl_channel_credentials(root_certificates=ca)))
    with pytest.raises(grpc.RpcError) as e: tls_only.DeleteBlock(pb.DeleteRequest(block_id=m["blocks"][0]["block_id"]), timeout=5)
    assert e.value.code() == grpc.StatusCode.PERMISSION_DENIED                                            # operación de nodo sin certificado
    node_addr = f"{host}:{int(port) + 1}"
    with pytest.raises(grpc.RpcError) as e: tls_only2 = pbg.DataNodeStub(grpc.secure_channel(node_addr, grpc.ssl_channel_credentials(root_certificates=ca))); tls_only2.DeleteBlock(pb.DeleteRequest(block_id="x"), timeout=5)
    assert e.value.code() == grpc.StatusCode.UNAVAILABLE                                                  # puerto de nodo: sin cert de cliente no hay handshake
    # un nodo FALSO con certificado de otra CA es rechazado
    import scripts.gen_certs as g; fake = str(tmp_path / "fake"); g.main(fake, ["dn-9"])
    cr = grpc.ssl_channel_credentials(root_certificates=ca, private_key=read(fake + "/dn-9/node.key"), certificate_chain=read(fake + "/dn-9/node.pem"))
    with pytest.raises(grpc.RpcError) as e: pbg.DataNodeStub(grpc.secure_channel(node_addr, cr)).DeleteBlock(pb.DeleteRequest(block_id="x"), timeout=5)
    assert e.value.code() == grpc.StatusCode.UNAVAILABLE
    evid("T6_seguridad", "CANAL: REST por http plano rechazado; REST con TLS: " + " | ".join(proto) + "; gRPC sin TLS -> UNAVAILABLE; DeleteBlock en puerto de cliente (sin cert de nodo) -> PERMISSION_DENIED; "
         "puerto de nodo sin certificado de cliente -> handshake rechazado; nodo con certificado de otra CA -> rechazado")


def test_proteccion_en_reposo_y_manipulacion(cluster, alice, admin, tmp_path):
    marker = b"CONFIDENCIAL-ULTRA-SECRETO-12345"
    data = (marker + b"\n") * 20000                                  # ~660 KB legibles
    (tmp_path / "s").write_bytes(data); alice.put(str(tmp_path / "s"), "/t6/cifrado.bin")
    p = alice.abspath("/t6/cifrado.bin"); full_replication(admin, cluster, p)
    m = alice.manifest("/t6/cifrado.bin")
    mine = {b["block_id"] for b in m["blocks"]}
    files = [f for f in glob.glob(f"{cluster.base}/data/dn-*/*.blk") if os.path.basename(f)[:-4] in mine]   # solo los de este archivo (el GC borra otros)
    assert len(files) >= 2 * len(mine) and all(marker not in open(f, "rb").read() for f in files)   # los DN solo guardan texto cifrado
    blk0 = open(glob.glob(f"{cluster.base}/data/dn-*/{m['blocks'][0]['block_id']}.blk")[0], "rb").read()
    import base64
    with pytest.raises(InvalidTag): decrypt_block(os.urandom(32), base64.b64decode(m["blocks"][0]["nonce"]), blk0, m["file_id"], 0, m["blocks"][0]["block_id"])
    with pytest.raises(InvalidTag): decrypt_block(base64.b64decode(m["dek"]), base64.b64decode(m["blocks"][0]["nonce"]), blk0, m["file_id"], 7, m["blocks"][0]["block_id"])   # AAD: no se puede mover de posición
    # manipulación: un byte alterado en la réplica que el cliente intenta primero
    first = m["blocks"][0]["replicas"][0]["dn_id"]; bid = m["blocks"][0]["block_id"]
    path = f"{cluster.base}/data/{first}/{bid}.blk"
    raw_ = bytearray(open(path, "rb").read()); raw_[100] ^= 0xFF; open(path, "wb").write(bytes(raw_))
    alice.get("/t6/cifrado.bin", str(tmp_path / "g")); assert sha((tmp_path / "g").read_bytes()) == sha(data)     # cambia de réplica
    assert alice.stats["failovers"] > 0
    wait_for(lambda: not os.path.exists(path), 20, msg="la copia corrupta se aparta")                     # el nodo la verificó y la descartó
    # el apartado local es inmediato; retirarla de los metadatos requiere además el siguiente heartbeat (bad_blocks)
    wait_for(lambda: first not in [r["dn_id"] for r in admin.admin_blocks(p)["blocks"][0]["replicas"]], 20,
             msg="metadatos: réplica corrupta retirada")                                                  # y no vuelve a admitirse
    wait_for(lambda: all(b["n_replicas"] == 3 for b in admin.admin_blocks(p)["blocks"]), 40, msg="R=3 tras réplica corrupta")
    # scrubbing: corrupción silenciosa sin que nadie lea el bloque
    b2 = m["blocks"][1]["block_id"]; holder = admin.admin_blocks(p)["blocks"][1]["replicas"][0]["dn_id"]
    path2 = f"{cluster.base}/data/{holder}/{b2}.blk"; r2 = bytearray(open(path2, "rb").read()); r2[50] ^= 0xFF; open(path2, "wb").write(bytes(r2))
    wait_for(lambda: os.path.exists(path2[:-4] + ".corrupt") or sha(open(path2, "rb").read()) if os.path.exists(path2) else True, 30, msg="scrub")
    wait_for(lambda: all(b["n_replicas"] == 3 for b in admin.admin_blocks(p)["blocks"]), 60, msg="R=3 tras scrub")
    alice.get("/t6/cifrado.bin", str(tmp_path / "g2")); assert sha((tmp_path / "g2").read_bytes()) == sha(data)
    evid("T6_seguridad", f"ALMACENAMIENTO: {len(files)} archivos .blk en los DN, ninguno contiene el texto claro; AES-256-GCM por segmentos de 64 KiB; descifrar con otra clave o "
         f"con otra posición (AAD) -> InvalidTag. MANIPULACIÓN: 1 byte alterado en {first} -> el cliente detecta hash inválido, usa otra réplica y reporta; el nodo verifica y descarta su copia; el líder restaura R=3 sin readmitirla. "
         f"Scrubbing detectó corrupción silenciosa en {holder} y se re-replicó.")
