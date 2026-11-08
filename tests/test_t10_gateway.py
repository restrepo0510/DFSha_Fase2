"""T10: el gateway NGINX real (deploy/aws/nginx.conf) delante de los 3 MDS: TLS 1.3, el 307 de un seguidor llega al cliente
y este sigue al líder; un 503 real (sin réplicas suficientes) llega intacto; caída del líder detrás del gateway. [DIST 12][HA 15]"""
import os, re, shutil, signal, ssl, subprocess, time
import httpx, pytest
from conftest import evid, sha, wait_for, alive_nodes, ROOT

pytestmark = pytest.mark.skipif(not shutil.which("nginx"), reason="nginx no instalado")


@pytest.fixture(scope="module")
def gateway(cluster, tmp_path_factory):
    d = tmp_path_factory.mktemp("nginx")
    conf = open(os.path.join(ROOT, "deploy/aws/nginx.conf")).read()
    ports = [p["http"] for p in cluster.mds.values()]
    conf = conf.replace("server MDS1_IP:8443; server MDS2_IP:8443; server MDS3_IP:8443;", " ".join(f"server 127.0.0.1:{p};" for p in ports))
    conf = conf.replace("listen 443 ssl;", "listen 127.0.0.1:18443 ssl;").replace("/etc/nginx/certs/node.pem", f"{cluster.certs}/gateway/node.pem")
    conf = conf.replace("/etc/nginx/certs/node.key", f"{cluster.certs}/gateway/node.key").replace("/etc/nginx/certs/ca.pem", cluster.ca)
    conf = conf.replace("events {}", f"pid {d}/nginx.pid; error_log {d}/error.log; events {{}}")
    conf = conf.replace("http {", f"http {{\n  access_log {d}/access.log; client_body_temp_path {d}/body; proxy_temp_path {d}/proxy;")
    open(d / "nginx.conf", "w").write(conf)
    p = subprocess.Popen(["nginx", "-c", str(d / "nginx.conf"), "-p", str(d), "-g", "daemon off;"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1)
    assert p.poll() is None, open(d / "error.log").read()
    yield "https://127.0.0.1:18443"
    p.send_signal(signal.SIGTERM); p.wait()


def test_gateway_307_y_errores_reales(cluster, gateway, tmp_path):
    from dfsha.client.sdk import DFSha
    d = DFSha([gateway], cluster.ca, "alice", "alicepw"); d.login()
    data = os.urandom(700_000); (tmp_path / "f").write_bytes(data)
    d.mkdir("/t10", parents=True); d.put(str(tmp_path / "f"), "/t10/gw.bin"); d.get("/t10/gw.bin", str(tmp_path / "g"))
    assert sha((tmp_path / "g").read_bytes()) == sha(data)
    ctx = ssl.create_default_context(cafile=cluster.ca)
    def get():
        for _ in range(20):                                   # un 503 UNAVAILABLE (elección en curso) es reintentable, como en el SDK
            r = httpx.get(gateway + "/v1/fs/users/alice/t10?op=stat", headers={"Authorization": f"Bearer {d.token}"}, verify=ctx)
            if r.status_code != 503: return r
            time.sleep(0.2)
        return r
    resp = [get() for _ in range(9)]
    statuses = {r.status_code for r in resp}
    assert statuses <= {200, 307} and 307 in statuses, [(r.status_code, r.text[:200]) for r in resp]   # los seguidores devuelven 307 hacia el líder
    o = subprocess.run(["openssl", "s_client", "-connect", "127.0.0.1:18443", "-tls1_2", "-CAfile", cluster.ca, "-brief"], input=b"", capture_output=True, timeout=10)
    assert "Protocol version: TLSv1.2" not in (o.stdout + o.stderr).decode()
    evid("T10_gateway", f"NGINX (deploy/aws/nginx.conf) delante de 3 MDS: put/get por meta.dfsha.internal con hash idéntico; respuestas directas {sorted(statuses)} "
         f"(307 desde seguidores, el cliente siguió {d.stats['redirects']} redirecciones); TLS 1.2 rechazado")


def test_gateway_503_real_y_caida_del_lider(cluster, gateway, tmp_path):
    from dfsha.client.sdk import DFSha
    from dfsha.common import DFSError
    d = DFSha([gateway], cluster.ca, "alice", "alicepw"); d.login()
    (tmp_path / "f").write_bytes(os.urandom(300_000))
    old = cluster.leader(); cluster.kill(old); t0 = time.time()
    d.put(str(tmp_path / "f"), "/t10/tras_caida.bin"); dt = time.time() - t0      # el gateway y el cliente encuentran al nuevo líder
    cluster.start_mds(old); time.sleep(4)
    adm = DFSha([gateway], cluster.ca, "admin", "adminpw"); adm.login()
    victims = [n for n in alive_nodes(adm)][1:]
    for n in victims: cluster.kill(n)
    wait_for(lambda: len(alive_nodes(adm)) <= 1, 30, msg="1 nodo vivo")
    with pytest.raises(DFSError) as e:
        d.put(str(tmp_path / "f"), "/t10/sin_replicas.bin")
    assert e.value.code == 503 and e.value.name == "INSUFFICIENT_REPLICAS"        # no se convierte en NOT_LEADER ni se reintenta el POST
    for n in victims: cluster.start_dn(n)
    wait_for(lambda: len(alive_nodes(adm)) >= 5, 40, msg="nodos de vuelta")
    evid("T10_gateway", f"caída del líder {old} detrás del gateway: put exitoso en {dt:.2f}s; con 1 nodo vivo el 503 INSUFFICIENT_REPLICAS llega intacto al cliente")
