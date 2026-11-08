"""Raft sobre mTLS.

PySyncObj sólo habla TCP plano, así que el tráfico Raft se encapsula en túneles TLS 1.3 con certificado de cliente
obligatorio (CN mds-*), firmados por la CA privada del clúster:

  PySyncObj(mds-i) -> 127.0.i.j:Pj (reenviador local) ==TLS 1.3 + mTLS==> host_j:Pj (escucha pública de mds-j)
                                                                          -> 127.0.j.j:Pj (Raft de mds-j, solo loopback)

Cada nodo se identifica ante Raft con un nombre lógico 'mds-j:Pj'; el resolver de PySyncObj se redirige en este
proceso para que ese nombre apunte a su reenviador local. Así Raft nunca escucha fuera de loopback y un nodo sin
certificado válido de la CA no puede ni conectarse (tampoco como nodo de solo lectura de PySyncObj).
"""
import logging, socket, ssl, threading
from pysyncobj import dns_resolver

log = logging.getLogger("raft-tls")


def _ctx(server, certs):
    c = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER if server else ssl.PROTOCOL_TLS_CLIENT)
    c.minimum_version = ssl.TLSVersion.TLSv1_3
    c.load_cert_chain(f"{certs}/node.pem", f"{certs}/node.key")
    c.load_verify_locations(f"{certs}/ca.pem")
    c.verify_mode = ssl.CERT_REQUIRED
    if not server:
        c.check_hostname = True
    return c


def _peer_is_mds(s):
    cert = s.getpeercert() or {}
    cn = [v for rdn in cert.get("subject", ()) for k, v in rdn if k == "commonName"]
    return bool(cn) and cn[0].startswith("mds-")


def _pump(a, b):
    try:
        while True:
            d = a.recv(65536)
            if not d:
                break
            b.sendall(d)
    except OSError:
        pass
    finally:
        for s in (a, b):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                s.close()
            except OSError:
                pass


def _join(a, b):
    threading.Thread(target=_pump, args=(a, b), daemon=True).start()
    threading.Thread(target=_pump, args=(b, a), daemon=True).start()


def _listen(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen(64)
    return s


def _serve(lsock, handler):
    def loop():
        while True:
            try:
                c, _ = lsock.accept()
            except OSError:
                return
            threading.Thread(target=handler, args=(c,), daemon=True).start()
    threading.Thread(target=loop, daemon=True).start()


def setup(cluster, me, certs):
    """cluster: {name: {'host':..., 'raft': 'host:port'}}. Devuelve (self_addr, otros, bind_addr) para SyncObj."""
    names = sorted(cluster)
    idx = {n: k + 1 for k, n in enumerate(names)}
    i = idx[me]
    port = {n: int(cluster[n]["raft"].rsplit(":", 1)[1]) for n in names}
    loop_ip = {n: f"127.0.{i}.{idx[n]}" for n in names}            # en este proceso: nombre lógico -> IP de loopback

    orig = dns_resolver.DnsCachingResolver.resolve

    def resolve(self, hostname):
        return loop_ip.get(hostname) or orig(self, hostname)
    dns_resolver.DnsCachingResolver.resolve = resolve

    srv_ctx, cli_ctx = _ctx(True, certs), _ctx(False, certs)
    internal = (loop_ip[me], port[me])

    # escucha pública TLS: acepta solo pares mds-* con certificado de la CA y reenvía al Raft local
    def inbound(c):
        try:
            c.settimeout(10)
            t = srv_ctx.wrap_socket(c, server_side=True)
            t.settimeout(None)
            if not _peer_is_mds(t):
                log.warning("conexión Raft rechazada: certificado sin CN mds-*")
                t.close()
                return
            r = socket.create_connection(internal, timeout=5)
            r.settimeout(None)
            _join(t, r)
        except (OSError, ssl.SSLError) as e:
            log.warning("handshake Raft rechazado: %s", e)
            try:
                c.close()
            except OSError:
                pass
    _serve(_listen(cluster[me]["host"], port[me]), inbound)

    # reenviadores locales hacia cada par
    for n in names:
        if n == me:
            continue
        host = cluster[n]["host"]

        def outbound(c, host=host, p=port[n]):
            try:
                raw = socket.create_connection((host, p), timeout=5)
                t = cli_ctx.wrap_socket(raw, server_hostname=host)
                t.settimeout(None)
                c.settimeout(None)
                if not _peer_is_mds(t):
                    raise ssl.SSLError("el par no presenta un certificado mds-*")
                _join(c, t)
            except (OSError, ssl.SSLError) as e:
                log.debug("túnel Raft hacia %s:%s no disponible: %s", host, p, e)
                c.close()
        _serve(_listen(loop_ip[n], port[n]), outbound)

    log.info("Raft sobre mTLS: %s escucha TLS en %s:%s; Raft interno en %s:%s", me, cluster[me]["host"], port[me], *internal)
    return f"{me}:{port[me]}", [f"{n}:{port[n]}" for n in names if n != me], f"{internal[0]}:{internal[1]}"
