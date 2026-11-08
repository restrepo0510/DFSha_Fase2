#!/usr/bin/env python3
"""Genera la CA privada, certificados por nodo (mTLS) y claves de firma/KEK.
Uso: gen_certs.py OUT_DIR nodo[:san1,san2] ...   (SAN por defecto: 127.0.0.1,localhost)
Salida: OUT/ca.pem, OUT/<nodo>/{ca.pem,node.pem,node.key}, OUT/keys/ (MDS) y OUT/keys-dn/ (solo clave pública de bloques)."""
import datetime as dt, ipaddress, os, secrets, sys
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.x509.oid import NameOID

PEM = serialization.Encoding.PEM
NOENC = serialization.NoEncryption()


def write(path, data, mode=0o600):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)
    os.chmod(path, mode)


def name(cn):
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def san_list(s):
    out = []
    for x in s.split(","):
        try:
            out.append(x509.IPAddress(ipaddress.ip_address(x)))
        except ValueError:
            out.append(x509.DNSName(x))
    return out


def main(out, specs):
    now = dt.datetime.now(dt.timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca = (x509.CertificateBuilder().subject_name(name("dfsha-ca")).issuer_name(name("dfsha-ca")).public_key(ca_key.public_key())
          .serial_number(x509.random_serial_number()).not_valid_before(now - dt.timedelta(minutes=5)).not_valid_after(now + dt.timedelta(days=365))
          .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
          .add_extension(x509.KeyUsage(digital_signature=False, content_commitment=False, key_encipherment=False, data_encipherment=False, key_agreement=False, key_cert_sign=True, crl_sign=True, encipher_only=False, decipher_only=False), critical=True)
          .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
          .sign(ca_key, hashes.SHA256()))
    ca_pem = ca.public_bytes(PEM)
    write(f"{out}/ca.pem", ca_pem, 0o644)
    for spec in specs:
        cn, _, sans = spec.partition(":")
        k = ec.generate_private_key(ec.SECP256R1())
        cert = (x509.CertificateBuilder().subject_name(name(cn)).issuer_name(ca.subject).public_key(k.public_key())
                .serial_number(x509.random_serial_number()).not_valid_before(now - dt.timedelta(minutes=5)).not_valid_after(now + dt.timedelta(days=90))
                .add_extension(x509.SubjectAlternativeName(san_list(sans or "127.0.0.1,localhost")), critical=False)
                .add_extension(x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH, x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
                .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
                .add_extension(x509.SubjectKeyIdentifier.from_public_key(k.public_key()), critical=False)
                .sign(ca_key, hashes.SHA256()))
        write(f"{out}/{cn}/node.pem", cert.public_bytes(PEM), 0o644)
        write(f"{out}/{cn}/node.key", k.private_bytes(PEM, serialization.PrivateFormat.PKCS8, NOENC))
        write(f"{out}/{cn}/ca.pem", ca_pem, 0o644)
    # claves de aplicación
    for n in ("jwt", "block"):
        k = Ed25519PrivateKey.generate()
        write(f"{out}/keys/{n}_ed25519.pem", k.private_bytes(PEM, serialization.PrivateFormat.PKCS8, NOENC))
        pub = k.public_key().public_bytes(PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        write(f"{out}/keys/{n}_ed25519.pub", pub, 0o644)
        if n == "block":
            write(f"{out}/keys-dn/block_ed25519.pub", pub, 0o644)
    write(f"{out}/keys/kek.hex", secrets.token_hex(32).encode(), 0o400)   # KEK maestra: archivo 0400
    print("certificados y claves en", out)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2:])
