# Despliegue en AWS Academy y guion de demostración

> **Despliegue guiado en tus instancias:** ver `docs/DESPLIEGUE_PASO_A_PASO.md` (`bash deploy/aws/deploy_all.sh`).

> Todo lo de este repositorio se ejecutó con procesos reales que se hablan por TCP con TLS/mTLS (`tests/`, incluido el gateway
> NGINX real en T10). Lo único que no se pudo ejecutar fuera de AWS son las VM y Docker: hagan un ensayo completo en el
> laboratorio y ajusten lo que difiera (tipos de instancia permitidos, máximo de instancias, IP).

## Topología (9 VM, Ubuntu 22.04/24.04, Docker)
| VM | Cantidad | Tipo sugerido | Rol | AZ |
|---|---|---|---|---|
| mds-1..3 | 3 | t3.small/medium | MDS (Raft sobre mTLS + REST + gRPC meta) | a, b, c |
| dn-1..5 | 5 | t3.medium + EBS gp3 30 GB cifrado | Nodos de datos | a, b, c, a, b |
| gateway/bastión | 1 | t3.medium | NGINX `meta.dfsha.internal`, cliente, benchmarks | a (IP elástica) |

Si el laboratorio limita a menos instancias, usar 4 DN y levantar el 5.º (y el 6.º/7.º de E9) como contenedores extra en VM de datos existentes, cada uno con su `DN_ID`, `DN_PORT` y directorio propio. Si solo hay 2 AZ se sigue cumpliendo "≥ 2 zonas".

## Puertos y grupos de seguridad
| Puerto | Uso | Origen permitido |
|---|---|---|
| 22 | SSH | solo su IP / bastión |
| 443 | NGINX (TLS 1.3) | clientes / VPC |
| 8443 (MDS) | REST de metadatos (TLS 1.3). El cliente sigue aquí el 307 hacia el líder | `sg-gateway`, clientes en la VPC |
| 7000 (MDS) | Raft dentro de túneles TLS 1.3 + mTLS | solo `sg-mds` |
| 9444 (MDS) | gRPC Meta: heartbeats y block reports (mTLS) | `sg-dn` |
| 9443 (DN) | gRPC cliente (TLS + block token) | clientes (`sg-gateway`) |
| 9444 (DN) | gRPC nodo (mTLS): cadena de replicación, reparación, `CheckBlocks` del líder | `sg-dn`, `sg-mds` |

Solo el bastión tiene IP pública; MDS y DN en subredes privadas. Raft escucha internamente solo en loopback (127.0.x.x).

## Pasos
1. `cp deploy/aws/env.example deploy/aws/env` y completar las IP privadas, zonas y contraseñas de los usuarios.
2. PKI: `bash deploy/aws/gen_all_certs.sh ./pki`. Copiar `pki/<nodo>/*` → `/opt/dfsha/certs/` de cada VM, `pki/keys` → `/opt/dfsha/keys/` (solo MDS; la KEK queda 0400), `pki/keys-dn` → `/opt/dfsha/keys-dn/` (DN), `pki/ca.pem` a los clientes (`/opt/dfsha/ca.pem`). Para el gateway, `pki/gateway/*` → `/opt/dfsha/certs/`.
3. En cada VM: copiar el repo a `/opt/dfsha/repo` y ejecutar `ROLE=mds MDS_ID=mds-1 bash deploy/aws/bootstrap.sh` · `ROLE=dn DN_ID=dn-1 DN_IP=<ip> DN_ZONE=a bash ...` · `ROLE=gateway bash ...`.
4. Cliente (bastión): `python3.13 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt`; `export DFSHA_META=https://meta.dfsha.internal DFSHA_CA=/opt/dfsha/ca.pem DFSHA_ZONE=a`; `python -m dfsha.client login admin`; `python -m dfsha.client admin nodes` debe mostrar los DN `ALIVE` y `ACTIVE`.
5. Las IP privadas se conservan al detener/arrancar una VM dentro de la VPC; si cambian, regenerar `env` y certificados.
6. Comprobar desde el bastión: `python -m pytest tests -q` también puede ejecutarse allí (usa su propio clúster local) para validar la instalación.

## Guion de demostración (video 10–15 min) y evidencias
| Prueba | Comandos en AWS | Evidencia a capturar |
|---|---|---|
| T1 | `head -c 1G /dev/urandom > g.bin; sha256sum g.bin; dfsha put g.bin g.bin; dfsha admin blocks g.bin; rm g.bin; dfsha get g.bin g.bin; sha256sum g.bin` | hashes idénticos; bloque→nodos/zonas; `ls /opt/dfsha/blocks` en 3 DN |
| T2 | `dfsha write f --off N`, `dfsha cat f --off N --len 8`, `dfsha append f`, abrir en dos terminales | manifiestos antes/después (cambia un `block_id`); lectura parcial |
| T3 | dos terminales con `write` sobre el mismo bloque (un 200 y un 409); sobre bloques distintos (ambos 200, rebase); `dfsha lock f --mode X --block 0 --hold 60` y un `write` al bloque 0 (423) y al bloque 1 (200); `dfsha lock f --mode S` en dos terminales | 409, rebase, 423; con `kill -STOP` del titular más allá de 30 s → 409 STALE_FENCING |
| T4 | en un DN: `sudo deploy/aws/chaos.sh kill` (o detener la VM); `dfsha get`; `dfsha admin nodes/blocks`; en el MDS líder: `sudo deploy/aws/chaos.sh kill` y repetir `dfsha stat` | lectura OK durante la caída; DEAD; R=3 restaurado; nuevo líder en 1–3 s |
| T5 | `sudo chaos.sh delay 5000ms` en un DN y escribir; luego `sudo chaos.sh partition`, escribir, `heal` | bloques 2/3 → 3/3; commit OK con W=2; con < 2 nodos → 503 sin archivo parcial |
| T6 | bob sobre `/users/alice` (403), token vencido, `openssl s_client -connect meta.dfsha.internal:443 -tls1_2` (rechazado), `tcpdump -A -i any port 9443 -c 50` durante un put, `grep -c SECRETO /opt/dfsha/blocks/*.blk`, `sudo chaos.sh corrupt` y leer | 401/403, TLS 1.3, tráfico ilegible, texto claro ausente en disco, réplica corrupta descartada; `/opt/dfsha/data/audit.log` del líder |
| UC9 | levantar un DN nuevo, `dfsha admin add dn-6`, ver `dfsha admin nodes` (sube su número de bloques); `dfsha admin retire dn-6` | rebalanceo gradual y retiro sin perder lecturas |
| T7 | ver abajo | `evidencias/bench.csv` + `python bench/plot.py --cond-prefix aws` |

## Experimentos E1–E10 en AWS (desde el bastión)
```bash
export DFSHA_META=https://meta.dfsha.internal DFSHA_CA=/opt/dfsha/ca.pem
B="python bench/bench.py --reps 5 --password <clave-alice> --admin-password <clave-admin>"
$B --exp e1 --cond aws --sizes 1,100,1024
$B --exp e2,e3 --cond aws --big 1024 --parallel 1,2,4,8 --block-sizes 4,16,64
$B --exp e5 --cond aws --mid 100 --clients 1,2,4,8
$B --exp e6,e8 --cond aws --mid 100 --volumes 1024,5120,10240 \
   --fault-cmd "ssh {dn} sudo /opt/dfsha/repo/deploy/aws/chaos.sh kill" --heal-cmd "ssh {dn} sudo /opt/dfsha/repo/deploy/aws/chaos.sh start"
$B --exp e7 --cond aws --leader-fault-cmd "ssh mds-1 sudo /opt/dfsha/repo/deploy/aws/chaos.sh kill" \
   --leader-heal-cmd "ssh mds-1 sudo /opt/dfsha/repo/deploy/aws/chaos.sh start"
# E4: reiniciar los MDS con DFSHA_R=1 DFSHA_W=1 en deploy/aws/env y repetir: $B --exp e4 --cond aws_R1W1 --mid 100
# E9: con 3, 5 y 7 DN activos: $B --exp e9 --cond aws_5dn --n-dn 5 --mid 100 --clients 8
# E10: clúster con DFSHA_ALLOW_PLAINTEXT=1 y cliente con DFSHA_BENCH_PLAINTEXT=1 (AES apagado) y/o sin DFSHA_CERTS (TLS apagado)
python bench/plot.py --cond-prefix aws
```
`{dn}` se reemplaza por el nodo víctima elegido (una réplica del archivo). Configure `~/.ssh/config` en el bastión con los nombres `dn-N` y `mds-N`.
