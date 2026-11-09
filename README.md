# DFSha — sistema de archivos distribuido (Proyecto 1, Fase II)

Sistemas Distribuidos 2026-2 · Universidad Pontificia Bolivariana, Facultad de Ingeniería, Arquitectura de Nube y Sistemas Distribuidos · Profesor: Álvaro Enrique Ospina Sanjuan.

**Equipo**: Juan Sebastián Jaramillo García · Gloria Yuliana Peña Ibargüen (propuesta de Fase I adoptada como base, ver `docs/INFORME_TECNICO_FINAL.pdf` §2) · Juan José Álvarez Restrepo.

Implementación del diseño de Fase I adoptado por el equipo (Opción 1: cliente/servidor con servicio distribuido entre servidores, en red privada).
Python 3.13 · FastAPI (REST de control) · gRPC (plano de datos) · Raft (PySyncObj, sobre mTLS) · AES-256-GCM en el cliente · TLS 1.3 / mTLS · Docker · NGINX.

## Documentación

| Documento | Contenido |
|---|---|
| [`docs/INFORME_TECNICO_FINAL.pdf`](docs/INFORME_TECNICO_FINAL.pdf) | Informe técnico final (entregable §5.3): comparación de propuestas individuales y decisión del equipo (matriz de riesgo), arquitectura consensuada, garantías, cambios frente a Fase I, API, algoritmos distribuidos, seguridad, despliegue, estrategia de pruebas, resultados reales en AWS (T1–T7), decisiones de ingeniería, limitaciones, trabajo futuro y conclusiones |
| [`docs/DISENO_FASE2.md`](docs/DISENO_FASE2.md) | Garantías comprobables, semántica de RF3, cumplimiento del diseño de Fase I, registro de decisiones completo, API, seguridad, limitaciones |
| [`docs/AWS.md`](docs/AWS.md) | Topología y pasos de despliegue en AWS Academy, guion de demostración para el video |
| [`docs/DESPLIEGUE_PASO_A_PASO.md`](docs/DESPLIEGUE_PASO_A_PASO.md) | Guion completo desde cero: crear las VM en la consola de AWS Academy, par de claves, grupo de seguridad, y desplegar con `deploy_all.sh` |

```
cliente (CLI/SDK: cifra, parte en bloques, hash, paralelo; solo conoce meta.dfsha.internal)
   │ REST/TLS 1.3 ─▶ NGINX meta.dfsha.internal ─▶ 3 × MDS (Raft 2/3 sobre mTLS: árbol, manifiestos, bloque→réplicas, locks, usuarios)
   │                     (un seguidor responde 307 → el cliente sigue al líder)      │ gRPC mTLS: heartbeat 3 s / block report / órdenes
   │ gRPC/TLS + block token (Ed25519, ≤ 60 s)                                         │ CheckBlocks: el líder verifica cada réplica antes del commit
   └──────────────▶ N ≥ 4 × DN primario ══cadena mTLS══▶ DN ══▶ DN (bloques inmutables <id>.blk, cifrados) ◀──┘
```

## Estructura

| Ruta | Contenido |
|---|---|
| `proto/dfsha.proto` | Contratos gRPC (`DataNode`, `Meta`) |
| `dfsha/common.py` | Cifrado AES-256-GCM por segmentos de 64 KiB, tokens Ed25519, hashes, errores del catálogo |
| `dfsha/mds/state.py` | Máquina de estados replicada con Raft: RF1, transacciones OPEN→PREPARED→COMMITTED, CAS/rebase, locks S/X por archivo o bloque con fencing, compactación, GC |
| `dfsha/mds/server.py` | API REST (FastAPI), 307 al líder, verificación de réplicas, refresh tokens, límite de logins, auditoría, monitores (DEAD, re-replicación, rebalanceo, retiro, GC, huérfanos) |
| `dfsha/mds/raft_tls.py` | Túneles TLS 1.3 con mTLS para el tráfico Raft (PySyncObj solo habla TCP plano) |
| `dfsha/mds/raft_safety.py` | Salvaguarda del *journal* de Raft (un `AppendEntries` atrasado no borra entradas ya aplicadas) |
| `dfsha/dn/server.py` | Nodo de datos: replicación en cadena, `.part→fsync→hash→.blk`, inmutabilidad, `GetBlock` por rango, `CheckBlocks`, heartbeat, block report, scrubbing, GC de `.part` |
| `dfsha/client/` | SDK (`DFSha`, `Handle`) y CLI (`python -m dfsha.client`) |
| `dfsha/pb/` | Stubs gRPC generados (`dfsha_pb2.py`, `dfsha_pb2_grpc.py`) |
| `scripts/` | `gen_certs.py` (CA, certificados mTLS por nodo), `local_cluster.py` (levanta 3 MDS + 5 DN reales en localhost, usado por los tests y en modo interactivo), `gen_proto.sh` (regenera los stubs gRPC), `resumen.py` (reúne evidencias + rendimiento + gráficas en `evidencias/Local/RESUMEN.md`) |
| `tests/` | `conftest.py` + T0, T1, T2, T3, T4 (incluye T5), T6, T8, T9, T10; cada prueba deja evidencia en `evidencias/` |
| `bench/` | `bench.py` (experimentos E1–E10, local o remoto), `plot.py` (gráficas), `run_local_all.sh` (corre varias condiciones en local) |
| `deploy/Dockerfile` | Imagen única; el rol se elige con el comando (`python -m dfsha.mds.server` \| `dfsha.dn.server` \| `dfsha.client`) |
| `deploy/aws/` | `deploy_all.sh` (despliegue automático de las VM), `bootstrap.sh` (provisión por rol en cada VM), `gen_all_certs.sh` (PKI/mTLS), `chaos_all.sh`/`chaos.sh` (inyección de fallos remota real), `compose.mds.yml`/`compose.dn.yml`, `nginx.conf`, `env.example` (IPs privadas de las VM), `inventory.txt` |
| `evidencias/AWS/` | Evidencia de T1–T7 corridas contra el **clúster real en AWS**: `T1_*.txt` … `T7_rendimiento.txt`, `rendimiento.csv`, `Graficas/` (diagramas de arquitectura, despliegue y secuencia) |
| `evidencias/Local/` | Evidencia de T0–T10 corridas en **local** (procesos separados con TLS/mTLS real): `T0_*.txt` … `T10_gateway.txt`, `RF1_directorios.txt`, `CLI.txt`, `bench.csv`, `RESUMEN.md`, `Graficas/` (E1–E10) |
| `docs/` | `INFORME_TECNICO_FINAL.pdf`, `DISENO_FASE2.md`, `AWS.md`, `DESPLIEGUE_PASO_A_PASO.md` |
| `requirements.txt` | Versiones exactas usadas en todas las pruebas (Python 3.13) |

## Inicio rápido (local, sin Docker)
```bash
python3.13 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python -m pytest tests -q                   # levanta 3 MDS + 5 DN con TLS/mTLS en procesos separados (~4 min); T10 requiere nginx
python -m scripts.local_cluster              # clúster interactivo; imprime DFSHA_META y DFSHA_CA
export DFSHA_META=https://127.0.0.1:18002 DFSHA_CA=/tmp/dfsha_local/certs/ca.pem   # basta UN MDS: si no es líder responde 307
python -m dfsha.client login alice -p alicepw
python -m dfsha.client mkdir docs && python -m dfsha.client put archivo.bin docs/archivo.bin && python -m dfsha.client ls docs
bash bench/run_local_all.sh && python bench/plot.py      # varias condiciones de rendimiento + gráficas
```
Usuarios de prueba: `alice/alicepw`, `bob/bobpw`, `carol/carolpw`, `admin/adminpw` (rol admin: solo operaciones de clúster). En AWS se definen al desplegar con `deploy_all.sh`.

## Despliegue y pruebas en AWS Academy (clúster real)

El sistema fue desplegado y verificado en **VM reales** de AWS Academy (1 gateway, 3 MDS, 4 DN) repartidas en 3 zonas de disponibilidad (`us-east-1a/b/c`), comunicándose por red real con TLS 1.3/mTLS — guía completa en [`docs/DESPLIEGUE_PASO_A_PASO.md`](docs/DESPLIEGUE_PASO_A_PASO.md).

**Resumen del despliegue** (todo se ejecuta desde el gateway, única VM con IP pública):
```bash
bash deploy/aws/bootstrap.sh                 # provisión del rol (mds|dn|gateway) en cada VM: ROLE=mds|dn|gateway bash bootstrap.sh
bash deploy/aws/gen_all_certs.sh             # genera la CA y un certificado mTLS por nodo (una vez, desde el operador)
bash deploy/aws/deploy_all.sh                # despliegue completo: preparar -> descubrir IP -> PKI -> enviar -> arrancar -> verificar
bash deploy/aws/deploy_all.sh status         # estado de contenedores y nodos
bash deploy/aws/deploy_all.sh logs dn-2      # logs de un nodo
bash deploy/aws/deploy_all.sh restart        # re-crear contenedores tras cambiar código/env, sin perder datos
```
Las IP privadas de las VM se definen copiando `deploy/aws/env.example` a `deploy/aws/env`.

**Inyección de fallos reales** sobre cualquier nodo, desde el gateway:
```bash
bash deploy/aws/chaos_all.sh dn-2 kill           # mata el proceso del nodo (start para revivirlo)
bash deploy/aws/chaos_all.sh dn-3 delay 5000ms   # retraso de red (undelay para quitarlo)
bash deploy/aws/chaos_all.sh dn-3 partition      # lo desconecta de la red (heal para reconectar)
bash deploy/aws/chaos_all.sh mds-1 kill          # mata al líder de metadatos
bash deploy/aws/chaos_all.sh dn-4 corrupt        # corrompe un bloque en disco
```

**Última corrida real** (2026-11-08, VM reales: 3 MDS + 4 DN + gateway): **T1–T7: 7/7 OK**. Detalle completo en `evidencias/AWS/`:

| Prueba | Resultado |
|---|---|
| T1 — Carga y recuperación | Archivo repartido en 3–4 DN; `get` a 109.5 MB/s; hash SHA-256 idéntico |
| T2 — Acceso RF3 | `x`/`w`/`a`/`r`/`r+`, close-to-open e instantánea por `open()`: todo OK |
| T3 — Concurrencia | CAS por versión + *rebase* en bloques disjuntos + anexos serializados + locks S/X con lease y fencing: OK |
| T4 — Replicación y fallos | `kill` real a un DN: lectura OK en 0.60 s durante la caída; `kill` real al líder MDS: 2.75 s de indisponibilidad, nuevo líder, escrituras siguen con quórum 2/3 |
| T5 — Consistencia | Réplica desconectada (`partition` real): escritura y lectura OK con W=2 en ≥ 2 zonas; sin quórum: `503` y sin datos parciales |
| T6 — Seguridad | 401/403 en todos los casos probados; TLS 1.3 aceptado y TLS 1.2 rechazado; tráfico real capturado sin texto claro; 1 byte corrompido en un `.blk` real → detectado |
| T7 — Rendimiento | 100 MB: `put` 71.6 MB/s / `get` 110.1 MB/s (paralelo=4); 4 clientes concurrentes leyendo el mismo archivo: 106.6 MB/s agregados |

## CLI
`login · logout · ls · cd · pwd · mkdir [-p] · rmdir · rm [-r] · mv · stat · put [-x] · get [--version] · cat [--off --len] · write --off (r+) · append · lock [--mode S|X] [--block N] [--wait s] [--hold s] · compact · admin nodes|blocks <ruta>|add <dn>|retire <dn>`

## RF3 — semántica declarada (no es POSIX completo)
`open(path, modo)`: **x** crear (falla si existe) · **w** reemplazar · **a** anexar · **r** leer · **r+** modificar rangos *dentro* del tamaño actual (copy-on-write de los bloques afectados).
Close-to-open: las escrituras son privadas hasta `close()/flush()`; el commit publica una versión nueva de forma atómica y cualquier `open()` posterior la ve. Cada `open()` fija una instantánea; `read(off, n)` trae solo los bloques afectados y, dentro de cada bloque, solo los segmentos GCM de 64 KiB necesarios. `lock()`: compartido o exclusivo, sobre el archivo o un bloque, con lease de 30 s y época de fencing. Detalle en `docs/DISENO_FASE2.md`.

## Pruebas → rúbrica (§5.2)
| Prueba | Archivo | Evidencia |
|---|---|---|
| Carga y recuperación (hash, bloques en varios nodos) | `test_t1_*` | `evidencias/Local/T1_*.txt`, `evidencias/AWS/T1_*.txt` |
| Acceso RF3 / RF1 | `test_t2_*` | `T2_rf3.txt`, `RF1_directorios.txt` |
| Concurrencia (CAS, rebase, anexos, lock y fencing) | `test_t3_*`, `test_t9_*` | `T3_*.txt`, `T9_*.txt` |
| Replicación y fallos (caída real de DN y del líder) | `test_t4_*` | `T4_*.txt` |
| Consistencia (réplica retrasada / desconectada / sin quórum) | `test_t4_*` | `T5_*.txt` |
| Seguridad (autorizado/denegado, canal, almacenamiento, manipulación) | `test_t6_*`, `test_t9_*` | `T6_*.txt`, `T9_*.txt` |
| Rendimiento (≥ 2 condiciones) | `bench/bench.py` | `evidencias/Local/bench.csv`, `evidencias/AWS/rendimiento.csv`, `Graficas/*.png` |
| Borrado, GC, instantánea expirada, escritura interrumpida, CLI | `test_t8_*` | `T8_*.txt`, `CLI.txt` |
| Fidelidad al diseño (307, PREPARED, ABA, tokens, locks S/X, refresh, auditoría, Raft mTLS, compactación, rebalanceo/retiro) | `test_t9_*` | `T9_fidelidad.txt` |
| Gateway NGINX real (307, 503 intacto, caída del líder) | `test_t10_*` | `T10_gateway.txt` |
| Journal de Raft: un `AppendEntries` atrasado no borra entradas aplicadas | `test_t0_*` | `T0_raft_journal.txt` |

`python scripts/resumen.py` reúne todas las evidencias locales, la tabla de rendimiento y las gráficas en `evidencias/Local/RESUMEN.md`.
