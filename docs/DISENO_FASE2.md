# DFSha — Diseño implementado (Fase II), cumplimiento del diseño de Fase I y registro de decisiones

## 1. Garantías (comprobables)
| ID | Garantía | Prueba |
|---|---|---|
| G1 | Una escritura está **confirmada** cuando `commit` devuelve 200. Antes de publicar, el líder **consulta a cada nodo asignado** (`CheckBlocks`, mTLS) y solo cuenta las réplicas cuyo SHA-256 coincide con el declarado; exige ≥ W=2 en ≥ 2 zonas (estado `PREPARED`) y publica el manifiesto por quórum Raft (`COMMITTED`). | T1, T5, T9 (acks inventados → 503 y nada publicado) |
| G2 | Tras el 200, cualquier `open()` posterior de cualquier cliente ve esa versión o una más nueva: `open`, `ls` y `stat` pasan por el log Raft. | T2 |
| G3 | Nunca se ve una versión parcial: bloques inmutables + publicación atómica en un único commit; un cliente muerto no publica nada; un `.part` nunca se anuncia. | T8, T5 |
| G4 | Cada `open()` fija una instantánea: las lecturas no se bloquean; si la versión superó la gracia (10 min) y fue liberada → `410 SNAPSHOT_EXPIRED`, también desde un handle ya abierto. | T2, T8, T9 |
| G5 | Dos commits con la misma versión base: gana exactamente uno (CAS en el líder); el otro recibe `409 VERSION_CONFLICT`. `r+` sobre bloques **disjuntos** se rebasa y ambos confirman. Si el archivo se borró y recreó durante la escritura → 409 (sin ABA). | T3, T9, E5 |
| G6 | Una réplica lenta, caída o corrupta no es visible: los lectores solo contactan réplicas del manifiesto; la corrupta se detecta (SHA-256 / etiqueta GCM), se cambia de réplica, el nodo la verifica y la aparta, y no se re-admite. | T4, T5, T6 |
| G7 | Exclusión: un lock (S o X, archivo o bloque) ajeno vigente impide escribir lo que solapa (`423 LOCKED`), y también `rm`/`mv`; un titular expirado que reaparece con su época recibe `409 STALE_FENCING`. | T3, T9 |

**Integridad ≠ coordinación.** Los hashes y la escritura atómica solo detectan corrupción y evitan publicar datos a medias; la exclusión viene de CAS por versión + locks con fencing.

## 2. Semántica de RF3
| Aspecto | Semántica |
|---|---|
| `write()` | **x** crear (409 si existe) · **w** reemplazar (versión completa nueva) · **a** anexar (bloques nuevos al final) · **r+** modificar rangos dentro del tamaño actual (copy-on-write de los bloques afectados). No se escribe más allá del EOF salvo anexando. |
| Granularidad | API por bytes; almacenamiento, copy-on-write y locks por bloque (≤ 64 MiB); cifrado y lectura autenticada por segmentos GCM de 64 KiB. |
| Visibilidad | Close-to-open: privado hasta `close()/flush()`; el commit publica una versión de forma atómica. |
| `read(off, n)` | De la instantánea abierta; trae solo los bloques afectados y, dentro de cada uno, solo los segmentos necesarios (`GetBlock` con rango). |
| `lock(path, block=None, mode='S'|'X', wait=0)` | Lease de 30 s renovado cada 10 s, guardado en Raft, con época monótona (fencing) que viaja en `begin` y `commit`. S+S compatibles; X excluye todo lo que solapa; lock de archivo solapa con todos sus bloques. Ocupado → 423 o espera hasta `wait`. |
| Anexos | Se serializan en el líder en orden de commit; no se pierden entre sí (el SDK reintenta con la versión nueva). Cuando quedan ≥ 8 bloques cortos al final, el cliente los compacta (op `c`): mismo contenido, bloques llenos, nueva versión. |
| Borrado | Lápida atómica; lectores con instantánea previa terminan dentro de la gracia; luego GC. |

## 3. Cumplimiento del diseño de Fase I
Leyenda: ✅ implementado tal cual · 🔁 implementado con un mecanismo equivalente (ver §4).

| Elemento del diseño | Estado | Dónde |
|---|---|---|
| Plano de control 3 MDS con Raft (2 de 3), datos fuera del camino del líder | ✅ | `mds/server.py`, `mds/state.py` |
| Entrada DNS + NGINX `meta.dfsha.internal`; los no líderes responden **307** hacia el líder | ✅ | `deploy/aws/nginx.conf`, T9, T10 |
| REST/JSON (FastAPI) para control, gRPC para datos, sin MOM (órdenes en la respuesta al heartbeat) | ✅ | |
| Bloques de 64 MiB, `block_id` UUID inmutable, copy-on-write | ✅ | |
| Colocación *power of two choices* por espacio libre, nodos y zonas distintas (≥ 2), primario que rota | ✅ | `MDS.alloc` |
| R=3, W=2 en ≥ 2 zonas; tercera réplica por la cadena o por re-replicación | ✅ | |
| **Replicación en pipeline**: cliente → primario, que reenvía en cadena | ✅ | `DataNode._chain` |
| Ubicación dinámica, réplicas ordenadas por cercanía/carga, refresco con `GET /v1/blocks/{id}/locations` | ✅ | |
| Reconstrucción: SHA-256 por bloque (texto cifrado), etiqueta GCM, concatenación por `block_idx`, `content_sha256` final | 🔁 (4.4) | |
| Transacción `OPEN → PREPARED → COMMITTED / ABORTED`, TTL 60 s renovado cada 15 s, commit idempotente por `txn_id` | ✅ | `GET /v1/txn/{id}` |
| `.part → fsync → hash → rename`; `.part` residual se borra por GC (10 min) o al reiniciar | ✅ | |
| Huérfanos y versiones viejas eliminados por GC / block report | ✅ | |
| Heartbeat 3 s; SUSPECT > 10 s; DEAD > 30 s; re-replicación priorizada con límite de ancho de banda | ✅ | `DFSHA_REREPL_MAX`, `DFSHA_REPL_MBPS` |
| Bloque corrupto: hash en lectura y scrubbing cada 6 h; copia desde réplica sana; la corrupta se descarta | ✅ | orden `verify` |
| UC9: alta de nodo + **rebalanceo gradual**; retiro de nodos | ✅ | `POST/DELETE /v1/admin/nodes`, T9 |
| `GetBlock` con rango opcional | ✅ | segmentos GCM |
| `ReplicateBlock` / `DeleteBlock` solo del líder (mTLS + rol) | ✅ | CN `mds-*` |
| Catálogo de errores A.4 (307, 400, 401, 403, 404, 409, 410, 423, 503; gRPC DATA_LOSS, PERMISSION_DENIED, UNAUTHENTICATED, RESOURCE_EXHAUSTED, DEADLINE_EXCEEDED) | ✅ | + `429 TOO_MANY_REQUESTS`, `ALREADY_EXISTS` |
| `lock()` exclusivo/compartido sobre archivo o bloque, lease 30 s en Raft, fencing, 423, 409 STALE_FENCING, rebase de bloques disjuntos | ✅ | |
| Argon2id; JWT EdDSA 15 min + **refresh 8 h**; **5 intentos de login por minuto** | ✅ | |
| `jti` contra reutilización de tokens | ✅ | refresh de un solo uso |
| mTLS entre nodos **incluido Raft**; un nodo sin certificado no entra | ✅ | `mds/raft_tls.py` |
| Autorización `/users/<uid>`; block tokens Ed25519 (bloque, operación, uid, txn, exp ≤ 60 s); rol admin separado | ✅ | |
| TLS 1.3 cliente→gateway y cliente→nodos | ✅ | REST y gateway fuerzan 1.3; gRPC negocia 1.3 (4.9) |
| Cifrado de sobre en el cliente: DEK AES-256 por archivo, AES-GCM, nonce de 96 bits, AAD = file_id‖block_idx‖block_id; DEK envuelta con AES-KW y KEK 0400 | ✅ | + índice de segmento en el AAD |
| Auditoría de solo-anexar en el líder (login, open, commit, rm, lock) | ✅ | `audit.log` (O_APPEND, 0600) |
| Experimentos E1–E10 con ≥ 5 repeticiones, media, desviación y p95, CSV | ✅ | `bench/bench.py`, `bench/plot.py` |

## 4. Registro de decisiones (cambios frente a la Fase I y por qué)
| # | Fase I | Implementado | Motivo |
|---|---|---|---|
| 4.1 | Raft | Biblioteca **PySyncObj** (Raft real). Los métodos `@replicated` son deterministas (el líder pasa `now`, ids y asignaciones). Los heartbeats **no** pasan por el log: viven en memoria del líder y el nuevo líder los reconstruye (~2 heartbeats en los que responde 503 reintentable). Se desactiva el modo *batch* de PySyncObj: con él cada operación de metadatos costaba ~100 ms; sin él, ~5 ms. | Escribir Raft desde cero no es abordable con calidad en el plazo; el log de heartbeats lo saturaría. |
| 4.2 | mTLS en Raft | PySyncObj solo habla TCP plano: cada MDS expone un túnel TLS 1.3 con certificado de cliente obligatorio (CN `mds-*`) y Raft escucha solo en loopback. La contraseña de PySyncObj queda como segunda capa opcional. | Cumplir mTLS sin modificar la biblioteca; impide además que un tercero se conecte como nodo de solo lectura de PySyncObj (que recibiría el log). |
| 4.3 | DEK "envuelta por el MDS" y cifrado en cliente | El cliente genera la DEK y la envía **por TLS** en el commit; el MDS la guarda envuelta (AES-KW, KEK en archivo 0400) y la devuelve desenvuelta por TLS en `open`. | El diseño entregaba al cliente la DEK envuelta con una KEK que solo tiene el MDS: el cliente no podía descifrar. El MDS ya era de confianza (limitación declarada en Fase I). |
| 4.4 | `content_sha256` del archivo | `content_sha256` = SHA-256 de la lista de SHA-256 en claro de cada bloque (verificados uno a uno al leer). | Se puede recalcular tras `a`/`r+` sin releer el archivo completo; misma fuerza de verificación. |
| 4.5 | AES-GCM por bloque | AES-GCM por **segmentos de 64 KiB** dentro de cada bloque (nonce base + contador, AAD con el índice de segmento). | Una sola etiqueta por bloque obliga a descargar 64 MiB para leer 1 byte; con segmentos `GetBlock` con rango trae y autentica solo lo necesario (sobrecosto: 16 B por 64 KiB). |
| 4.6 | Anexos "sin conflicto" | El líder serializa; si cambió la versión base, el SDK recifra y reintenta (el AAD lleva `block_idx`). | Coherente con el AAD que impide mover bloques. |
| 4.7 | Compactación de anexos "en segundo plano" | La dispara el cliente al cerrar un anexo cuando hay ≥ 8 bloques cortos al final (op `c`, best-effort, conflictos ignorados); también `dfsha compact`. | Solo el cliente maneja texto en claro; los nodos de datos nunca lo ven. |
| 4.8 | Cadena de replicación | Pipeline real (cada eslabón guarda y reenvía cada trozo mientras lo recibe; el segundo eslabón va a otra zona). Si un eslabón cae o tarda más de 1 s, el primario **refuerza** empujando el bloque completo a los asignados que faltan. | Una cadena pura se bloquea con un eslabón lento; el refuerzo conserva W=2 sin publicar nada a medias. |
| 4.9 | TLS 1.3 cliente→nodos | gRPC (BoringSSL) negocia TLS 1.3 con estos clientes, pero grpcio para Python no expone un mínimo de versión; el REST (uvicorn) y el gateway sí fuerzan 1.3. | Limitación de la API de grpcio. |
| 4.10 | Límite de login | 5 intentos **fallidos** por minuto y usuario (y 20 por IP); bloqueado → 429 aunque la clave sea correcta. | Contar también los exitosos bloquearía a usuarios legítimos sin aumentar la protección. |
| 4.11 | Registro de nodos | Por defecto (`DFSHA_DN_ADMISSION=auto`) un DN con certificado válido de la CA se registra solo por heartbeat (§8.4); con `admin` solo se usa tras `dfsha admin add`. El retiro (`dfsha admin retire`) traslada sus bloques antes de liberarlo. | §8.4 dice que los DN se registran solos; UC9 pide alta administrativa: ambos modos. |
| 4.12 | `BlockReport` en flujo | Cliente-streaming en lotes de 1000 ids. | — |
| 4.13 | — (defecto de la biblioteca) | `raft_safety.py` envuelve el journal de PySyncObj para que un AppendEntries atrasado nunca borre entradas ya aplicadas (PySyncObj las borraba aunque no hubiera conflicto). | Regla de Raft §5.3; sin esto, un seguidor podía perder entradas confirmadas tras caídas del líder (la advertencia de PySyncObj apareció al repetir caídas del líder en E7). Prueba T0. |
| 4.14 | G2 (open ve el último commit) | `GET /v1/files/{path}` (el `open()` del cliente) pasa por `barrier()` antes de leer, igual que `ls`/`stat`. | Faltaba en la implementación inicial: bajo carga, un `open()` justo después de un `commit` 200 podía leer una versión anterior (lectura no linealizable). Reproducido de forma intermitente corriendo la suite completa bajo carga (T3, rebase de bloques distintos). |

## 5. Colocación, replicación y recuperación
* Colocación: *power of two choices* por espacio libre; nodos distintos y ≥ 2 zonas; el segundo eslabón de la cadena va a otra zona.
* Detección: heartbeat 3 s; SUSPECT > 10 s (no recibe escrituras); DEAD > 30 s (se retira de las ubicaciones).
* Re-replicación: prioriza bloques con menos réplicas, evita zonas ya cubiertas, ≤ 4 órdenes simultáneas y 50 MB/s por nodo. Los bloques recién confirmados no se re-replican durante `3·HB+2 s` (su tercera réplica aún se está anunciando).
* Rebalanceo: con todo en R=3 y sin reparaciones pendientes, traslada bloques del nodo más cargado al menos cargado (≤ 4 a la vez) respetando zonas; un traslado se confirma cuando el destino anuncia la copia y entonces se borra en el origen.
* Block report al arrancar y periódico: réplicas perdidas se retiran; bloques desconocidos → huérfanos → borrado; sobrantes (nodo que vuelve) se borran.
* Scrubbing periódico y orden `verify` tras un reporte del cliente: solo el nodo, al recalcular el hash, declara corrupta su copia → `.corrupt` (se borra tras 1 h) → aviso en el siguiente heartbeat → el líder la retira y re-replica; no se re-admite en ese nodo durante 10 min. El reporte de un cliente por sí solo no retira réplicas (mientras se verifica, esa réplica se ofrece al final).
* Borrado: lápida atómica; versiones viejas y archivos borrados se liberan tras la gracia (600 s) si ninguna instantánea los fija.
* Escritura interrumpida: TTL de la transacción; sin commit → ABORTED y GC de huérfanos; `.part` abandonados → GC a los 10 min.

## 6. API (resumen; OpenAPI en `/docs` de cada MDS)
REST `/v1`: `auth/login`, `auth/refresh`, `auth/logout` · `fs/{path}` (GET ls/stat, POST mkdir/mv, DELETE rm/rmdir) · `txn` (+ `GET /{id}`, `/tokens`, `/renew`, `/commit`, `/abort`) · `files/{path}?version=` · `blocks/{id}/locations`, `blocks/{id}/bad` · `locks` (POST, GET ?path=, PUT/DELETE `/{id}`) · `admin/nodes` (GET, POST, DELETE `/{dn}`), `admin/blocks?path=`.
gRPC: `DataNode.{PutBlock, GetBlock, StoreBlock, ReplicateBlock, DeleteBlock, CheckBlocks}`, `Meta.{Heartbeat, BlockReport}`.
Errores: 307 `NOT_LEADER` (Location al líder) · 400 `INVALID_PATH/INVALID_ARGUMENT` · 401 `UNAUTHENTICATED` · 403 `FORBIDDEN` · 404 `NOT_FOUND` · 409 `VERSION_CONFLICT/STALE_FENCING/NOT_EMPTY/EXISTS` · 410 `SNAPSHOT_EXPIRED/LEASE_EXPIRED` · 423 `LOCKED` · 429 `TOO_MANY_REQUESTS` · 503 `INSUFFICIENT_REPLICAS/UNAVAILABLE`; gRPC `DATA_LOSS/PERMISSION_DENIED/UNAUTHENTICATED/NOT_FOUND/ALREADY_EXISTS/RESOURCE_EXHAUSTED/DEADLINE_EXCEEDED`.

## 7. Seguridad
Argon2id + JWT EdDSA (access 15 min, refresh 8 h de un solo uso) · 5 intentos fallidos de login por minuto · autorización: todo path pertenece a `/users/<sub>`; el directorio raíz del usuario no se puede borrar ni mover; admin solo opera el clúster · block tokens Ed25519 (≤ 60 s: bloque, operación, uid, txn), renovables durante una transacción abierta · TLS 1.3 cliente→gateway/MDS y TLS cliente→DN · mTLS entre nodos y en Raft (CA privada; CN `dn-*`/`mds-*` controla qué operaciones puede pedir cada uno; el `dn_id` del heartbeat debe coincidir con el certificado) · AES-256-GCM en el cliente por segmentos (AAD = file_id‖idx‖block_id‖segmento) · DEK por archivo envuelta con KEK · bloques inmutables (un `.blk` existente no se sobrescribe) · EBS cifrado como segunda capa · auditoría de solo-anexar en el líder.

## 8. Limitaciones declaradas
Un único grupo Raft (escala de metadatos acotada; futuro: particionar por uid) · el MDS es de confianza (conoce la KEK y entrega la DEK por TLS) · sin compartición entre usuarios, sin POSIX completo, sin archivos dispersos, sin erasure coding ni geo-replicación · el mínimo de TLS 1.3 en gRPC depende de la biblioteca (4.9) · la auditoría es un archivo por MDS (cada líder anexa al suyo).
