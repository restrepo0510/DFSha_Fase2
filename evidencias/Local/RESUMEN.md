# Resumen de evidencias

Corrida local: 3 MDS + 5 DN como procesos separados que se comunican por TCP con TLS 1.3 / mTLS (incluido Raft) y, en T10, con el gateway NGINX real. Las cifras de rendimiento de un solo host NO son representativas de AWS: repetir allí (docs/AWS.md).

## T0_raft_journal

- AppendEntries atrasado: entradas 1..8 ya aplicadas se conservan (PySyncObj las habría borrado); conflicto real en entradas no aplicadas: se trunca y se reemplaza (Raft §5.3)

## T1_carga_recuperacion

- archivo 3158073 B -> 13 bloques, versión 1  put=0.17s get=0.08s
- sha256 origen   8c5530d5b33999d8686398a82bb98564ef50c0738b90fe630dbe2f77ab9f7da8
- sha256 recuperado 8c5530d5b33999d8686398a82bb98564ef50c0738b90fe630dbe2f77ab9f7da8  (IDÉNTICO)
- nodos usados: ['dn-2', 'dn-3', 'dn-4', 'dn-5']
-   bloque  0 b-8ba6d163f4 -> dn-2(b), dn-4(a), dn-3(c)
-   bloque  1 b-a3bae3e300 -> dn-2(b), dn-4(a), dn-3(c)
-   bloque  2 b-c3326561e6 -> dn-2(b), dn-4(a), dn-3(c)
-   bloque  3 b-8e10bb80d6 -> dn-2(b), dn-3(c), dn-5(b)
-   bloque  4 b-26a231cc03 -> dn-4(a), dn-3(c), dn-5(b)
-   bloque  5 b-e9f8d046fb -> dn-2(b), dn-4(a), dn-3(c)
-   bloque  6 b-1c45a78b3e -> dn-2(b), dn-4(a), dn-3(c)
-   bloque  7 b-a0457b22c0 -> dn-2(b), dn-4(a), dn-3(c)
-   bloque  8 b-08865f8ad3 -> dn-2(b), dn-3(c), dn-5(b)
-   bloque  9 b-dbbea4573c -> dn-4(a), dn-3(c), dn-5(b)
-   bloque 10 b-ac782e34a3 -> dn-2(b), dn-4(a), dn-3(c)
-   bloque 11 b-f895862472 -> dn-2(b), dn-4(a), dn-3(c)
-   bloque 12 b-b8286f1f47 -> dn-4(a), dn-3(c), dn-5(b)

## T2_rf3

- x (EXISTS al repetir), r+ (solo bloque 1 cambió: b-9f54a39e->b-6198171c), r+ fuera de EOF (400), read(off,n) cruzando 2 bloques = 2 GetBlock, a (v3, 4 bloques), w (v4), close-to-open e instantánea: OK

## T3_concurrencia

- [sin lock, mismo bloque] resultados={'c1': '409 VERSION_CONFLICT', 'c2': '200'}; contenido final=b'BBBB'; versión=2
- [sin lock, bloques distintos] ambos commits OK por rebase: versión 3 con cambios de los dos clientes
- [4 appends concurrentes] los 4 se aplican en orden de commit, size=39
- [lock] 2.º lock -> 423 LOCKED; escritor sin lock -> 423; titular expirado -> 409 STALE_FENCING (épocas 1 -> 2); contenido intacto

## T4_replicacion_fallos

- kill -9 dn-1; get durante la caída OK en 0.07s (hash idéntico); DEAD detectado; re-replicación a R=3 en 9.6s tras DEAD; nodo reiniciado reconciliado (sobrantes eliminados)
- kill -9 del líder mds-2: ventana de indisponibilidad 2.93s; nuevo líder mds-1; metadatos intactos y escrituras siguen (quórum 2/3); mds-2 reintegrado

## T5_consistencia

- réplica retrasada (dn-3 +15s): commit 200 con 1/8 bloques en 2/3 (UNDER_REPLICATED); lectura inmediata íntegra sin contactar a dn-3; tras la llegada de la réplica, todos 3/3
- réplica desconectada (kill dn-2): escritura OK con W=2, re-replicación a 3/3 sin ella; con <2 nodos vivos: 503 INSUFFICIENT_REPLICAS y el archivo no existe (sin parciales)

## T6_seguridad

- AUTORIZADO: alice opera en /users/alice. DENEGADO: login con clave mala 401; sin JWT 401; JWT expirado 401; JWT con firma falsa 401; bob sobre /users/alice 403 (ls, manifest, open); alice a /v1/admin 403; admin sobre contenido de usuarios 403
- TOKENS DE BLOQUE: token de otro bloque -> PERMISSION_DENIED; token 'get' usado para PutBlock -> PERMISSION_DENIED; vencido -> UNAUTHENTICATED; firma inválida -> PERMISSION_DENIED
- CANAL: REST por http plano rechazado; REST con TLS: Protocol version: TLSv1.3 | Ciphersuite: TLS_AES_256_GCM_SHA384 | Verification: OK; gRPC sin TLS -> UNAVAILABLE; DeleteBlock en puerto de cliente (sin cert de nodo) -> PERMISSION_DENIED; puerto de nodo sin certificado de cliente -> handshake rechazado; nodo con certificado de otra CA -> rechazado
- ALMACENAMIENTO: 9 archivos .blk en los DN, ninguno contiene el texto claro; AES-256-GCM por segmentos de 64 KiB; descifrar con otra clave o con otra posición (AAD) -> InvalidTag. MANIPULACIÓN: 1 byte alterado en dn-2 -> el cliente detecta hash inválido, usa otra réplica y reporta; el nodo verifica y descarta su copia; el líder restaura R=3 sin readmitirla. Scrubbing detectó corrupción silenciosa en dn-1 y se re-replicó.

## T8_borrado_huerfanos

- rm -> lápida (stat 404) y el lector con instantánea siguió leyendo en la gracia; GC liberó 3 bloques a los 24s; versión 1 reemplazada -> 410 SNAPSHOT_EXPIRED tras la gracia y sus bloques liberados; cliente muerto sin commit: archivo nunca publicado, 2 bloques huérfanos eliminados por GC (TTL de la transacción + gracia)

## T9_fidelidad

- [307] un seguidor responde 307 NOT_LEADER con Location=https://127.0.0.1:18002...; el cliente sigue al líder conservando método y cuerpo
- [G1] commit con acks inventados -> 503 INSUFFICIENT_REPLICAS (el líder verificó el hash en cada nodo), txn ABORTED, nada publicado; escritura real OPEN -> PREPARED -> COMMITTED y commit repetido idempotente
- [ABA] borrar y recrear el archivo durante una escritura -> 409 VERSION_CONFLICT; el archivo nuevo queda íntegro
- [home] rm -r y mv del directorio raíz del usuario -> 403
- [tokens] subida que dura más que la vigencia del block token: el cliente renovó 1 token(s) y confirmó
- [locks] S+S compatibles; X frente a S -> 423; escritor bloqueado por S ajeno -> 423; X por bloque: bloques distintos en paralelo, mismo bloque -> 423; lock(wait) espera a que se libere
- [locks] rm/mv de un archivo bloqueado por otro titular -> 423; el titular lo mueve y su lock lo acompaña
- [auth] refresh rota los tokens (8 h); reutilizar un refresh token (mismo jti) -> 401; tras logout -> 401
- [auth] 5 intentos fallidos por minuto: [401, 401, 401, 401, 401, 429] y luego 429 aun con la clave correcta
- [auditoría] 134 eventos en mds-1/audit.log (O_APPEND, 0600); tipos: ['abort', 'bad_replica', 'commit', 'commit_failed', 'leader_elected', 'lock', 'lock_denied', 'login', 'login_failed', 'mkdir', 'mv', 'open', 'refresh_reuse', 'replica_corrupt', 'rm', 'unlock']
- [canal] Raft solo acepta TLS 1.3 con certificado mds-* (en claro, sin certificado o con certificado dn-* -> cerrado); el REST de metadatos rechaza TLS 1.2
- [mTLS] DeleteBlock/CheckBlocks con certificado dn-* -> PERMISSION_DENIED; con certificado mds-* -> permitido
- [datos] un bloque existente no se sobrescribe (ALREADY_EXISTS); un .part abandonado se borra por GC sin reiniciar el nodo
- [RF3] read(off, 100 B) dentro de un bloque de 256 KiB trajo 65552 B (un segmento GCM de 64 KiB + etiqueta) con GetBlock por rango
- [anexos] tras 8 bloques cortos por anexos, la compactación los reescribió en 1 bloque; versión final 10 con 2 bloques y el contenido intacto
- [instantánea] un handle abierto cuya versión superó la gracia y fue liberada -> 410 SNAPSHOT_EXPIRED
- [UC9] alta de dn-6 -> rebalanceo gradual le trasladó 8 bloques; retiro -> sus bloques se trasladaron (DECOMMISSIONED, 0 bloques); lecturas íntegras durante todo el proceso
- [integridad] un cliente que reporta como corruptas réplicas sanas no las retira: cada nodo recalcula el hash y solo él puede apartar su copia

## RF1_directorios

- mkdir -p, ls, cd, rmdir (409 NOT_EMPTY), mv, mkdir existente (409 EXISTS), rm -r, ls inexistente (404): OK

## T10_gateway

- NGINX (deploy/aws/nginx.conf) delante de 3 MDS: put/get por meta.dfsha.internal con hash idéntico; respuestas directas [200, 307] (307 desde seguidores, el cliente siguió 3 redirecciones); TLS 1.2 rechazado
- caída del líder mds-1 detrás del gateway: put exitoso en 6.45s; con 1 nodo vivo el 503 INSUFFICIENT_REPLICAS llega intacto al cliente

## CLI

- login, mkdir, cd, put, ls, get (hash idéntico), write r+ por offset, cat con rango, append, mv, lock, rm, rmdir: OK

## Rendimiento (bench.csv)

| Exp. | Condición | Operación | MB | Bloque MiB | Paralelo | Clientes | Media s | p95 s | MB/s | Extra |
|---|---|---|---|---|---|---|---|---|---|---|
| E1 | local_R3W2_tls_aes | put | 1.0 | 4.0 | 4 | 1 | 0.0752 | 0.104 | 13.3 | blocks=1 |
| E1 | local_R3W2_tls_aes | get | 1.0 | 4.0 | 4 | 1 | 0.013 | 0.0145 | 76.84 |  |
| E1 | local_R3W2_tls_aes | put | 10.0 | 4.0 | 4 | 1 | 0.2046 | 0.2605 | 48.87 | blocks=3 |
| E1 | local_R3W2_tls_aes | get | 10.0 | 4.0 | 4 | 1 | 0.0677 | 0.1292 | 147.75 |  |
| E1 | local_R3W2_tls_aes | put | 50.0 | 4.0 | 4 | 1 | 0.5369 | 0.589 | 93.12 | blocks=13 |
| E1 | local_R3W2_tls_aes | get | 50.0 | 4.0 | 4 | 1 | 0.1916 | 0.2105 | 260.9 |  |
| E2 | local_R3W2_tls_aes | put | 64.0 | 4.0 | 1 | 1 | 1.0113 | 1.2046 | 63.28 | blocks=16 |
| E2 | local_R3W2_tls_aes | get | 64.0 | 4.0 | 1 | 1 | 0.3253 | 0.3623 | 196.72 |  |
| E2 | local_R3W2_tls_aes | put | 64.0 | 4.0 | 2 | 1 | 0.9361 | 1.1449 | 68.37 | blocks=16 |
| E2 | local_R3W2_tls_aes | get | 64.0 | 4.0 | 2 | 1 | 0.2968 | 0.3403 | 215.66 |  |
| E2 | local_R3W2_tls_aes | put | 64.0 | 4.0 | 4 | 1 | 0.7322 | 0.9918 | 87.41 | blocks=16 |
| E2 | local_R3W2_tls_aes | get | 64.0 | 4.0 | 4 | 1 | 0.3707 | 0.5057 | 172.64 |  |
| E2 | local_R3W2_tls_aes | put | 64.0 | 4.0 | 8 | 1 | 0.7425 | 0.8288 | 86.19 | blocks=16 |
| E2 | local_R3W2_tls_aes | get | 64.0 | 4.0 | 8 | 1 | 0.2869 | 0.3351 | 223.08 |  |
| E3 | local_R3W2_tls_aes | put | 64.0 | 4.0 | 4 | 1 | 0.671 | 0.7189 | 95.38 | blocks=16 |
| E3 | local_R3W2_tls_aes | get | 64.0 | 4.0 | 4 | 1 | 0.2714 | 0.3606 | 235.78 |  |
| E3 | local_R3W2_tls_aes | put | 64.0 | 16.0 | 4 | 1 | 0.7812 | 0.8863 | 81.93 | blocks=4 |
| E3 | local_R3W2_tls_aes | get | 64.0 | 16.0 | 4 | 1 | 0.2677 | 0.3041 | 239.03 |  |
| E3 | local_R3W2_tls_aes | put | 64.0 | 64.0 | 4 | 1 | 0.8355 | 0.8777 | 76.6 | blocks=1 |
| E3 | local_R3W2_tls_aes | get | 64.0 | 64.0 | 4 | 1 | 0.5107 | 0.8287 | 125.33 |  |
| E5 | local_R3W2_tls_aes | get_concurrente | 16.0 | 4.0 | 4 | 1 | 0.0707 | 0.0783 | 226.32 |  |
| E5 | local_R3W2_tls_aes | write_bloques_distintos | 0 | 4.0 | 4 | 1 | 0.0995 | 0.1212 |  | conflictos_pct=0.0;commit_p95_s=0.1209;commits_por_s=10.05 |
| E5 | local_R3W2_tls_aes | write_mismo_bloque | 0 | 4.0 | 4 | 1 | 0.1131 | 0.141 |  | conflictos_pct=0.0;commit_p95_s=0.1407;commits_por_s=8.84 |
| E5 | local_R3W2_tls_aes | get_concurrente | 32.0 | 4.0 | 4 | 2 | 0.1559 | 0.2156 | 205.2 |  |
| E5 | local_R3W2_tls_aes | write_bloques_distintos | 0 | 4.0 | 4 | 2 | 0.2254 | 0.4496 |  | conflictos_pct=0.0;commit_p95_s=0.4484;commits_por_s=8.87 |
| E5 | local_R3W2_tls_aes | write_mismo_bloque | 0 | 4.0 | 4 | 2 | 0.1617 | 0.1703 |  | conflictos_pct=50.0;commit_p95_s=0.1699;commits_por_s=12.37 |
| E5 | local_R3W2_tls_aes | get_concurrente | 64.0 | 4.0 | 4 | 4 | 0.3418 | 0.4197 | 187.25 |  |
| E5 | local_R3W2_tls_aes | write_bloques_distintos | 0 | 4.0 | 4 | 4 | 0.2861 | 0.3241 |  | conflictos_pct=0.0;commit_p95_s=0.303;commits_por_s=13.98 |
| E5 | local_R3W2_tls_aes | write_mismo_bloque | 0 | 4.0 | 4 | 4 | 0.2549 | 0.2878 |  | conflictos_pct=75.0;commit_p95_s=0.2796;commits_por_s=15.69 |
| E5 | local_R3W2_tls_aes | get_concurrente | 128.0 | 4.0 | 4 | 8 | 0.6405 | 0.9002 | 199.84 |  |
| E5 | local_R3W2_tls_aes | write_bloques_distintos | 0 | 4.0 | 4 | 8 | 0.4586 | 0.5282 |  | conflictos_pct=50.0;commit_p95_s=0.4674;commits_por_s=17.44 |
| E5 | local_R3W2_tls_aes | write_mismo_bloque | 0 | 4.0 | 4 | 8 | 0.4096 | 0.4384 |  | conflictos_pct=87.5;commit_p95_s=0.4238;commits_por_s=19.53 |
| E6 | local_R3W2_tls_aes | caida_dn_lectura | 16.0 | 4.0 | 4 | 1 | 0.1199 | 0.2074 | 133.43 | exito_pct=100.0 |
| E6 | local_R3W2_tls_aes | caida_dn_escritura | 16.0 | 4.0 | 4 | 1 | 0.2073 | 0.2279 | 77.18 | exito_pct=100.0 |
| E7 | local_R3W2_tls_aes | caida_lider | 0 | 0 | 1 | 1 | 2.1024 | 2.9457 |  |  |
| E8 | local_R3W2_tls_aes | re_replicacion | 16.0 | 4.0 | 1 | 1 | 2.0177 | 2.0207 | 7.93 |  |
| E8 | local_R3W2_tls_aes | re_replicacion | 48.0 | 4.0 | 1 | 1 | 4.0336 | 5.5439 | 11.9 |  |
| E8 | local_R3W2_tls_aes | re_replicacion | 96.0 | 4.0 | 1 | 1 | 6.6648 | 8.07 | 14.4 |  |
| E4 | local_R3W2 | put | 16.0 | 4.0 | 4 | 1 | 0.2334 | 0.261 | 68.56 | blocks=4 |
| E4 | local_R3W2 | get | 16.0 | 4.0 | 4 | 1 | 0.0757 | 0.0831 | 211.3 |  |
| E4 | local_R1W1 | put | 16.0 | 4.0 | 4 | 1 | 0.1242 | 0.1483 | 128.81 | blocks=4 |
| E4 | local_R1W1 | get | 16.0 | 4.0 | 4 | 1 | 0.0707 | 0.0795 | 226.29 |  |
| E9 | local_3dn | put_agregado | 128.0 | 4.0 | 4 | 8 | 1.472 | 1.6187 | 86.96 | nodos_datos=3 |
| E9 | local_3dn | get_agregado | 128.0 | 4.0 | 4 | 8 | 0.5106 | 0.536 | 250.7 | nodos_datos=3 |
| E9 | local_5dn | put_agregado | 128.0 | 4.0 | 4 | 8 | 1.472 | 1.7637 | 86.96 | nodos_datos=5 |
| E9 | local_5dn | get_agregado | 128.0 | 4.0 | 4 | 8 | 0.7377 | 0.9471 | 173.51 | nodos_datos=5 |
| E9 | local_7dn | put_agregado | 128.0 | 4.0 | 4 | 8 | 1.4864 | 1.6884 | 86.12 | nodos_datos=7 |
| E9 | local_7dn | get_agregado | 128.0 | 4.0 | 4 | 8 | 0.7382 | 0.9708 | 173.4 | nodos_datos=7 |
| E10 | tls_on_aes_on | put | 16.0 | 4.0 | 4 | 1 | 0.2634 | 0.3179 | 60.74 | blocks=4 |
| E10 | tls_on_aes_on | get | 16.0 | 4.0 | 4 | 1 | 0.0767 | 0.0825 | 208.49 |  |
| E10 | tls_on_aes_off | put | 16.0 | 4.0 | 4 | 1 | 0.2004 | 0.2332 | 79.86 | blocks=4 |
| E10 | tls_on_aes_off | get | 16.0 | 4.0 | 4 | 1 | 0.0688 | 0.0771 | 232.52 |  |
| E10 | tls_off_aes_on | put | 16.0 | 4.0 | 4 | 1 | 0.1975 | 0.217 | 81.01 | blocks=4 |
| E10 | tls_off_aes_on | get | 16.0 | 4.0 | 4 | 1 | 0.0701 | 0.0748 | 228.11 |  |
| E10 | tls_off_aes_off | put | 16.0 | 4.0 | 4 | 1 | 0.1752 | 0.185 | 91.35 | blocks=4 |
| E10 | tls_off_aes_off | get | 16.0 | 4.0 | 4 | 1 | 0.0617 | 0.0684 | 259.47 |  |

## Gráficas

![E10_seguridad.png](graficas/E10_seguridad.png)
![E1_tamano.png](graficas/E1_tamano.png)
![E2_paralelismo.png](graficas/E2_paralelismo.png)
![E3_tamano_bloque.png](graficas/E3_tamano_bloque.png)
![E4_replicacion.png](graficas/E4_replicacion.png)
![E5_concurrencia.png](graficas/E5_concurrencia.png)
![E6_E7_fallos.png](graficas/E6_E7_fallos.png)
![E8_rereplicacion.png](graficas/E8_rereplicacion.png)
![E9_escalabilidad.png](graficas/E9_escalabilidad.png)
