# Despliegue de DFSha en AWS Academy — desde cero

Resultado final: 8 VM (1 gateway, 3 de metadatos, 4 de datos) repartidas en 3 zonas de disponibilidad reales, con TLS/mTLS, y el sistema
funcionando. Tiempo: ~45 min la primera vez (30 de consola + 10–15 de despliegue automático).

| VM | Tipo | Zona | Disco | Rol |
|---|---|---|---|---|
| gateway | t3.medium | us-east-1a | 20 GiB | NGINX, cliente, benchmarks y **desde aquí se despliega todo** |
| mds-1 / mds-2 / mds-3 | t3.small | 1a / 1b / 1c | 10 GiB | Metadatos (Raft) |
| dn-1 / dn-2 / dn-3 / dn-4 | t3.medium | 1a / 1b / 1c / 1a | 20 GiB | Datos |

## PARTE 1 — Laboratorio
1. En Canvas abre *Learner Lab* → **Start Lab**. Espera el círculo verde junto a «AWS» → clic en **AWS**.
2. Arriba a la derecha elige la región **N. Virginia (us-east-1)**.

## PARTE 2 — Clave SSH
3. EC2 → *Pares de claves* → **Crear par de claves**: nombre `dfsha-key`, tipo RSA, formato `.pem` → Crear. Se descarga `dfsha-key.pem`. **Guárdalo: no se puede volver a descargar.**

## PARTE 3 — Grupo de seguridad (el firewall)
4. EC2 → *Grupos de seguridad* → **Crear grupo de seguridad**: nombre `dfsha-sg`, descripción `DFSha`, VPC la predeterminada. Reglas de entrada → agregar **SSH (22)**, origen **Mi IP**. Crear.
5. Abre `dfsha-sg` → *Editar reglas de entrada* → *Agregar regla*: tipo **Todo el tráfico**, origen **Personalizado** → elige **el mismo grupo `dfsha-sg`** → Guardar.
   (Permite que las VM se hablen entre sí —SSH, Raft, REST, gRPC— sin abrir nada al mundo. Si tu IP cambia otro día, edita la regla SSH y vuelve a elegir «Mi IP».)

## PARTE 4 — Lanzar las 8 instancias
6. EC2 → **Lanzar instancias**. Primero el gateway:
   * Nombre: `gateway` · AMI: **Ubuntu Server 24.04 LTS** (x86_64) · Tipo: **t3.medium** · Par de claves: `dfsha-key`.
   * *Configuración de red* → **Editar**: VPC predeterminada · **Subred: la de us-east-1a** · Asignar IP pública automáticamente: **Habilitar** · Grupos de seguridad: *Seleccionar existente* → `dfsha-sg`.
   * Almacenamiento: **20 GiB gp3** (en *Avanzado*, cifrado: Sí, si te lo permite) → **Lanzar instancia**.
7. Crea las otras 7 con **Acciones → Imágenes y plantillas → Lanzar más como esta** (copia clave, AMI y grupo de seguridad). En cada una cambia solo nombre, tipo, **subred (zona)** y disco:

   | Nombre | Tipo | Zona (subred) | Disco |
   |---|---|---|---|
   | mds-1 | t3.small | us-east-1a | 10 GiB |
   | mds-2 | t3.small | us-east-1b | 10 GiB |
   | mds-3 | t3.small | us-east-1c | 10 GiB |
   | dn-1 | t3.medium | us-east-1a | 20 GiB |
   | dn-2 | t3.medium | us-east-1b | 20 GiB |
   | dn-3 | t3.medium | us-east-1c | 20 GiB |
   | dn-4 | t3.medium | us-east-1a | 20 GiB |

   Si una zona rechaza el tipo, usa otra; lo importante es que haya **3 zonas distintas** y que anotes la letra real de cada VM.
8. EC2 → *Instancias*: 8 en «En ejecución». Espera **2/2 comprobaciones** en todas.

## PARTE 5 — IP fija para el gateway
9. EC2 → *IP elásticas* → **Asignar dirección IP elástica** → Asignar. Selecciónala → *Acciones* → **Asociar dirección IP elástica** → instancia `gateway` → Asociar. Apunta esa IP (la llamo `IP_GATEWAY`).

## PARTE 6 — Anotar las IP privadas
10. En *Instancias* pulsa el engranaje ⚙ y activa las columnas **Dirección IPv4 privada** y **Zona de disponibilidad**. Apunta las 8 IP privadas (empiezan por `172.31.`) y comprueba la zona de cada una.

## PARTE 7 — Desde tu PC (terminal de Ubuntu/WSL)
11. ```bash
    mkdir -p ~/.ssh && cp /mnt/c/Users/juanj/Downloads/dfsha-key.pem ~/.ssh/ && chmod 400 ~/.ssh/dfsha-key.pem
    cp "/mnt/c/Users/juanj/Downloads/dfsha_repo_aws.zip" ~/
    ```
12. Prueba: `ssh -i ~/.ssh/dfsha-key.pem ubuntu@IP_GATEWAY` → escribe `yes` → debe salir `ubuntu@ip-172-31-...:~$` → `exit`.
    (Si se cuelga: regla SSH del paso 4. Si «Permission denied»: falta `chmod 400`.)
13. Sube el zip y la clave:
    ```bash
    scp -i ~/.ssh/dfsha-key.pem ~/dfsha_repo_aws.zip ubuntu@IP_GATEWAY:~
    scp -i ~/.ssh/dfsha-key.pem ~/.ssh/dfsha-key.pem ubuntu@IP_GATEWAY:~/.ssh/dfsha-key.pem
    ```

## PARTE 8 — Dentro del gateway
14. ```bash
    ssh -i ~/.ssh/dfsha-key.pem ubuntu@IP_GATEWAY
    chmod 400 ~/.ssh/dfsha-key.pem
    sudo apt-get update && sudo apt-get install -y unzip
    unzip -q dfsha_repo_aws.zip && cd dfsha_repo
    ```
15. Completa el inventario: `nano deploy/aws/inventory.txt` y reemplaza cada `IP_PRIVADA` por la IP privada real (la **letra de zona** debe coincidir con la zona real de esa VM). Guarda con Ctrl+O, Enter, Ctrl+X. Comprueba: `cat deploy/aws/inventory.txt` y `grep IP_PRIVADA deploy/aws/inventory.txt` (no debe imprimir nada).
16. Comprueba que el gateway llega a todas:
    ```bash
    while read n ip z r _; do [[ "$n" == \#* || -z "$n" ]] && continue; echo -n "$n: "; ssh -i ~/.ssh/dfsha-key.pem -o StrictHostKeyChecking=no -o ConnectTimeout=8 ubuntu@$ip hostname; done < deploy/aws/inventory.txt
    ```
    Deben salir 8 líneas. Si alguna falla: paso 5, o esa VM no está 2/2.
17. **Desplegar:** `bash deploy/aws/deploy_all.sh 2>&1 | tee deploy.log` (8–15 min; se puede repetir si se corta). Etapas 1/6 a 6/6. Debe terminar con los 4 DN `ALIVE` y `PRUEBA OK: 40 MB ... hash idéntico`.

## PARTE 9 — Usarlo
18. ```bash
    A=$(grep ^alice= deploy/aws/.state/credenciales.txt | cut -d= -f2); P=$(grep ^admin= deploy/aws/.state/credenciales.txt | cut -d= -f2)
    export DFSHA_PASSWORD=$A; dfsha login alice
    dfsha mkdir -p docs; head -c 100M /dev/urandom > g.bin; sha256sum g.bin
    dfsha put g.bin docs/g.bin; dfsha get docs/g.bin g2.bin; sha256sum g2.bin
    alias dfa='DFSHA_STATE=/var/lib/dfsha-admin dfsha'; DFSHA_PASSWORD=$P dfa login admin; dfa admin nodes; dfa admin blocks docs/g.bin
    ```
19. Fallos reales: `bash deploy/aws/chaos_all.sh dn-2 kill` (y `start`), `dn-3 delay 5000ms` (`undelay`), `dn-3 partition` (`heal`), `mds-1 kill`, `dn-4 corrupt`. Más pruebas en `docs/AWS.md`.

## PARTE 10 — Cuidar el laboratorio
* Al terminar de trabajar: EC2 → seleccionar las 8 → *Estado de la instancia* → **Detener** (no «Terminar», que las borra). Cuida el presupuesto del laboratorio.
* Al volver: Start Lab → iniciar las 8 → entrar al gateway por `IP_GATEWAY` → `cd ~/dfsha_repo && bash deploy/aws/deploy_all.sh status` (los contenedores arrancan solos; si no: `... restart`). Las IP privadas se conservan.
* Al terminar el proyecto, **libera la IP elástica** (IP elásticas → Acciones → Liberar).

## Problemas frecuentes
| Síntoma | Solución |
|---|---|
| Se cuelga el SSH | Reglas del grupo `dfsha-sg` (pasos 4–5) o la instancia no está en 2/2 |
| `Permission denied (publickey)` | Falta `chmod 400`, o la instancia no es Ubuntu (usuario `ec2-user`) |
| `complete inventory.txt: ... IP_PRIVADA` | Falta reemplazar alguna IP (paso 15) |
| `INSUFFICIENT_REPLICAS` | Menos de 2 DN o 2 zonas vivas: `dfa admin nodes` |
| `logs` de un nodo | `bash deploy/aws/deploy_all.sh logs mds-1` |
