#!/usr/bin/env bash
# Despliegue completo de DFSha en AWS Academy. SE EJECUTA EN EL GATEWAY (única VM con acceso desde fuera).
#   bash deploy/aws/deploy_all.sh            # todo: preparar -> descubrir IP -> PKI -> enviar -> arrancar -> verificar
#   bash deploy/aws/deploy_all.sh status     # estado de contenedores y de los nodos del clúster
#   bash deploy/aws/deploy_all.sh logs dn-2  # últimas líneas del log de un nodo
#   bash deploy/aws/deploy_all.sh restart    # re-crear contenedores (tras cambiar código/env), sin tocar datos
#   DRY=1 bash deploy/aws/deploy_all.sh      # solo muestra lo que haría (no ejecuta nada remoto)
# Requisitos: clave SSH en ~/.ssh/dfsha-key.pem (o KEY=...), el repo en el gateway, y el grupo de seguridad dfsha-sg con
# "todo el tráfico desde el propio dfsha-sg" (ver docs/DESPLIEGUE_PASO_A_PASO.md).
set -euo pipefail
cd "$(dirname "$0")/../.."
REPO=$(pwd); ST=deploy/aws/.state; mkdir -p "$ST"; chmod 700 "$ST"
KEY=${KEY:-$HOME/.ssh/dfsha-key.pem}; U=$(id -un); DRY=${DRY:-0}
INV=deploy/aws/inventory.txt; IPS=$ST/ips.txt
SSHO=(-i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o ServerAliveInterval=15 -o LogLevel=ERROR)
RAFT=7000; REST=8443; MGRPC=9444; DNP=9443
say(){ printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
die(){ printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }
run(){ if [ "$DRY" = 1 ]; then echo "  [dry] $*"; else "$@"; fi; }

# ---------------------------------------------------------------- inventario
declare -A DNS ZONE ROLE PRIV
NAMES=()
while read -r n d z r _; do
  [[ -z "${n:-}" || "$n" == \#* ]] && continue
  NAMES+=("$n"); DNS[$n]=$d; ZONE[$n]=$z; ROLE[$n]=$r
done < "$INV"
MDS=(); DN=()
for n in "${NAMES[@]}"; do case "${ROLE[$n]}" in mds) MDS+=("$n");; dn) DN+=("$n");; esac; done
for n in "${NAMES[@]}"; do [[ "${DNS[$n]}" == *IP_PRIVADA* ]] && die "complete deploy/aws/inventory.txt: $n todavía tiene IP_PRIVADA"; done
[ ${#MDS[@]} -eq 3 ] || die "se esperan 3 MDS en $INV"; [ ${#DN[@]} -ge 3 ] || die "se esperan >= 3 DN en $INV"
load_ips(){ PRIV=(); [ -f "$IPS" ] && while read -r n ip; do PRIV[$n]=$ip; done < "$IPS" || true; }
tgt(){ echo "${PRIV[$1]:-${DNS[$1]}}"; }
rsh(){ local n=$1; shift; if [ "$DRY" = 1 ]; then echo "  [dry] ssh $n: $*"; else ssh "${SSHO[@]}" "$U@$(tgt "$n")" "$@"; fi; }
is_private(){ [[ "$1" =~ ^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.) ]]; }
INSTALL_DOCKER='command -v docker >/dev/null || { if command -v apt-get >/dev/null; then sudo apt-get update -y && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io iproute2 iptables curl; else sudo dnf install -y docker iproute-tc iptables-nft curl; fi; }; sudo systemctl enable --now docker'

# ---------------------------------------------------------------- 1. preparar el gateway (Docker + imagen)
prep_gateway(){
  say "1/6 Gateway: Docker e imagen dfsha:latest"
  [ -f "$KEY" ] || [ "$DRY" = 1 ] || die "falta la clave SSH $KEY (cópiela al gateway con scp y chmod 400)"
  run bash -c "$INSTALL_DOCKER"
  run sudo docker build -q -t dfsha:latest -f deploy/Dockerfile .
}

# ---------------------------------------------------------------- 2. descubrir IP privadas
discover(){
  say "2/6 Descubriendo IP privadas (DNS público -> IP privada dentro de la VPC)"
  if [ "$DRY" = 1 ] && [ -s "$IPS" ]; then load_ips; echo "  [dry] usando $IPS"; return; fi
  : > "$IPS"
  for n in "${NAMES[@]}"; do
    local ip=""
    if [ "${ROLE[$n]}" = gateway ]; then ip=$(ip -4 route get 1.1.1.1 | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1); exit}}')
    else ip=$(getent ahostsv4 "${DNS[$n]}" | awk 'NR==1{print $1}')
      if ! is_private "${ip:-x}"; then   # el DNS no resolvió a IP privada: preguntar por SSH
        ip=$(ssh "${SSHO[@]}" "$U@${DNS[$n]}" "ip -4 route get 1.1.1.1 | awk '{for(i=1;i<=NF;i++) if(\$i==\"src\"){print \$(i+1); exit}}'") \
          || die "no pude conectar por SSH a $n (${DNS[$n]}). Revise dfsha-sg: 'todo el tráfico' desde el propio grupo y SSH desde su IP."
      fi
    fi
    is_private "$ip" || die "IP inválida para $n: '$ip'"
    echo "$n $ip" >> "$IPS"; printf '  %-8s %s\n' "$n" "$ip"
  done
  load_ips
}

# ---------------------------------------------------------------- 3. env, contraseñas y PKI
mkpw(){ head -c 18 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 14; }
make_env(){
  say "3/6 Configuración, contraseñas y PKI (CA privada + un certificado por nodo)"
  if [ ! -f "$ST/credenciales.txt" ]; then
    printf 'alice=%s\nbob=%s\nadmin=%s\nraft=%s\n' "$(mkpw)" "$(mkpw)" "$(mkpw)" "$(mkpw)" > "$ST/credenciales.txt"; chmod 600 "$ST/credenciales.txt"
  fi
  # shellcheck disable=SC1090
  source <(sed 's/^/CRED_/' "$ST/credenciales.txt")
  local cl="" gr=""
  for n in "${MDS[@]}"; do cl+="${cl:+,}$n=${PRIV[$n]}:$RAFT:$REST:$MGRPC"; gr+="${gr:+,}${PRIV[$n]}:$MGRPC"; done
  local common="DFSHA_HB_S=3
DFSHA_SUSPECT_S=10
DFSHA_DEAD_S=30
DFSHA_GRACE_S=600
DFSHA_BLOCK_SIZE=67108864
DFSHA_KEYS=/keys
DFSHA_CERTS=/certs"
  printf '%s\nDFSHA_MDS_CLUSTER=%s\nDFSHA_DN_ADMISSION=auto\nDFSHA_REBALANCE=1\nDFSHA_USERS=alice:%s,bob:%s,admin:%s:admin\nDFSHA_RAFT_PASSWORD=%s\n' \
    "$common" "$cl" "$CRED_alice" "$CRED_bob" "$CRED_admin" "$CRED_raft" > "$ST/mds.env"
  printf '%s\nDFSHA_MDS_GRPC=%s\n' "$common" "$gr" > "$ST/dn.env"; chmod 600 "$ST/mds.env"
  # nginx
  sed -e "s/MDS1_IP/${PRIV[${MDS[0]}]}/;s/MDS2_IP/${PRIV[${MDS[1]}]}/;s/MDS3_IP/${PRIV[${MDS[2]}]}/" deploy/aws/nginx.conf > "$ST/nginx.conf"
  # PKI: SAN = IP privada + nombre; gateway además con su IP pública y meta.dfsha.internal
  local specs=() gwpub=${DNS[gateway]}
  gwpub=$(echo "$gwpub" | sed -E 's/^ec2-([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)\..*/\1.\2.\3.\4/')
  for n in "${MDS[@]}"; do specs+=("$n:${PRIV[$n]},$n,mds.dfsha.internal"); done
  for n in "${DN[@]}"; do specs+=("$n:${PRIV[$n]},$n"); done
  local gsan="${PRIV[gateway]}"; [ "$gwpub" != "${PRIV[gateway]}" ] && gsan+=",$gwpub"
  specs+=("gateway:$gsan,meta.dfsha.internal,localhost")
  if [ -d "$ST/pki" ]; then echo "  (PKI ya existe en $ST/pki: se reutiliza; bórrela para regenerar)"; return; fi
  if [ "$DRY" = 1 ]; then python3 scripts/gen_certs.py "$ST/pki" "${specs[@]}" >/dev/null; echo "  [dry] PKI generada localmente (${#specs[@]} certificados)"
  else sudo docker run --rm -v "$REPO/$ST:/out" dfsha:latest python scripts/gen_certs.py /out/pki "${specs[@]}" >/dev/null
       sudo chown -R "$U:$(id -gn)" "$ST/pki"; fi
}

# ---------------------------------------------------------------- 4. enviar imagen, repo, certificados
ship_one(){
  local n=$1; local role=${ROLE[$n]}; local lid rid
  rsh "$n" "$INSTALL_DOCKER; sudo mkdir -p /opt/dfsha/{data,blocks,keys,keys-dn,certs,repo}; sudo chown -R $U /opt/dfsha/repo"
  if [ "$n" != gateway ]; then
    lid=$(sudo docker images -q dfsha:latest 2>/dev/null || echo local); rid=$(rsh "$n" "sudo docker images -q dfsha:latest" 2>/dev/null || true)
    if [ "$DRY" != 1 ] && [ "$lid" != "$rid" ]; then sudo docker save dfsha:latest | gzip | ssh "${SSHO[@]}" "$U@$(tgt "$n")" 'gunzip | sudo docker load' >/dev/null
    else echo "  [$n] imagen: $([ "$DRY" = 1 ] && echo dry || echo 'ya cargada')"; fi
  fi
  if [ "$DRY" != 1 ]; then
    tar czf - --exclude=.git --exclude=.venv --exclude=evidencias --exclude=.state --exclude=__pycache__ --exclude=.pytest_cache . | ssh "${SSHO[@]}" "$U@$(tgt "$n")" 'tar xzf - -C /opt/dfsha/repo'
    tar -C "$ST/pki/$n" -cf - . | ssh "${SSHO[@]}" "$U@$(tgt "$n")" 'sudo tar xf - -C /opt/dfsha/certs'
    case "$role" in
      mds) tar -C "$ST/pki/keys" -cf - . | ssh "${SSHO[@]}" "$U@$(tgt "$n")" 'sudo tar xf - -C /opt/dfsha/keys'
           cat "$ST/mds.env" | ssh "${SSHO[@]}" "$U@$(tgt "$n")" 'sudo tee /opt/dfsha/node.env >/dev/null && sudo chmod 600 /opt/dfsha/node.env';;
      dn)  tar -C "$ST/pki/keys-dn" -cf - . | ssh "${SSHO[@]}" "$U@$(tgt "$n")" 'sudo tar xf - -C /opt/dfsha/keys-dn'
           cat "$ST/dn.env" | ssh "${SSHO[@]}" "$U@$(tgt "$n")" 'sudo tee /opt/dfsha/node.env >/dev/null';;
      gateway) cp "$ST/pki/ca.pem" /tmp/ca.pem; sudo install -m 644 /tmp/ca.pem /opt/dfsha/ca.pem
               sudo install -m 644 "$ST/nginx.conf" /opt/dfsha/nginx.conf;;
    esac
    rsh "$n" "grep -q meta.dfsha.internal /etc/hosts || echo '${PRIV[gateway]} meta.dfsha.internal' | sudo tee -a /etc/hosts >/dev/null"
  else echo "  [dry] enviar imagen, repo, certificados y env a $n"; fi
}
ship(){
  say "4/6 Enviando imagen Docker, código, certificados y configuración a cada VM (en paralelo)"
  [ "$DRY" = 1 ] || sudo mkdir -p /opt/dfsha/{data,blocks,keys,keys-dn,certs,repo}
  local pids=() n
  for n in "${NAMES[@]}"; do ( ship_one "$n" > "$ST/ship_$n.log" 2>&1 && echo "  OK   $n" || { echo "  FALLÓ $n (ver $ST/ship_$n.log)"; exit 1; } ) & pids+=($!); done
  local bad=0; for p in "${pids[@]}"; do wait "$p" || bad=1; done; [ $bad = 0 ] || die "falló el envío a alguna VM"
}

# ---------------------------------------------------------------- 5. arrancar contenedores
start_one(){
  local n=$1; local role=${ROLE[$n]}
  case "$role" in
    mds) rsh "$n" "sudo docker rm -f dfsha-mds >/dev/null 2>&1; sudo docker run -d --name dfsha-mds --restart unless-stopped --network host --env-file /opt/dfsha/node.env -e DFSHA_MDS_ID=$n -e DFSHA_DATA_DIR=/data -v /opt/dfsha/data:/data -v /opt/dfsha/keys:/keys:ro -v /opt/dfsha/certs:/certs:ro dfsha:latest python -m dfsha.mds.server";;
    dn)  rsh "$n" "sudo docker rm -f dfsha-dn >/dev/null 2>&1; sudo docker run -d --name dfsha-dn --restart unless-stopped --network host --cap-add NET_ADMIN --env-file /opt/dfsha/node.env -e DFSHA_DN_ID=$n -e DFSHA_DN_PORT=$DNP -e DFSHA_DN_ADDR=${PRIV[$n]}:$DNP -e DFSHA_ZONE=${ZONE[$n]} -e DFSHA_DATA_DIR=/blocks -v /opt/dfsha/blocks:/blocks -v /opt/dfsha/keys-dn:/keys:ro -v /opt/dfsha/certs:/certs:ro dfsha:latest python -m dfsha.dn.server";;
    gateway) run bash -c "sudo docker rm -f nginx >/dev/null 2>&1; sudo docker run -d --name nginx --restart unless-stopped --network host -v /opt/dfsha/nginx.conf:/etc/nginx/nginx.conf:ro -v /opt/dfsha/certs:/etc/nginx/certs:ro nginx:1.27 >/dev/null";;
  esac
}
install_client(){
  say "   Instalando los comandos 'dfsha' y 'dfsha-bench' en el gateway"
  local w=/tmp/dfsha.wrapper b=/tmp/dfsha-bench.wrapper
  cat > $w <<EOF
#!/usr/bin/env bash
# Cliente DFSha en contenedor. Variables útiles: DFSHA_PASSWORD, DFSHA_BLOCK_SIZE (por defecto 8 MiB), DFSHA_ZONE
T=""; [ -t 0 ] && T="-t"; sudo mkdir -p "\${DFSHA_STATE:-/var/lib/dfsha-client}"
exec sudo docker run --rm -i \$T --network host --add-host meta.dfsha.internal:${PRIV[gateway]} \\
  -e DFSHA_META=https://meta.dfsha.internal -e DFSHA_CA=/ca.pem -e DFSHA_HOME=/state -e DFSHA_ZONE=\${DFSHA_ZONE:-a} \\
  -e DFSHA_BLOCK_SIZE=\${DFSHA_BLOCK_SIZE:-8388608} -e "DFSHA_PASSWORD=\${DFSHA_PASSWORD:-}" \\
  -v /opt/dfsha/ca.pem:/ca.pem:ro -v \${DFSHA_STATE:-/var/lib/dfsha-client}:/state -v "\$PWD":/work -w /work dfsha:latest python -m dfsha.client "\$@"
EOF
  cat > $b <<EOF
#!/usr/bin/env bash
# Benchmarks (bench/bench.py) desde el gateway; escribe en /opt/dfsha/evidencias/bench.csv
mkdir -p /opt/dfsha/evidencias
exec sudo docker run --rm -i --network host --add-host meta.dfsha.internal:${PRIV[gateway]} \\
  -e DFSHA_META=https://meta.dfsha.internal -e DFSHA_CA=/ca.pem -e DFSHA_ZONE=\${DFSHA_ZONE:-a} \\
  -v /opt/dfsha/ca.pem:/ca.pem:ro -v /opt/dfsha/evidencias:/app/evidencias dfsha:latest python bench/bench.py "\$@"
EOF
  run sudo install -m 755 $w /usr/local/bin/dfsha; run sudo install -m 755 $b /usr/local/bin/dfsha-bench; run sudo mkdir -p /var/lib/dfsha-client
}
start(){
  say "5/6 Arrancando MDS (Raft), luego DN, luego gateway NGINX"
  local n
  for n in "${MDS[@]}"; do start_one "$n" & done; wait
  sleep 6
  for n in "${DN[@]}"; do start_one "$n" & done; wait
  start_one gateway; install_client
}

# ---------------------------------------------------------------- 6. verificar
verify(){
  say "6/6 Verificación: login, nodos y prueba de ida y vuelta con hash"
  [ "$DRY" = 1 ] && { echo "  [dry] (verificación omitida)"; return; }
  source <(sed 's/^/CRED_/' "$ST/credenciales.txt")
  local i; for i in $(seq 1 40); do curl -sk --max-time 3 https://meta.dfsha.internal/healthz 2>/dev/null | grep -q '"ready":true' && break; sleep 3; done
  curl -sk https://meta.dfsha.internal/healthz | grep -q ready || die "el servicio de metadatos no responde: revise 'bash deploy/aws/deploy_all.sh logs mds-1'"
  export DFSHA_STATE=/var/lib/dfsha-admin; DFSHA_PASSWORD=$CRED_admin dfsha login admin >/dev/null
  for i in $(seq 1 30); do [ "$(dfsha admin nodes 2>/dev/null | grep -c '"ALIVE"')" -ge ${#DN[@]} ] && break; sleep 3; done
  dfsha admin nodes | python3 -c "import sys,json;d=json.load(sys.stdin);print('  líder:',d.get('leader'));[print('  ',n['dn_id'],'zona',n['zone'],n['state'],n.get('admin',''),'bloques',n.get('blocks',0)) for n in d['nodes']]"
  unset DFSHA_STATE
  local t; t=$(mktemp -d); head -c 40000000 /dev/urandom > "$t/f.bin"; local h1; h1=$(sha256sum "$t/f.bin" | cut -d' ' -f1)
  ( cd "$t"; DFSHA_PASSWORD=$CRED_alice dfsha login alice >/dev/null; dfsha mkdir -p smoke; dfsha put f.bin smoke/f.bin >/dev/null; dfsha get smoke/f.bin g.bin >/dev/null )
  local h2; h2=$(sha256sum "$t/g.bin" | cut -d' ' -f1)
  [ "$h1" = "$h2" ] && echo "  PRUEBA OK: 40 MB subidos y recuperados con hash idéntico ($h1)" || die "hash distinto: $h1 vs $h2"
  rm -rf "$t"
  echo; echo "Usuarios y contraseñas: $ST/credenciales.txt   (alice/bob = usuarios, admin = rol de clúster)"
  echo "Siguiente: docs/DESPLIEGUE_PASO_A_PASO.md (sección 'Demostraciones')"
}

# ---------------------------------------------------------------- estado / logs
status(){
  load_ips
  for n in "${NAMES[@]}"; do printf '%-8s %-14s ' "$n" "${PRIV[$n]:-?}"; rsh "$n" "sudo docker ps --format '{{.Names}} {{.Status}}' | tr '\n' ' '; echo" 2>&1 | tail -1; done
  echo; DFSHA_STATE=/var/lib/dfsha-admin dfsha admin nodes 2>&1 | head -40
}

CMD=${1:-all}
case "$CMD" in
  all) load_ips; prep_gateway; discover; make_env; ship; start; verify;;
  restart) load_ips; [ -f "$IPS" ] || die "primero ejecute 'all'"; make_env; for n in "${MDS[@]}"; do start_one "$n" & done; wait; sleep 6; for n in "${DN[@]}"; do start_one "$n" & done; wait; start_one gateway;;
  status) status;;
  logs) load_ips; n=${2:?nombre del nodo}; c=dfsha-${ROLE[$n]}; [ "${ROLE[$n]}" = gateway ] && c=nginx; rsh "$n" "sudo docker logs --tail 60 $c 2>&1";;
  *) sed -n 2,8p "$0";;
esac
