#!/usr/bin/env bash
# Inyección de fallos REALES en una VM (ejecutar en la VM afectada, como root).
#   chaos.sh kill            -> docker kill del contenedor dfsha (caída abrupta de un DN o de un MDS)
#   chaos.sh stop|start      -> docker stop/start del contenedor dfsha
#   chaos.sh delay 3000ms    -> réplica retrasada (tc netem)       | chaos.sh undelay
#   chaos.sh partition       -> desconexión: DROP del tráfico gRPC (9443/9444), Raft (7000) y REST (8443) | chaos.sh heal
#   chaos.sh corrupt         -> altera 1 byte de un bloque (demo de integridad / scrubbing)
set -e; IF=${IF:-$(ip route show default | awk '{print $5; exit}')}
C=$(docker ps -aq --filter ancestor=dfsha:latest | head -1)
RULES=("INPUT -p tcp -m multiport --dports 9443,9444,7000,8443 -j DROP" "OUTPUT -p tcp -m multiport --dports 9443,9444,7000,8443 -j DROP")
case "$1" in
  kill) docker kill "$C" ;;
  stop) docker stop "$C" ;;
  start) docker start "$C" ;;
  delay) tc qdisc add dev "$IF" root netem delay "${2:-3000ms}" ;;
  undelay) tc qdisc del dev "$IF" root netem ;;
  partition) for r in "${RULES[@]}"; do iptables -A $r; done ;;
  heal) for r in "${RULES[@]}"; do iptables -D $r 2>/dev/null || true; done ;;   # solo quita las reglas propias
  corrupt) f=$(ls /opt/dfsha/blocks/*.blk | head -1); printf '\xff' | dd of="$f" bs=1 seek=100 conv=notrunc 2>/dev/null; echo "corrompido $f" ;;
  *) sed -n 2,8p "$0" ;;
esac
