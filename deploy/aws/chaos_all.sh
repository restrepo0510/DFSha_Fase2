#!/usr/bin/env bash
# Inyección de fallos desde el GATEWAY sobre cualquier nodo:   bash deploy/aws/chaos_all.sh <nodo> <acción> [arg]
#   acciones: kill | stop | start | delay 3000ms | undelay | partition | heal | corrupt      (ver chaos.sh)
set -euo pipefail; cd "$(dirname "$0")/../.."
N=${1:?nodo (dn-1, mds-2, ...)}; shift
IP=$(awk -v n="$N" '$1==n{print $2}' deploy/aws/.state/ips.txt); [ -n "$IP" ] || { echo "nodo desconocido: $N"; exit 1; }
exec ssh -i "${KEY:-$HOME/.ssh/dfsha-key.pem}" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$(id -un)@$IP" "sudo bash /opt/dfsha/repo/deploy/aws/chaos.sh $*"
