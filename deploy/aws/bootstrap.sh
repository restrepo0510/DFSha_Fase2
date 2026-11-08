#!/usr/bin/env bash
# Idempotente. Ejecutar en CADA VM (Ubuntu 22.04/24.04) como root:  ROLE=mds|dn|gateway bash bootstrap.sh
set -euo pipefail
ROLE=${ROLE:?defina ROLE=mds|dn|gateway}
command -v docker >/dev/null || { apt-get update -y && apt-get install -y docker.io docker-compose-v2 iproute2 iptables; systemctl enable --now docker; }
mkdir -p /opt/dfsha/{data,blocks,keys,keys-dn,certs}
cd /opt/dfsha/repo 2>/dev/null || { echo "Copie el repositorio a /opt/dfsha/repo (git clone o scp)"; exit 1; }
docker build -t dfsha:latest -f deploy/Dockerfile .
# nombre estable para clientes (si Route 53 privado no está disponible en el laboratorio)
source deploy/aws/env
grep -q meta.dfsha.internal /etc/hosts || echo "$GATEWAY_IP meta.dfsha.internal" >> /etc/hosts
case "$ROLE" in
  mds) : "${MDS_ID:?MDS_ID=mds-N}"; MDS_ID=$MDS_ID docker compose -f deploy/aws/compose.mds.yml --env-file deploy/aws/env up -d ;;
  dn)  : "${DN_ID:?}" "${DN_IP:?}" "${DN_ZONE:?}"; DN_ID=$DN_ID DN_IP=$DN_IP DN_ZONE=$DN_ZONE docker compose -f deploy/aws/compose.dn.yml --env-file deploy/aws/env up -d ;;
  gateway) sed -e "s/MDS1_IP/$MDS1_IP/;s/MDS2_IP/$MDS2_IP/;s/MDS3_IP/$MDS3_IP/" deploy/aws/nginx.conf > deploy/aws/nginx.rendered.conf
           docker rm -f nginx 2>/dev/null || true; docker run -d --restart unless-stopped --name nginx --network host -v /opt/dfsha/repo/deploy/aws/nginx.rendered.conf:/etc/nginx/nginx.conf:ro -v /opt/dfsha/certs:/etc/nginx/certs:ro nginx:1.27 ;;
esac
echo "OK rol=$ROLE"
