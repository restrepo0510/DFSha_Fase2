#!/usr/bin/env bash
# Ejecutar UNA vez en la máquina del operador/bastión. Genera la CA y un certificado por nodo con su IP privada en el SAN.
set -e; cd "$(dirname "$0")/../.."; source deploy/aws/env
OUT=${1:-./pki}
python3 scripts/gen_certs.py "$OUT" \
  "mds-1:$MDS1_IP,mds-1,mds.dfsha.internal" "mds-2:$MDS2_IP,mds-2,mds.dfsha.internal" "mds-3:$MDS3_IP,mds-3,mds.dfsha.internal" \
  "dn-1:$DN1_IP,dn-1" "dn-2:$DN2_IP,dn-2" "dn-3:$DN3_IP,dn-3" "dn-4:$DN4_IP,dn-4" "dn-5:$DN5_IP,dn-5" \
  "gateway:$GATEWAY_IP,meta.dfsha.internal,localhost"
echo "Distribuya: pki/<nodo>/ -> /opt/dfsha/certs en cada VM; pki/keys -> VM de MDS (/opt/dfsha/keys, KEK 0400); pki/keys-dn -> VM de DN; pki/ca.pem -> clientes"
