#!/usr/bin/env bash
# Mide 2+ condiciones en local: E1+E2 (tamaño y paralelismo), E4 (R=1/W=1 vs R=3/W=2), E10 (TLS+AES encendido vs TLS apagado).
set -e; cd "$(dirname "$0")/.."; REPS=${REPS:-3}
rm -f evidencias/bench.csv
python3 bench/bench.py --local --reps $REPS --cond "E1E2_R3W2_tls" --sizes 1,10,40 --parallel 1,4
python3 bench/bench.py --local --reps $REPS --cond "E4_R1W1_tls" --rw 1,1 --sizes 10 --parallel 4
python3 bench/bench.py --local --reps $REPS --cond "E10_R3W2_sin_tls" --tls 0 --sizes 10 --parallel 4
