#!/usr/bin/env python3
"""Reúne evidencias/*.txt, la tabla de bench.csv y las gráficas en evidencias/RESUMEN.md."""
import csv, glob, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
E = os.path.join(ROOT, "evidencias")
out = ["# Resumen de evidencias", "",
       "Corrida local: 3 MDS + 5 DN como procesos separados que se comunican por TCP con TLS 1.3 / mTLS (incluido Raft) y, en T10, "
       "con el gateway NGINX real. Las cifras de rendimiento de un solo host NO son representativas de AWS: repetir allí (docs/AWS.md).", ""]
for f in sorted(glob.glob(os.path.join(E, "*.txt")), key=lambda p: (len(os.path.basename(p).split("_")[0]), p)):
    out += [f"## {os.path.basename(f)[:-4]}", "", *[f"- {l.rstrip()}" for l in open(f) if l.strip()], ""]
csvp = os.path.join(E, "bench.csv")
if os.path.exists(csvp):
    rows = list(csv.DictReader(open(csvp)))
    out += ["## Rendimiento (bench.csv)", "", "| Exp. | Condición | Operación | MB | Bloque MiB | Paralelo | Clientes | Media s | p95 s | MB/s | Extra |", "|---|---|---|---|---|---|---|---|---|---|---|"]
    out += [f"| {r['experiment']} | {r['condition']} | {r['op']} | {r['size_mb']} | {r['block_mb']} | {r['parallel']} | {r['clients']} | {r['mean_s']} | {r['p95_s']} | {r['throughput_mb_s']} | {r['extra']} |" for r in rows]
    out.append("")
g = sorted(glob.glob(os.path.join(E, "graficas", "*.png")))
if g:
    out += ["## Gráficas", ""] + [f"![{os.path.basename(p)}](graficas/{os.path.basename(p)})" for p in g] + [""]
open(os.path.join(E, "RESUMEN.md"), "w").write("\n".join(out))
print(os.path.join(E, "RESUMEN.md"))
