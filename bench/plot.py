#!/usr/bin/env python3
"""Gráficas de los experimentos E1–E10 a partir de evidencias/bench.csv -> evidencias/graficas/*.png
Uso: python bench/plot.py [--csv evidencias/bench.csv] [--out evidencias/graficas] [--cond-prefix aws]
Media ± desviación estándar de las repeticiones; una sola escala por gráfica (sin ejes dobles)."""
import argparse, csv, os, sys, textwrap
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]        # paleta categórica validada (orden fijo, nunca cíclica)
INK, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
plt.rcParams.update({"figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "axes.edgecolor": GRID, "axes.labelcolor": MUTED,
                     "xtick.color": MUTED, "ytick.color": MUTED, "text.color": INK, "font.size": 10, "axes.titlesize": 12,
                     "axes.titleweight": "bold", "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
                     "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True, "legend.frameon": False})


def load(path, prefix):
    rows = [r for r in csv.DictReader(open(path)) if r.get("experiment")]
    return [r for r in rows if not prefix or r["condition"].startswith(prefix)]


def f(r, k):
    try: return float(r[k])
    except (ValueError, KeyError): return float("nan")


def extra(r, k):
    for kv in (r.get("extra") or "").split(";"):
        if kv.startswith(k + "="): return float(kv.split("=", 1)[1])
    return float("nan")


def thr_err(r):
    """Throughput (MB/s) y su desviación aproximada a partir de la desviación del tiempo."""
    m, s, size = f(r, "mean_s"), f(r, "std_s"), f(r, "size_mb")
    t = size / m if m else float("nan")
    return t, t * s / m if m else 0


def settitle(ax, title):
    """Envuelve títulos largos a ~9 caracteres por pulgada de ancho de la figura (evita que se corten)."""
    w = max(28, int(ax.figure.get_size_inches()[0] * 9))
    ax.set_title(textwrap.fill(title, w), loc="left")


def save(fig, out, name, note):
    w = max(60, int(fig.get_size_inches()[0] * 15))
    lines_n = fig.text(0.01, 0.01, textwrap.fill(note, w), fontsize=8, color=MUTED).get_text().count("\n") + 1
    fig.tight_layout(rect=(0, 0.015 + 0.025 * lines_n, 1, 1))
    p = os.path.join(out, name); fig.savefig(p, dpi=160); plt.close(fig); print(p)


def lines(ax, groups, xkey, ylab, title):
    for i, (label, rs) in enumerate(groups.items()):
        rs = sorted(rs, key=lambda r: f(r, xkey))
        xs = [f(r, xkey) for r in rs]; ys, es = zip(*[thr_err(r) for r in rs])
        ax.errorbar(xs, ys, yerr=es, color=SERIES[i], lw=2, marker="o", ms=6, capsize=3, label=label)
        ax.annotate(f"{ys[-1]:.0f}", (xs[-1], ys[-1]), textcoords="offset points", xytext=(6, 0), color=INK, fontsize=9, va="center")
    ax.set_ylabel(ylab); settitle(ax, title); ax.set_ylim(bottom=0)
    if len(groups) > 1: ax.legend()


def bars(ax, cats, series, ylab, title, fmt="{:.2f}"):
    n = len(series); w = 0.8 / n
    for i, (label, vals) in enumerate(series.items()):
        xs = [k + (i - (n - 1) / 2) * w for k in range(len(cats))]
        ys = [v[0] for v in vals]; es = [v[1] for v in vals]
        ax.bar(xs, ys, width=w * 0.92, yerr=es if any(es) else None, color=SERIES[i], capsize=3, label=label, error_kw={"ecolor": MUTED, "lw": 1})
        for x, y, e in zip(xs, ys, es):
            ax.annotate(fmt.format(y), (x, y + (e or 0)), textcoords="offset points", xytext=(0, 3), ha="center", fontsize=8, color=INK)
    ax.set_xticks(range(len(cats))); ax.set_xticklabels(cats); ax.set_ylabel(ylab); settitle(ax, title)
    if n > 1: ax.legend()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=os.path.join(ROOT, "evidencias", "bench.csv"))
    ap.add_argument("--out", default=os.path.join(ROOT, "evidencias", "graficas"))
    ap.add_argument("--cond-prefix", default="")
    a = ap.parse_args()
    rows = load(a.csv, a.cond_prefix); os.makedirs(a.out, exist_ok=True)
    by = defaultdict(list)
    for r in rows: by[r["experiment"]].append(r)
    note = "Media ± desviación estándar de las repeticiones (CSV: evidencias/bench.csv)."

    if by["E1"]:
        fig, ax = plt.subplots(figsize=(7, 4))
        g = {op: [r for r in by["E1"] if r["op"] == op] for op in ("put", "get")}
        lines(ax, g, "size_mb", "MB/s", "E1 · El throughput crece con el tamaño: la latencia fija pesa en archivos pequeños")
        ax.set_xscale("log"); ax.set_xlabel("tamaño del archivo (MB, escala log)")
        save(fig, a.out, "E1_tamano.png", note)
    if by["E2"]:
        fig, ax = plt.subplots(figsize=(7, 4))
        g = {op: [r for r in by["E2"] if r["op"] == op] for op in ("put", "get")}
        lines(ax, g, "parallel", "MB/s", "E2 · Flujos paralelos por cliente")
        ax.set_xscale("log", base=2); ax.set_xlabel("bloques en paralelo"); ax.set_xticks([1, 2, 4, 8], ["1", "2", "4", "8"])
        save(fig, a.out, "E2_paralelismo.png", note)
    if by["E3"]:
        fig, axs = plt.subplots(1, 2, figsize=(10, 4))
        g = {op: [r for r in by["E3"] if r["op"] == op] for op in ("put", "get")}
        lines(axs[0], g, "block_mb", "MB/s", "E3 · Throughput según el tamaño de bloque")
        axs[0].set_xscale("log", base=2); axs[0].set_xlabel("tamaño de bloque (MiB)")
        put = sorted(g["put"], key=lambda r: f(r, "block_mb"))
        axs[1].bar([f"{f(r, 'block_mb'):.0f}" for r in put], [extra(r, "blocks") for r in put], color=SERIES[0], width=0.5)
        for i, r in enumerate(put): axs[1].annotate(f"{extra(r, 'blocks'):.0f}", (i, extra(r, "blocks")), xytext=(0, 3), textcoords="offset points", ha="center", fontsize=9)
        settitle(axs[1], "Entradas de metadatos por archivo"); axs[1].set_xlabel("tamaño de bloque (MiB)"); axs[1].set_ylabel("bloques")
        save(fig, a.out, "E3_tamano_bloque.png", note)
    if by["E4"]:
        fig, ax = plt.subplots(figsize=(7, 4))
        conds = sorted({r["condition"] for r in by["E4"]})
        ser = {op: [(f(r, "mean_s"), f(r, "std_s")) for c in conds for r in by["E4"] if r["condition"] == c and r["op"] == op] for op in ("put", "get")}
        bars(ax, [c.replace("local_", "") for c in conds], ser, "segundos", "E4 · Costo de la replicación (latencia)", "{:.3f}")
        save(fig, a.out, "E4_replicacion.png", note)
    if by["E5"]:
        fig, axs = plt.subplots(1, 2, figsize=(10, 4))
        gets = sorted([r for r in by["E5"] if r["op"] == "get_concurrente"], key=lambda r: f(r, "clients"))
        lines(axs[0], {"lecturas concurrentes": gets}, "clients", "MB/s agregados", "E5 · Lecturas concurrentes")
        axs[0].set_xlabel("clientes"); axs[0].set_xticks(sorted({f(r, "clients") for r in gets}))
        for i, kind in enumerate(("write_bloques_distintos", "write_mismo_bloque")):
            rs = sorted([r for r in by["E5"] if r["op"] == kind], key=lambda r: f(r, "clients"))
            axs[1].plot([f(r, "clients") for r in rs], [extra(r, "conflictos_pct") for r in rs], color=SERIES[i], lw=2, marker="o", ms=6,
                        label=kind.replace("write_", "escrituras en ").replace("_", " "))
        settitle(axs[1], "E5 · Commits rechazados (409)"); axs[1].set_xlabel("clientes"); axs[1].set_ylabel("% de commits")
        axs[1].set_xticks(sorted({f(r, "clients") for r in gets}))
        axs[1].set_ylim(0, 100); axs[1].legend()
        save(fig, a.out, "E5_concurrencia.png", note + " Mismo bloque: gana uno por commit; bloques distintos: rebase.")
    if by["E6"] or by["E7"]:
        fig, ax = plt.subplots(figsize=(7, 4))
        rs = by["E6"] + by["E7"]
        cats = [r["op"].replace("caida_", "caída ").replace("dn_", "DN · ").replace("_", " ") for r in rs]
        bars(ax, cats, {"tiempo de la operación con fallo": [(f(r, "mean_s"), f(r, "std_s")) for r in rs]}, "segundos",
             "E6/E7 · Operaciones durante la caída de un DN y del líder", "{:.2f}")
        for i, r in enumerate(rs):
            if r["experiment"] == "E6": ax.annotate(f"éxito {extra(r, 'exito_pct'):.0f}%", (i, 0), xytext=(0, -26), textcoords="offset points", ha="center", fontsize=8, color=MUTED)
        save(fig, a.out, "E6_E7_fallos.png", note + " E7: desde el kill del líder hasta la primera operación exitosa.")
    if by["E8"]:
        fig, ax = plt.subplots(figsize=(7, 4))
        rs = sorted(by["E8"], key=lambda r: f(r, "size_mb"))
        ax.errorbar([f(r, "size_mb") for r in rs], [f(r, "mean_s") for r in rs], yerr=[f(r, "std_s") for r in rs], color=SERIES[0], lw=2, marker="o", ms=6, capsize=3)
        ax.set_xlabel("datos del nodo caído (MB)"); ax.set_ylabel("segundos hasta R=3"); ax.set_ylim(bottom=0)
        settitle(ax, "E8 · Re-replicación: tiempo proporcional al volumen")
        save(fig, a.out, "E8_rereplicacion.png", note + " Medido desde que el nodo se declara DEAD.")
    if by["E9"]:
        fig, ax = plt.subplots(figsize=(7, 4))
        g = {op.replace("_agregado", ""): [dict(r, dn=extra(r, "nodos_datos")) for r in by["E9"] if r["op"] == op] for op in ("put_agregado", "get_agregado")}
        lines(ax, g, "dn", "MB/s agregados", "E9 · Escalabilidad con el número de nodos de datos")
        ax.set_xlabel("nodos de datos"); ax.set_xticks([3, 5, 7])
        save(fig, a.out, "E9_escalabilidad.png", note + " En un solo host todos los nodos comparten CPU y disco; repetir en AWS.")
    if by["E10"]:
        fig, axs = plt.subplots(1, 2, figsize=(10, 4))
        conds = ["tls_on_aes_on", "tls_on_aes_off", "tls_off_aes_on", "tls_off_aes_off"]
        conds = [c for c in conds if any(r["condition"].endswith(c) for r in by["E10"])]
        ser = {op: [thr_err(next(r for r in by["E10"] if r["condition"].endswith(c) and r["op"] == op)) for c in conds] for op in ("put", "get")}
        labels = [c.replace("tls_", "TLS ").replace("_aes_", " · AES ").replace("on", "sí").replace("off", "no") for c in conds]
        bars(axs[0], labels, ser, "MB/s", "E10 · Throughput con y sin seguridad", "{:.0f}")
        cpu = {op: [(f(next(r for r in by["E10"] if r["condition"].endswith(c) and r["op"] == op), "cpu_s"), 0) for c in conds] for op in ("put", "get")}
        bars(axs[1], labels, cpu, "s de CPU del cliente por operación", "E10 · CPU del cliente", "{:.3f}")
        for ax in axs: ax.tick_params(axis="x", labelsize=8)
        save(fig, a.out, "E10_seguridad.png", note)


if __name__ == "__main__":
    sys.exit(main())
