"""CLI dfsha: login, logout, ls, cd, pwd, mkdir, rmdir, rm, mv, stat, put, get, cat, write, append, lock, compact, admin"""
import argparse, json, os, sys, time
from ..common import DFSError
from .sdk import DFSha

CFG = os.path.join(os.getenv("DFSHA_HOME", os.path.expanduser("~/.dfsha")), "config.json")


def load():
    return json.load(open(CFG)) if os.path.exists(CFG) else {}


def save(c):
    os.makedirs(os.path.dirname(CFG), exist_ok=True)
    fd = os.open(CFG, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)      # contiene tokens: solo el dueño puede leerlo
    with os.fdopen(fd, "w") as f:
        json.dump(c, f)


def client():
    c = load()
    d = DFSha(ca=os.getenv("DFSHA_CA") or c.get("ca"), user=c.get("user"), token=c.get("token"), refresh_token=c.get("refresh"))
    d.cwd = c.get("cwd")
    return d, c


def main(argv=None):
    ap = argparse.ArgumentParser(prog="dfsha")
    sp = ap.add_subparsers(dest="cmd", required=True)
    a = sp.add_parser("login"); a.add_argument("user"); a.add_argument("--password", "-p", default=os.getenv("DFSHA_PASSWORD"))
    sp.add_parser("logout")
    for n in ("ls", "cd", "rmdir", "stat"):
        a = sp.add_parser(n); a.add_argument("path", nargs="?", default=".")
    sp.add_parser("pwd")
    a = sp.add_parser("mkdir"); a.add_argument("path"); a.add_argument("-p", action="store_true")
    a = sp.add_parser("rm"); a.add_argument("path"); a.add_argument("-r", action="store_true")
    a = sp.add_parser("mv"); a.add_argument("src"); a.add_argument("dst")
    for n in ("put", "get"):
        a = sp.add_parser(n); a.add_argument("src"); a.add_argument("dst")
        if n == "put": a.add_argument("-x", action="store_true", help="crear (falla si existe)")
        if n == "get": a.add_argument("--version", type=int, default=0)
    a = sp.add_parser("cat"); a.add_argument("path"); a.add_argument("--off", type=int, default=0); a.add_argument("--len", type=int, default=-1)
    a = sp.add_parser("write", help="r+: modifica bytes desde un offset (stdin)"); a.add_argument("path"); a.add_argument("--off", type=int, required=True)
    a = sp.add_parser("append"); a.add_argument("path")
    a = sp.add_parser("lock"); a.add_argument("path"); a.add_argument("--hold", type=float, default=30)
    a.add_argument("--mode", choices=["S", "X"], default="X"); a.add_argument("--block", type=int); a.add_argument("--wait", type=float, default=0)
    a = sp.add_parser("compact"); a.add_argument("path")
    a = sp.add_parser("admin"); a.add_argument("what", choices=["nodes", "blocks", "add", "retire"]); a.add_argument("arg", nargs="?")
    args = ap.parse_args(argv)
    d, c = client()
    try:
        if args.cmd == "login":
            import getpass
            d.login(args.user, args.password or getpass.getpass("password: "))
            c.update(user=args.user, token=d.token, refresh=d.refresh_token, cwd=d.cwd); save(c); print("ok", d.cwd); return 0
        if not d.token:
            print("haga login primero", file=sys.stderr); return 2
        if args.cmd == "logout":
            d.logout(); c.pop("token", None); c.pop("refresh", None); save(c); print("ok"); return 0
        if args.cmd == "ls":
            for e in d.ls(args.path): print(f"{e['type']}  {e.get('size', '-'):>12}  v{e.get('version', '-')}  {e['name']}")
        elif args.cmd == "cd": c["cwd"] = d.cd(args.path); print(c["cwd"])
        elif args.cmd == "pwd": print(d.cwd)
        elif args.cmd == "mkdir": d.mkdir(args.path, args.p)
        elif args.cmd == "rmdir": d.rmdir(args.path)
        elif args.cmd == "rm": d.rm(args.path, args.r)
        elif args.cmd == "mv": d.mv(args.src, args.dst)
        elif args.cmd == "stat": print(json.dumps(d.stat(args.path), indent=1))
        elif args.cmd == "put": print(json.dumps(d.put(args.src, args.dst, args.x)))
        elif args.cmd == "get": print(json.dumps(d.get(args.src, args.dst, args.version)))
        elif args.cmd == "cat":
            h = d.open(args.path, "r"); sys.stdout.buffer.write(h.read(args.off, args.len if args.len >= 0 else h.size)); h.close()
        elif args.cmd == "write":
            with d.open(args.path, "r+") as h: h.write(sys.stdin.buffer.read(), args.off)
        elif args.cmd == "append":
            with d.open(args.path, "a") as h: h.write(sys.stdin.buffer.read())
        elif args.cmd == "lock":
            e = d.lock(args.path, block=args.block, mode=args.mode, wait=args.wait)
            print("lock", args.mode, "bloque" if args.block is not None else "archivo", args.block if args.block is not None else "", "época", e, "mantenido", args.hold, "s", flush=True)
            time.sleep(args.hold); d.unlock(e)
        elif args.cmd == "compact": print(json.dumps(d.compact(args.path)))
        elif args.cmd == "admin":
            if args.what == "nodes": out = d.admin_nodes()
            elif args.what == "blocks": out = d.admin_blocks(args.arg)
            elif args.what == "add": out = d.admin_add_node(args.arg)
            else: out = d.admin_retire_node(args.arg)
            print(json.dumps(out, indent=1))
        c.update(token=d.token, refresh=d.refresh_token, cwd=c.get("cwd", d.cwd)); save(c)   # tokens renovados por refresh
        return 0
    except DFSError as e:
        print(f"ERROR {e.code} {e.name}: {e.msg}", file=sys.stderr); return 1


if __name__ == "__main__":
    sys.exit(main())
