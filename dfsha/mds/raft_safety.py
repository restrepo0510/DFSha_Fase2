"""Corrección de seguridad para PySyncObj 0.3.x.

Ante un AppendEntries retrasado o retransmitido (prevLogIdx menor que el último índice del seguidor), PySyncObj borra
TODAS las entradas posteriores a prevLogIdx y vuelve a agregar las recibidas, aunque no haya conflicto, y lo advierte con
"truncating already-applied log entries ... state machine may have diverged". Raft (§5.3) solo permite borrar desde la
primera entrada en CONFLICTO (mismo índice, distinto término). Borrar entradas ya aplicadas es peligroso: el seguidor
pierde entradas confirmadas que ya había reconocido y, si en ese momento cae el líder, podría elegirse un líder sin ellas.

Este envoltorio del journal aplica la regla: nunca borra entradas ya aplicadas (aplicadas => confirmadas => idénticas en
cualquier líder futuro, por Leader Completeness) e ignora la re-adición duplicada de una entrada que ya tiene con el mismo
índice y término. Las entradas no aplicadas siguen el comportamiento original (se truncan y se reemplazan).
"""
import logging

log = logging.getLogger("raft-safety")


class SafeJournal:
    def __init__(self, inner, applied):
        self._j = inner
        self._applied = applied          # función -> índice de la última entrada aplicada
        self.kept = 0                     # cuántas veces se evitó borrar entradas aplicadas (métrica / pruebas)

    def __getattr__(self, name):
        return getattr(self._j, name)

    def __getitem__(self, item):
        return self._j[item]

    def __len__(self):
        return len(self._j)

    def deleteEntriesFrom(self, pos):
        if len(self._j):
            first = self._j[0][1]
            keep = self._applied() - first + 1          # posición siguiente a la última entrada aplicada
            if pos < keep:
                self.kept += 1
                log.info("AppendEntries atrasado: se conservan las entradas ya aplicadas (hasta el índice %d)", self._applied())
                pos = keep
        if pos < len(self._j):
            self._j.deleteEntriesFrom(pos)

    def add(self, command, idx, term):
        if len(self._j):
            first, last = self._j[0][1], self._j[-1][1]
            if first <= idx <= last:
                e = self._j[idx - first]
                if e[2] == term:
                    return                                # ya la tiene (misma entrada): nada que hacer
                log.critical("conflicto en la entrada %d (término %s vs %s): se trunca desde ahí", idx, e[2], term)
                self._j.deleteEntriesFrom(idx - first)
        self._j.add(command, idx, term)


class _MuteDivergenceWarning(logging.Filter):
    """La advertencia crítica de PySyncObj se emite ANTES de truncar; con SafeJournal ya no se trunca nada aplicado."""
    def filter(self, record):
        return "truncating already-applied" not in str(record.msg)


def install(syncobj):
    """Envuelve el journal de un SyncObj ya construido."""
    logging.getLogger("pysyncobj.syncobj").addFilter(_MuteDivergenceWarning())
    j = syncobj._SyncObj__raftLog
    if not isinstance(j, SafeJournal):
        syncobj._SyncObj__raftLog = SafeJournal(j, lambda: syncobj._SyncObj__raftLastApplied)
    return syncobj._SyncObj__raftLog
