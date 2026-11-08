"""T0: corrección del journal Raft (dfsha/mds/raft_safety.py): un AppendEntries atrasado no borra entradas ya aplicadas;
un conflicto real en entradas no aplicadas sí se trunca (regla de Raft §5.3). Prueba unitaria, sin clúster."""
from pysyncobj.journal import MemoryJournal
from dfsha.mds.raft_safety import SafeJournal
from conftest import evid


def journal(n, term=1, applied=0):
    j = MemoryJournal()
    for i in range(1, n + 1):
        j.add(b"cmd%d" % i, i, term)
    st = {"applied": applied}
    return SafeJournal(j, lambda: st["applied"]), st


def test_append_entries_atrasado_conserva_lo_aplicado():
    j, st = journal(10, applied=8)
    # PySyncObj ante un AppendEntries con prevLogIdx=5 y 2 entradas: borra desde la posición de idx 6 y re-agrega 6..7
    j.deleteEntriesFrom(5)
    for i in (6, 7):
        j.add(b"cmd%d" % i, i, 1)
    assert [e[1] for e in j] == list(range(1, 9)) and j.kept == 1       # 1..8 aplicadas intactas; 9 y 10 (no aplicadas) se truncan
    assert [e[0] for e in j][:8] == [b"cmd%d" % i for i in range(1, 9)]


def test_conflicto_real_en_entradas_no_aplicadas_se_trunca():
    j, st = journal(10, applied=4)
    j.deleteEntriesFrom(4)                     # desde idx 5: no hay nada aplicado ahí
    j.add(b"nuevo5", 5, 2)
    assert [(e[1], e[2]) for e in j] == [(1, 1), (2, 1), (3, 1), (4, 1), (5, 2)] and j.kept == 0
    evid("T0_raft_journal", "AppendEntries atrasado: entradas 1..8 ya aplicadas se conservan (PySyncObj las habría borrado); "
         "conflicto real en entradas no aplicadas: se trunca y se reemplaza (Raft §5.3)")
