import sys
import threading


class LamportClock:
    """Часы Лампорта + журнал событий в формате из задания.

    Счётчик и запись в лог меняются под одним замком,
    поэтому порядок строк в логе совпадает с порядком значений L.
    """

    def __init__(self, name, log_file=None, echo=False):
        self.name = name
        self._t = 0
        self._lock = threading.Lock()
        self._echo = echo
        self._file = open(log_file, "a", encoding="utf-8") if log_file else None

    def now(self):
        with self._lock:
            return self._t

    def local_event(self, kind, text):
        """Локальное событие (SEND, APPLY ...): сначала +1, потом запись. Возвращает L."""
        with self._lock:
            self._t += 1
            self._write(kind, text, self._t)
            return self._t

    def send(self, text):
        return self.local_event("SEND", text)

    def receive(self, text, received):
        """Получение сообщения: L = max(свой, полученный) + 1."""
        with self._lock:
            self._t = max(self._t, received) + 1
            self._write("RECV", text, self._t, received)
            return self._t

    def _write(self, kind, text, t, received=None):
        line = f"[{self.name}] {kind:<6} {text:<46} L={t}"
        if received is not None:
            line += f"  (received L={received})"
        if self._file:
            self._file.write(line + "\n")
            self._file.flush()
        if self._echo:
            print(line, file=sys.stdout, flush=True)