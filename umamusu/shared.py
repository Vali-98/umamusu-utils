import enum
import logging
import apsw
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path


@dataclass
class State:
    version: str
    meta_path: Path
    master_path: Path
    appdata_path: Path
    storage_path: Path
    log_path: Path | None


state = State(*[None] * 6)

# DB-related
_master_conn: apsw.Connection | None = None
_meta_conn: apsw.Connection | None = None


@contextmanager
def _db_cursor(conn: apsw.Connection):
    """
    Context manager for APSW cursors.
    APSW cursors don't need explicit close, but this keeps symmetry and safety.
    """
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur = None  # release reference


def master_cursor():
    """Return a context-managed APSW cursor for the master database."""
    global _master_conn

    if _master_conn is None:
        _master_path = state.master_path
        if not _master_path.exists():
            _master_path = state.appdata_path / "master/master.mdb"
        if not _master_path.exists():
            raise FileNotFoundError(f"master DB path does not exist: {_master_path}")

        _master_conn = apsw.Connection(str(_master_path))

    return _db_cursor(_master_conn)

GlobalDBKey = bytes([
    0x56, 0x63, 0x6B, 0x63, 0x42, 0x72, 0x37, 0x76,
    0x65, 0x70, 0x41, 0x62
])

DBBaseKey = bytes([
    0xF1, 0x70, 0xCE, 0xA4, 0xDF, 0xCE, 0xA3, 0xE1,
    0xA5, 0xD8, 0xC7, 0x0B, 0xD1, 0x00, 0x00, 0x00
])


def gen_final_key(key: bytes) -> bytes:
    if len(DBBaseKey) < 13:
        raise ValueError("Invalid Base Key length")

    # XOR each byte in key with DBBaseKey[i % 13]
    return bytes((key[i] ^ DBBaseKey[i % 13]) for i in range(len(key)))

def meta_cursor():
    """Return a context-managed APSW cursor for the meta database."""
    global _meta_conn

    if _meta_conn is None:
        _meta_path = state.meta_path
        if not _meta_path.exists():
            _meta_path = state.appdata_path / "meta"
        if not _meta_path.exists():
            raise FileNotFoundError(f"meta DB path does not exist: {_meta_path}")

        _meta_conn = apsw.Connection(str(_meta_path))
    _meta_conn.pragma("hexkey",gen_final_key(GlobalDBKey).hex())
    return _db_cursor(_meta_conn)


# Logging
class Status(enum.Enum):
    OK = enum.auto()
    ERR = enum.auto()


class CustomAdapter(logging.LoggerAdapter):
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"

    def process(self, msg, kwargs):
        if status := kwargs.pop("status", None):
            if status == Status.OK:
                return f"{self.OKGREEN}{msg}{self.ENDC}", kwargs
            elif status == Status.ERR:
                return f"{self.FAIL}{msg}{self.ENDC}", kwargs
        return msg, kwargs


def get_logger(name: str):
    """Get a logger, writing to file if state.log_path is set."""
    logger = logging.getLogger(name)
    if state.log_path is None:
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
    else:
        logger.setLevel(logging.INFO)
        state.log_path.mkdir(exist_ok=True)
        handler = logging.FileHandler(
            filename=Path(state.log_path, f"{name}.log"), mode="w+", encoding="utf8"
        )

    logger.addHandler(handler)
    logger = CustomAdapter(logger, {})
    return logger


class AppDataException(Exception):
    def __init__(self):
        super().__init__("Unable to find AppData folder")
