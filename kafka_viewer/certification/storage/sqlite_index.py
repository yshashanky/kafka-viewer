from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from ..models import MISSING, NormalizedRecord


def pack(value):
    """Tagged JSON, never pickle; distinguishes missing, decimal and input objects."""
    if value is MISSING:
        return ["missing"]
    if isinstance(value, Decimal):
        return ["decimal", str(value)]
    if isinstance(value, dict):
        return ["dict", [[k, pack(v)] for k, v in sorted(value.items())]]
    if isinstance(value, (list, tuple)):
        return ["list", [pack(v) for v in value]]
    return [type(value).__name__, value]


def unpack(value):
    tag = value[0]
    if tag == "missing":
        return MISSING
    if tag == "decimal":
        return Decimal(value[1])
    if tag == "dict":
        return {k: unpack(v) for k, v in value[1]}
    if tag == "list":
        return [unpack(v) for v in value[1]]
    return value[1]


def dumps(value):
    return json.dumps(pack(value), ensure_ascii=True, allow_nan=False, separators=(",", ":"))


def loads(value):
    return unpack(json.loads(value))


def id_key(value):
    # Keep type strict but canonicalize same-type numeric representations.
    if isinstance(value, Decimal):
        sign, digits, exponent = value.as_tuple()
        digits = list(digits)
        while len(digits) > 1 and digits[-1] == 0:
            digits.pop()
            exponent += 1
        value = Decimal(0) if value == 0 else Decimal((sign, tuple(digits), exponent))
    elif type(value) is float and value == 0:
        value = 0.0
    return dumps(value)


class SQLiteIndex:
    """Owns a private temporary directory; bounded transactions and indexed lookups."""

    def __init__(self):
        self._directory = TemporaryDirectory(prefix="kafka-certify-")
        self.path = Path(self._directory.name) / "evidence.sqlite3"
        self._closed = False
        self._pending = 0
        try:
            self.db = sqlite3.connect(self.path)
            self.db.executescript("""
                PRAGMA temp_store=FILE;
                PRAGMA cache_size=-4096;
                CREATE TABLE records (
                    seq INTEGER PRIMARY KEY, side TEXT NOT NULL, id_key TEXT,
                    eligible INTEGER NOT NULL, payload TEXT NOT NULL);
                CREATE INDEX records_id ON records(id_key, side);
                CREATE TABLE evidence (seq INTEGER PRIMARY KEY, payload TEXT NOT NULL);
            """)
        except BaseException:
            self.close()
            raise

    def insert(self, side, record, eligible=True):
        if side not in ("source", "destination"):
            raise ValueError("Invalid record side")
        key = id_key(record.id) if isinstance(record, NormalizedRecord) else None
        self.db.execute("INSERT INTO records(side,id_key,eligible,payload) VALUES (?,?,?,?)",
                        (side, key, int(eligible), dumps(asdict(record))))
        self._tick()

    def _tick(self):
        self._pending += 1
        if self._pending >= 500:
            self.flush()

    def flush(self):
        self.db.commit()
        self._pending = 0

    def groups(self):
        yield from self.db.execute("""
            SELECT id_key, SUM(side='source'), SUM(side='destination'),
                   SUM(side='source' AND eligible=1), SUM(side='source' AND eligible=0)
            FROM records WHERE id_key IS NOT NULL GROUP BY id_key ORDER BY id_key
        """)

    def occurrences(self, key, side):
        for payload, eligible in self.db.execute(
            "SELECT payload,eligible FROM records WHERE id_key=? AND side=? ORDER BY seq", (key, side)
        ):
            yield {**loads(payload), "eligible": bool(eligible)}

    def errors(self):
        for side, payload in self.db.execute(
            "SELECT side,payload FROM records WHERE id_key IS NULL ORDER BY seq"
        ):
            yield {"side": side, **loads(payload)}

    def add_evidence(self, evidence):
        self.db.execute("INSERT INTO evidence(payload) VALUES (?)", (dumps(evidence),))
        self._tick()

    def evidence(self):
        for payload, in self.db.execute("SELECT payload FROM evidence ORDER BY seq"):
            yield loads(payload)

    def close(self):
        if not self._closed:
            self._closed = True
            if hasattr(self, "db"):
                self.db.close()
            self._directory.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
