import os
from datetime import date

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

_cluster = None
_session = None


def get_session():
    global _cluster, _session
    if _session is None:
        host     = os.getenv("CASSANDRA_HOST", "127.0.0.1")
        port     = int(os.getenv("CASSANDRA_PORT", "9042"))
        user     = os.getenv("CASSANDRA_USER", "")
        password = os.getenv("CASSANDRA_PASSWORD", "")
        auth     = PlainTextAuthProvider(user, password) if user else None

        _cluster = Cluster([host], port=port, auth_provider=auth)
        _session = _cluster.connect(os.getenv("CASSANDRA_KEYSPACE", "reviews"))
        print(f"✓ Cassandra connected → {host}:{port}")
    return _session


def shutdown():
    global _cluster, _session
    if _cluster:
        _cluster.shutdown()
        _cluster = None
        _session = None


def query(cql: str, params: tuple = ()) -> list[dict]:
    """Execute CQL, return list of dicts. Cursor always closed."""
    session = get_session()
    try:
        rows = session.execute(cql, params)
        result = []
        for row in rows:
            d = {}
            for k, v in row._asdict().items():
                d[k] = v.isoformat() if isinstance(v, date) else v
            result.append(d)
        return result
    except Exception:
        raise  # re-raise so routes return proper 500