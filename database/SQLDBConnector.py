from config.configs import DB_CONFIG

import pandas as pd
from contextlib import contextmanager

from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

class SQLDBConnector:

    def __init__(self):
        pass

    @contextmanager
    def ssh_tunnel(self):

        tunnel = SSHTunnelForwarder(
            ssh_address_or_host=(DB_CONFIG['SSH_HOST'], DB_CONFIG['SSH_PORT']),
            ssh_username=DB_CONFIG['SSH_USER'],
            ssh_pkey=DB_CONFIG['SSH_PKEY'],
            remote_bind_address=(DB_CONFIG['DB_HOST'], DB_CONFIG['DB_PORT'])
        )

        try:
            tunnel.start()
            local_bind_port = tunnel.local_bind_port
            print(f"SSH tunnel started on port {local_bind_port}")
            yield local_bind_port

        finally:
            tunnel.close()

    @contextmanager
    def connect(self):

        with self.ssh_tunnel() as local_bind_port:

            url = URL.create(
                drivername="mysql+pymysql",
                username=DB_CONFIG["DB_USER"],
                password=DB_CONFIG["DB_PASS"],
                host="127.0.0.1",
                port=local_bind_port,
                database=DB_CONFIG["DB_NAME"],
                query={"charset": "utf8mb4"},  # keeps charset with the URL
            )

            engine = create_engine(
                url,
                pool_size=8,
                max_overflow=8,
                pool_recycle=3600,
                pool_pre_ping=True,
                future=True,
                connect_args={
                    "connect_timeout": 10,
                }
            )

            try:
                yield engine
            finally:
                engine.dispose()

    def query_to_dataframe(self, query, chunksize=None):
        with self.connect() as engine:
            return pd.read_sql(query, engine, chunksize=chunksize)
