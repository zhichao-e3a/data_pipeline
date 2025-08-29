import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent

ENV_PATH    = ROOT / "config" / ".env"
SSH_PATH    = ROOT / "config" / "ef_aliyun_pem"

load_dotenv(ENV_PATH)

DB_CONFIG = {
    'DB_HOST'   : os.getenv("DB_HOST"),
    'DB_PORT'   : int(os.getenv("DB_PORT")),
    'DB_USER'   : os.getenv("DB_USER"),
    'DB_PASS'   : os.getenv("DB_PASS"),
    'DB_NAME'   : os.getenv("DB_NAME"),
    'SSH_HOST'  : os.getenv("SSH_HOST"),
    'SSH_PORT'  : int(os.getenv("SSH_PORT")),
    'SSH_USER'  : os.getenv("SSH_USER"),
    'SSH_PKEY'  : str(SSH_PATH)
}

ST_CRED = {
    'ST_USER' : os.getenv("ST_USER"),
    'ST_PASS' : os.getenv("ST_PASS")
}

MONGO_CONFIG = {
    'DB_HOST' : os.getenv("MONGO_URL"),
    'DB_NAME' : os.getenv("MONGO_NAME")
}

DEFAULT_MONGO_CONFIG = {
    "DB_HOST"   : os.getenv("MONGO_URL_E3A"),
    "DB_NAME"   : os.getenv("MONGO_NAME_E3A")
}
