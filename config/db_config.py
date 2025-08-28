import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent

ENV_PATH = ROOT / "config" / ".env"

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
    'SSH_PKEY'  : os.getenv("SSH_PKEY")
}

MONGO_CONFIG = {
    'DB_HOST' : os.getenv("MONGO_HOST"),
    'DB_NAME' : os.getenv("MONGO_NAME")
}