from config.configs import MONGO_CONFIG, DEFAULT_MONGO_CONFIG

import json
import logging
import logging.config
import logging.handlers
from queue import Queue

from datetime import datetime

from pymongo import MongoClient

class JsonFormatter(logging.Formatter):

    def format(
            self,
            record: logging.LogRecord
    ) -> str:

        payload = {
            "logger"    : record.name,
            "level"     : record.levelname,
            "msg"       : record.getMessage(),
            "ts"        : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        for k in (
            "job_id",
            "data_origin",
            "task",
            "task_n",
            "duration",
            "rows",
            "cols",
            "n_links",
            "rows_added",
            "error",
            "cancelled"
        ):

            v = getattr(record, k, None)
            payload[k] = v

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)

class MongoHandler(logging.Handler):

    def __init__(
            self,
            mongo_uri   : str,
            db_name     : str,
            level       = logging.INFO
    ):

        super().__init__(level)

        self.client = MongoClient(mongo_uri)
        self.coll   = self.client[db_name]["pipeline_logging"]

    def emit(self, record: logging.LogRecord) -> None:

        try:
            doc = json.loads(self.format(record))
            self.coll.insert_one(doc)

        except Exception as e:
            print(e)
            pass

def setup_logging(
    remote : bool
):

    if remote:
        config = DEFAULT_MONGO_CONFIG
    else:
        config = MONGO_CONFIG

    # Infinite queue handler
    q = Queue(-1)
    queue_handler = logging.handlers.QueueHandler(q)

    # Attach queue handler to root
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Remove pre-existing handlers to prevent duplicate logs
    for h in list(root.handlers):
        root.removeHandler(h)

    # Add queue handler
    root.addHandler(queue_handler)

    # Build sinks used by listener thread (QueueListener)
    sinks   = []
    fmt     = JsonFormatter()

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt)
    sinks.append(console_handler)

    mongo_handler = MongoHandler(
        mongo_uri   = config["DB_HOST"],
        db_name     = config["DB_NAME"]
    )
    # mongo_handler already has level=logging.INFO
    mongo_handler.setFormatter(fmt)
    sinks.append(mongo_handler)

    # Start the QueueListener
    listener = logging.handlers.QueueListener(q, *sinks, respect_handler_level=True)
    listener.start()

    # Return listener so that caller can call stop()
    return listener