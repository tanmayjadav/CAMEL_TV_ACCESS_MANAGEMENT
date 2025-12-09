from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request

DATA_PATH = Path("Others/scheduler_dummy_transactions.json")
DATA = json.loads(DATA_PATH.read_text(encoding="utf-8"))

app = Flask(__name__)


def parse_created_at(raw: str) -> datetime:
    return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


@app.get("/all-user-transactions")
def all_user_transactions():
    since_param = request.args.get("since", type=int)
    if since_param:
        since_dt = datetime.fromtimestamp(since_param, tz=timezone.utc)
        transactions = [
            txn for txn in DATA if parse_created_at(txn["created_at"]) > since_dt
        ]
    else:
        transactions = DATA
    return jsonify(transactions)


if __name__ == "__main__":
    # runs on http://127.0.0.1:9000
    app.run(host="127.0.0.1", port=9000)