"""Rebuild dashboard/index.html from output/insights.json, so the static dashboard never goes stale.

    python dashboard/build_dashboard.py            (run.py calls this after every local run)
"""
from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def build(insights_path: str, out_path: str = os.path.join(HERE, "index.html")) -> str:
    with open(os.path.join(HERE, "template.html"), encoding="utf-8") as fh:
        template = fh.read()
    with open(insights_path, encoding="utf-8") as fh:
        data = json.load(fh)
    # "</" is escaped so text inside the JSON can never close the <script> tag
    payload = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(template.replace("__DATA__", payload))
    return out_path


if __name__ == "__main__":
    root = os.path.dirname(HERE)
    src = sys.argv[1] if len(sys.argv) > 1 else os.path.join(root, "output", "insights.json")
    print("dashboard written ->", build(src))
