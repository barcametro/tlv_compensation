"""Pipeline configuration: one YAML file, resolved relative to the project root."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass

try:                     # PyYAML ships with Databricks runtimes; the fallback keeps the
    import yaml          # pipeline independent of the serverless environment version
except ImportError:      # pragma: no cover
    yaml = None


def _parse_simple_yaml(text: str) -> dict:
    """Parser for this project's config shape only: top-level sections of `key: value`."""
    out, section = {}, None
    for raw in text.splitlines():
        line = re.sub(r"\s+#.*$", "", raw).rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, _, val = line.strip().partition(":")
        val = val.strip().strip('"').strip("'")
        if not raw.startswith((" ", "\t")):
            section = out.setdefault(key, {}) if not val else None
            if val:
                out[key] = val
        elif section is not None:
            section[key] = val
    return out


@dataclass(frozen=True)
class Config:
    root: str
    year: int
    rate: float
    cap: float
    trading_start_min: int
    trading_end_min: int
    closures_csv: str
    businesses_dir: str
    category_map: str
    storage_format: str
    base_path: str
    catalog: str
    schema: str
    min_street_match_rate: float

    @property
    def trading_minutes(self) -> int:
        return self.trading_end_min - self.trading_start_min


def _minutes(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def load(path: str = "conf/pipeline.yml", root: str | None = None, **overrides) -> Config:
    """Load config. `root` defaults to the directory containing conf/."""
    root = os.path.abspath(root or os.path.dirname(os.path.dirname(os.path.abspath(path))))
    with open(os.path.join(root, "conf", os.path.basename(path)), encoding="utf-8") as fh:
        text = fh.read()
    c = yaml.safe_load(text) if yaml else _parse_simple_yaml(text)

    def p(rel: str) -> str:  # absolute paths stay as-is (e.g. /Volumes/... on Databricks)
        return rel if os.path.isabs(rel) or rel.startswith(("dbfs:", "/Volumes")) else os.path.join(root, rel)

    comp, src, st = c["compensation"], c["sources"], c["storage"]
    cfg = dict(
        root=root,
        year=int(comp["year"]),
        rate=float(comp["rate_ils_per_sqm_per_day"]),
        cap=float(comp["daily_cap_ils"]),
        trading_start_min=_minutes(comp["trading_hours_start"]),
        trading_end_min=_minutes(comp["trading_hours_end"]),
        closures_csv=p(src["closures_csv"]),
        businesses_dir=p(src["businesses_dir"]),
        category_map=p(src["category_map"]),
        storage_format=st["format"],
        base_path=p(st["base_path"]),
        catalog=st["catalog"],
        schema=st["schema"],
        min_street_match_rate=float(c["quality"]["min_street_match_rate_2023"]),
    )
    cfg.update({k: v for k, v in overrides.items() if v is not None})
    return Config(**cfg)
