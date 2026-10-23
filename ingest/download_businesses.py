#!/usr/bin/env python3
"""
Download the Tel Aviv businesses layer (ArcGIS MapServer 925) in full.

Why this exists: the layer caps responses at maxRecordCount = 2000, but holds ~37k
features. A single `where=1=1&outFields=*&f=json` call silently returns the first
2000 and sets "exceededTransferLimit": true. This script pages through everything.

Design:
  * Keyset pagination on OBJECTID (WHERE OBJECTID > last ORDER BY OBJECTID), which
    stays correct even if rows change between calls — unlike offset paging.
  * Every page is written to disk exactly as received (the landing / raw zone).
  * ArcGIS often reports errors as HTTP 200 with {"error": ...}; those are retried.
  * Self-verifying: total and unique OBJECTIDs must equal returnCountOnly, or it
    exits non-zero. A manifest records counts and a sha256 per file.
  * Owner name / owner ID fields are personal data and unused by the assignment,
    so they are excluded at source unless --include-pii is passed.

Stdlib only — runs locally, in Colab, or in a Databricks notebook.

Usage:
  python download_businesses.py --out landing/businesses
"""
import argparse, hashlib, json, os, sys, time, urllib.parse, urllib.request
from datetime import datetime, timezone

DEFAULT_URL = "https://gisn.tel-aviv.gov.il/arcgis/rest/services/IView2/MapServer/925/query"
ALL_FIELDS = ["OBJECTID", "id_esek", "shimush", "shem_rechov", "ms_bayit", "tr_hakama",
              "sw_taun_rishui", "shetach", "x_coord", "y_coord", "date_import"]
PII_FIELDS = ["shem_machzik_rashi", "ms_mezahe_machzik_rashi"]


def call(url, params, retries=6, timeout=90):
    qs = urllib.parse.urlencode(params)
    req = urllib.request.Request(f"{url}?{qs}", headers={"User-Agent": "tlv-medallion-assignment/1.0"})
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = r.read()
            data = json.loads(body.decode("utf-8"))
            if isinstance(data, dict) and "error" in data:          # ArcGIS error inside a 200
                raise RuntimeError(f"ArcGIS error: {data['error']}")
            return data, body
        except Exception as e:                                       # network, 5xx, bad JSON, ArcGIS error
            if attempt == retries:
                raise
            wait = min(2 ** attempt, 60)
            print(f"  attempt {attempt} failed ({e}); retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--out", default="landing/businesses")
    ap.add_argument("--page-size", type=int, default=2000)
    ap.add_argument("--include-pii", action="store_true")
    ap.add_argument("--pause", type=float, default=0.5, help="seconds between pages, to be polite")
    a = ap.parse_args()

    fields = ALL_FIELDS + (PII_FIELDS if a.include_pii else [])
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = os.path.join(a.out, f"run={run_ts}")
    os.makedirs(out_dir, exist_ok=True)

    expected = call(a.url, {"where": "1=1", "returnCountOnly": "true", "f": "json"})[0]["count"]
    print(f"server reports {expected:,} features")

    last_oid, page, seen, files = -1, 0, set(), []
    while True:
        params = {
            "where": f"OBJECTID > {last_oid}",
            "outFields": ",".join(fields),
            "orderByFields": "OBJECTID ASC",
            "resultRecordCount": a.page_size,
            "returnGeometry": "true",
            "f": "json",
        }
        data, body = call(a.url, params)
        feats = data.get("features", [])
        if not feats:
            break
        path = os.path.join(out_dir, f"businesses_page_{page:04d}.json")
        with open(path, "wb") as fh:
            fh.write(body)                                            # raw bytes, untouched
        oids = [f["attributes"]["OBJECTID"] for f in feats]
        seen.update(oids)
        files.append({"file": os.path.basename(path), "features": len(feats),
                      "min_oid": min(oids), "max_oid": max(oids),
                      "sha256": hashlib.sha256(body).hexdigest()})
        print(f"page {page:>3}: {len(feats):>5} features  OBJECTID {min(oids)}–{max(oids)}  total {len(seen):,}")
        last_oid, page = max(oids), page + 1
        time.sleep(a.pause)

    total = sum(f["features"] for f in files)
    manifest = {"source": a.url, "run_utc": run_ts, "fields_requested": fields,
                "pii_included": a.include_pii, "expected_count": expected,
                "features_downloaded": total, "unique_objectids": len(seen),
                "pages": len(files), "files": files}
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)

    ok = total == expected == len(seen)
    print(f"\n{'OK' if ok else 'MISMATCH'}: expected {expected:,}, downloaded {total:,}, "
          f"unique OBJECTIDs {len(seen):,}  ->  {out_dir}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
