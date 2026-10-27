"""Glue for running the layers as Databricks notebook tasks.

The notebooks stay thin: they read job parameters from widgets, point the config at
a Unity Catalog Volume, switch storage to Delta, and call the same layer functions
that the local run and the tests exercise.

The repo root is passed in explicitly. On serverless the working directory of a
notebook task is not something to rely on, so each notebook resolves the root
itself (job parameter -> sys.path -> notebook path) before importing this package.
"""
from __future__ import annotations

import os
import shutil

from . import config

WIDGETS = {"catalog": "workspace", "schema": "tlv", "volume": "landing", "repo_root": ""}


def widgets(dbutils) -> dict:
    for k, v in WIDGETS.items():
        dbutils.widgets.text(k, v)
    return {k: dbutils.widgets.get(k) for k in WIDGETS}


def volume_path(w: dict) -> str:
    return f"/Volumes/{w['catalog']}/{w['schema']}/{w['volume']}"


def cfg(dbutils, repo_root: str) -> config.Config:
    w = widgets(dbutils)
    vol = volume_path(w)
    return config.load(
        "conf/pipeline.yml",
        root=repo_root,
        storage_format="delta",
        catalog=w["catalog"],
        schema=w["schema"],
        closures_csv=f"{vol}/closures/rechov_sagur.csv",
        businesses_dir=f"{vol}/businesses",
        # category_map stays conf/category_map.csv in the repo: it's configuration, not landed data
    )


def seed_landing(repo_root: str, volume: str) -> dict:
    """Copy the repo's landing/ files into the volume, skipping files already there.

    Never overwrites: once real ingestion lands files in the volume, a re-run of
    setup leaves them alone. Returns counts of copied and skipped files.
    """
    src_root = os.path.join(repo_root, "landing")
    copied = skipped = 0
    for dirpath, _, files in os.walk(src_root):
        rel = os.path.relpath(dirpath, src_root)
        dst_dir = os.path.normpath(os.path.join(volume, rel))
        os.makedirs(dst_dir, exist_ok=True)
        for f in files:
            dst = os.path.join(dst_dir, f)
            if os.path.exists(dst):
                skipped += 1
                continue
            shutil.copyfile(os.path.join(dirpath, f), dst)   # copyfile: Volumes don't take chmod/utime
            copied += 1
    return {"copied": copied, "skipped": skipped}


def landing_inventory(volume: str) -> dict:
    """File counts per top-level landing folder, for a visible check after setup."""
    out = {}
    for top in ("closures", "businesses"):
        n = 0
        for _, _, files in os.walk(os.path.join(volume, top)):
            n += len(files)
        out[top] = n
    return out
