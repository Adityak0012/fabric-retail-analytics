"""Convert notebooks/src/*.py (percent format) into Fabric-importable .ipynb files.

    python tools/build_notebooks.py          -> notebooks/*.ipynb

Conventions in the .py sources
  # %%                      starts a code cell
  # %% [markdown]           starts a markdown cell (lines keep their leading "# ")
  # %% tags=["parameters"]  code cell marked as the Fabric/papermill parameters cell
  # MAGIC %run nb_x         becomes the notebook magic `%run nb_x` in the .ipynb
Keeping sources as .py makes them diff-able in git and testable locally.
"""
import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "notebooks" / "src"
OUT = ROOT / "notebooks"


def parse_cells(text: str):
    cells, cur = [], None
    for line in text.splitlines():
        m = re.match(r"^# %%(.*)$", line)
        if m:
            if cur:
                cells.append(cur)
            hdr = m.group(1)
            cur = {"type": "markdown" if "[markdown]" in hdr else "code",
                   "tags": ["parameters"] if "parameters" in hdr else [], "lines": []}
            continue
        if cur is None:
            continue
        if cur["type"] == "markdown":
            line = re.sub(r"^# ?", "", line)
        elif line.startswith("# MAGIC "):
            line = line[len("# MAGIC "):]
        cur["lines"].append(line)
    if cur:
        cells.append(cur)
    for c in cells:  # trim blank edges
        while c["lines"] and not c["lines"][-1].strip():
            c["lines"].pop()
        while c["lines"] and not c["lines"][0].strip():
            c["lines"].pop(0)
    return [c for c in cells if c["lines"]]


def to_ipynb(cells):
    nb_cells = []
    for i, c in enumerate(cells):
        src = [l + "\n" for l in c["lines"]]
        src[-1] = src[-1].rstrip("\n")
        cell = {"id": f"cell-{i:02d}", "cell_type": c["type"], "metadata": {"tags": c["tags"]} if c["tags"] else {}, "source": src}
        if c["type"] == "code":
            cell.update({"execution_count": None, "outputs": []})
        nb_cells.append(cell)
    return {
        "cells": nb_cells,
        "metadata": {
            "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark", "language": "Python"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4, "nbformat_minor": 5,
    }


if __name__ == "__main__":
    for py in sorted(SRC.glob("nb_*.py")):
        nb = to_ipynb(parse_cells(py.read_text(encoding="utf-8")))
        dest = OUT / (py.stem + ".ipynb")
        dest.write_text(json.dumps(nb, indent=1, ensure_ascii=False), encoding="utf-8")
        print(f"built {dest.relative_to(ROOT)}  ({len(nb['cells'])} cells)")
