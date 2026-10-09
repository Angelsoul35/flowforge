# Vendored dataset slice

`energy_spain_hourly_slice.json` is a small, real slice of **hourly electricity
demand for Spain**, used by the real-data pipeline (`flowforge/real_pipeline.py`)
so it runs end-to-end, deterministically and offline. The live fetch path
(`flowforge.real_data.fetch_rows`) reads the same data from the network; this file
is a captured copy so tests and the benchmark need no network and no credentials.

## What is in the file

A JSON document shaped like the Hugging Face *datasets-server* `/rows` response:

```json
{
  "dataset": "vitaliy-sharandin/energy-consumption-hourly-spain",
  "config": "default", "split": "train",
  "columns": ["time", "total load actual", "generation solar",
              "generation wind onshore", "price actual"],
  "num_rows_total": 35064,
  "slice": {"offset": 0, "length": 168,
            "first_time": "2015-01-01 00:00:00+01:00",
            "last_time":  "2015-01-07 23:00:00+01:00"},
  "rows": [{"row_idx": 0, "row": {"time": "...", "total load actual": 25385.0, ...}}, ...]
}
```

- **168 rows** = the first 7 full days (2015-01-01 .. 2015-01-07), hourly.
- A **5-column projection** of the source's 29 columns — only the fields the
  pipeline consumes. Keeping the projection is what makes the slice small.
- The data is **real and unretouched**, gaps included: `total load actual` has 6
  missing hours and solar/wind 7 (a real 6-hour outage on 2015-01-05 plus one
  isolated hour). The pipeline's `clean` step interpolates them and its `validate`
  step gates on the missing-data fraction — that is the point of using real data.

## Source and license

- **Immediate source:** Hugging Face dataset
  [`vitaliy-sharandin/energy-consumption-hourly-spain`](https://huggingface.co/datasets/vitaliy-sharandin/energy-consumption-hourly-spain),
  read through the public datasets-server rows API (no authentication).
- **Upstream:** this is the well-known Kaggle dataset *"Hourly energy demand
  generation and weather"* (nicholasjhana), published under **CC0 1.0 (public
  domain)**, itself compiled from the **ENTSO-E Transparency Platform** and **Red
  Eléctrica de España (REE)** open data.
- The Hugging Face mirror does not carry an explicit license tag; it redistributes
  the CC0 upstream. Only public, non-personal grid measurements are included here.

## How to regenerate

The slice is reproducible from the live API. From the repo root, with the package
installed (`pip install -e ".[dev]"`):

```bash
python - <<'PY'
import json
from flowforge import real_data

rows = real_data.fetch_rows(offset=0, length=168)   # paginates over the 100-row API cap
payload = {
    "dataset": real_data.DATASET, "config": real_data.CONFIG, "split": real_data.SPLIT,
    "columns": list(real_data.COLUMNS), "num_rows_total": 35064,
    "slice": {"offset": 0, "length": len(rows),
              "first_time": rows[0]["time"], "last_time": rows[-1]["time"]},
    "rows": [{"row_idx": i, "row": r} for i, r in enumerate(rows)],
}
with open("datasets/energy_spain_hourly_slice.json", "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=1, ensure_ascii=False)
    f.write("\n")
PY
```

The equivalent raw request for one page is:

```
https://datasets-server.huggingface.co/rows?dataset=vitaliy-sharandin/energy-consumption-hourly-spain&config=default&split=train&offset=0&length=100
```
