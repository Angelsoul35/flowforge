"""Fetch and load a real public dataset: hourly electricity demand for Spain.

The data is the ENTSO-E / Red Eléctrica de España hourly series published as the
Hugging Face dataset ``vitaliy-sharandin/energy-consumption-hourly-spain``. It is
read from the Hugging Face *datasets-server* ``/rows`` API, which needs no auth and
returns JSON. A small real slice is vendored under ``datasets/`` so the pipeline is
deterministic and runs offline; the live fetch path is the same parser, exercised
in tests with a monkeypatched :data:`urlopen`.

Two entry points, one parser:

- :func:`fetch_rows` hits the real HTTP API (paginating over its 100-row limit).
- :func:`load_vendored_rows` reads the captured slice shipped in the repo.

Both return ``list[dict]`` rows with the columns in :data:`COLUMNS`.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from pathlib import Path

#: The real public dataset and its single split.
DATASET = "vitaliy-sharandin/energy-consumption-hourly-spain"
CONFIG = "default"
SPLIT = "train"

#: The datasets-server ``/rows`` endpoint caps ``length`` at 100 rows per request.
ROWS_API = "https://datasets-server.huggingface.co/rows"
MAX_PAGE = 100

#: The columns the real pipeline consumes (a projection of the 29-column source).
COLUMNS = (
    "time",
    "total load actual",
    "generation solar",
    "generation wind onshore",
    "price actual",
)

#: The vendored real slice (repo-root ``datasets/``), captured from the API.
_REPO_ROOT = Path(__file__).resolve().parent.parent
VENDORED_SLICE = _REPO_ROOT / "datasets" / "energy_spain_hourly_slice.json"

#: Bound at module level so tests can monkeypatch ``flowforge.real_data.urlopen``.
urlopen = urllib.request.urlopen


def rows_url(offset: int, length: int) -> str:
    """Build the datasets-server ``/rows`` URL for one page of the real dataset."""
    query = urllib.parse.urlencode(
        {"dataset": DATASET, "config": CONFIG, "split": SPLIT, "offset": offset, "length": length}
    )
    return f"{ROWS_API}?{query}"


def parse_payload(payload: dict) -> list[dict]:
    """Extract the row dicts from a datasets-server (or vendored) JSON payload."""
    return [entry["row"] for entry in payload["rows"]]


def fetch_rows(offset: int = 0, length: int = 168, *, timeout: float = 30.0) -> list[dict]:
    """Fetch ``length`` real rows from the live API, paginating over :data:`MAX_PAGE`.

    Goes through :data:`urlopen`, which tests replace to serve the vendored bytes
    without a network call. The projection in :data:`COLUMNS` is applied so the live
    path returns exactly the same shape as :func:`load_vendored_rows`.
    """
    rows: list[dict] = []
    fetched = 0
    while fetched < length:
        page = min(MAX_PAGE, length - fetched)
        with urlopen(rows_url(offset + fetched, page), timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        batch = parse_payload(payload)
        if not batch:
            break  # the dataset has fewer rows than requested
        rows.extend(batch)
        fetched += len(batch)
    return [{col: row[col] for col in COLUMNS} for row in rows[:length]]


def load_vendored_rows(path: Path = VENDORED_SLICE) -> list[dict]:
    """Load the vendored real slice from disk (the default, offline source)."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return parse_payload(payload)
