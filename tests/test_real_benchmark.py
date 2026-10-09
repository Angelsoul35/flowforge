"""Functional tests for the real electricity-demand pipeline and its DAG.

The reference numbers pinned here were computed independently from the vendored
slice (``datasets/energy_spain_hourly_slice.json``); each assertion fails if the
real transform or the engine behaviour it targets is broken.
"""

from __future__ import annotations

import io
import json
import urllib.parse

import pytest

from flowforge import real_benchmark, real_data
from flowforge.dag import DagError, layers, topological_order
from flowforge.engine import Engine
from flowforge.models import StepStatus
from flowforge.real_data import COLUMNS, fetch_rows, load_vendored_rows, rows_url
from flowforge.real_pipeline import (
    EDGES,
    PipelineError,
    PipelineState,
    _interpolate,
    aggregate_daily,
    build_pipeline,
    clean_observations,
    detect_peaks,
    summarize,
    validate_raw,
)

# Reference values, computed by hand from the vendored slice.
EXPECTED_LAYERS = [
    ["fetch"],
    ["clean", "validate"],
    ["aggregate"],
    ["detect_peaks", "summary_stats"],
    ["report"],
]
EXPECTED_ORDER = [
    "fetch",
    "clean",
    "validate",
    "aggregate",
    "detect_peaks",
    "summary_stats",
    "report",
]
JAN2_TOTAL_MWH = 654031.0  # sum of 24 clean hourly readings for 2015-01-02
JAN2_RENEWABLE_MWH = 299462.0
WEEK_TOTAL_MWH = 4463717.0
WEEK_PEAK_MW = 36195.0


@pytest.fixture
def run_report():
    state = PipelineState()
    report = Engine(max_parallel=4).run(build_pipeline(state))
    return state, report


# --------------------------------------------------------------------------- #
# Engine structure on the real DAG
# --------------------------------------------------------------------------- #
def test_layers_group_the_parallel_stages():
    steps = build_pipeline(PipelineState())
    assert layers(steps) == EXPECTED_LAYERS


def test_topological_order_of_real_dag():
    steps = build_pipeline(PipelineState())
    assert topological_order(steps) == EXPECTED_ORDER


def test_clean_and_validate_share_a_layer_but_report_is_last():
    steps = build_pipeline(PipelineState())
    computed = layers(steps)
    assert set(computed[1]) == {"clean", "validate"}  # independent -> parallel
    assert set(computed[3]) == {"detect_peaks", "summary_stats"}
    assert computed[-1] == ["report"]


# --------------------------------------------------------------------------- #
# Real output (independently pinned)
# --------------------------------------------------------------------------- #
def test_pipeline_run_succeeds_and_fills_state(run_report):
    state, report = run_report
    assert report.status is StepStatus.SUCCESS
    assert set(report.succeeded()) == set(EXPECTED_ORDER)
    assert state.report.startswith("Spain hourly electricity demand")


def test_known_daily_total_matches_independent_reference(run_report):
    state, _ = run_report
    jan2 = next(d for d in state.daily if d.date == "2015-01-02")
    assert jan2.hours == 24
    assert jan2.total_load_mwh == JAN2_TOTAL_MWH
    assert jan2.peak_hour == 21
    assert jan2.peak_load_mw == 33668.0
    assert jan2.renewable_mwh == JAN2_RENEWABLE_MWH
    assert jan2.renewable_share == pytest.approx(JAN2_RENEWABLE_MWH / JAN2_TOTAL_MWH)


def test_week_summary_matches_independent_reference(run_report):
    state, _ = run_report
    s = state.summary
    assert s.days == 7
    assert s.week_total_mwh == WEEK_TOTAL_MWH
    assert s.mean_daily_mwh == pytest.approx(WEEK_TOTAL_MWH / 7)
    assert (s.highest_demand_day, s.highest_demand_mwh) == ("2015-01-07", 770520.0)
    assert (s.lowest_demand_day, s.lowest_demand_mwh) == ("2015-01-05", 545021.0)


def test_week_peak_hour_detected(run_report):
    state, _ = run_report
    assert state.peaks.week_peak_mw == WEEK_PEAK_MW
    assert state.peaks.week_peak_ts.isoformat() == "2015-01-07T17:00:00+01:00"
    assert state.peaks.per_day["2015-01-02"] == (21, 33668.0)


def test_validation_report_describes_the_real_slice(run_report):
    state, _ = run_report
    v = state.validation
    assert (v.rows, v.days, v.hourly_cadence) == (168, 7, True)
    assert v.load_null_fraction == pytest.approx(6 / 168)


# --------------------------------------------------------------------------- #
# Cleaning / interpolation (mutation-proven)
# --------------------------------------------------------------------------- #
def test_interpolate_fills_interior_gap_linearly():
    assert _interpolate([0.0, None, None, 3.0]) == [0.0, 1.0, 2.0, 3.0]


def test_interpolate_carries_one_sided_edges():
    assert _interpolate([None, 2.0]) == [2.0, 2.0]
    assert _interpolate([2.0, None]) == [2.0, 2.0]


def test_interpolate_rejects_all_null_column():
    with pytest.raises(PipelineError, match="empty column"):
        _interpolate([None, None])


def test_clean_fills_the_real_six_hour_gap():
    obs = clean_observations(load_vendored_rows())
    assert len(obs) == 168
    assert all(o.load_mw is not None for o in obs)
    # The real gap is 2015-01-05 12:00..17:00; index 108 == 12:00.
    noon = next(o for o in obs if o.ts.isoformat() == "2015-01-05T12:00:00+01:00")
    assert noon.load_mw == 24066.0  # linear fill between 11:00 and 18:00


# --------------------------------------------------------------------------- #
# Validation gate rejects bad data (mutation-proven)
# --------------------------------------------------------------------------- #
def _good_rows():
    return load_vendored_rows()


def test_validate_accepts_the_real_slice():
    report = validate_raw(_good_rows())
    assert report.rows == 168


def test_validate_rejects_empty():
    with pytest.raises(PipelineError, match="no rows"):
        validate_raw([])


def test_validate_rejects_non_whole_days():
    with pytest.raises(PipelineError, match="whole days"):
        validate_raw(_good_rows()[:23])


def test_validate_rejects_broken_cadence():
    rows = _good_rows()[:24]
    rows[5] = {**rows[5], "time": "2015-01-01 05:30:00+01:00"}  # half-hour step
    with pytest.raises(PipelineError, match="hourly"):
        validate_raw(rows)


def test_validate_rejects_too_many_nulls():
    rows = _good_rows()[:24]
    for i in range(10):  # 10/24 > 10% threshold
        rows[i] = {**rows[i], "total load actual": None}
    with pytest.raises(PipelineError, match="missing demand"):
        validate_raw(rows)


def test_validate_rejects_missing_column():
    rows = _good_rows()[:24]
    rows[0] = {k: v for k, v in rows[0].items() if k != "price actual"}
    with pytest.raises(PipelineError, match="missing columns"):
        validate_raw(rows)


def test_aggregate_empty_is_rejected_downstream():
    # summarize/detect need data; aggregate of nothing yields nothing.
    assert aggregate_daily([]) == []


def test_summarize_rejects_no_days():
    with pytest.raises(PipelineError, match="no daily aggregates"):
        summarize([])


def test_detect_peaks_rejects_no_observations():
    with pytest.raises(PipelineError, match="no observations"):
        detect_peaks([], [])


# --------------------------------------------------------------------------- #
# Guarantee: cycle rejected
# --------------------------------------------------------------------------- #
def test_cycle_is_rejected_before_running():
    cyclic = build_pipeline(PipelineState(), extra_edges={"fetch": ("report",)})
    with pytest.raises(DagError, match="cycle"):
        Engine().run(cyclic)


# --------------------------------------------------------------------------- #
# Guarantee: retry then cascade to dependents only
# --------------------------------------------------------------------------- #
def test_failing_step_retried_then_cascades_to_dependents_only():
    attempts = {"n": 0}

    def boom() -> None:
        attempts["n"] += 1
        raise RuntimeError("peak detector crashed")

    state = PipelineState()
    steps = build_pipeline(
        state, actions={"detect_peaks": boom}, retries={"detect_peaks": 2}
    )
    report = Engine(max_parallel=4).run(steps)

    assert report.status is StepStatus.FAILED
    # retried: 1 + 2 = 3 attempts
    assert attempts["n"] == 3
    assert report.results["detect_peaks"].status is StepStatus.FAILED
    assert report.results["detect_peaks"].attempts == 3
    # cascades to the dependent only
    assert report.results["report"].status is StepStatus.SKIPPED
    assert report.results["report"].skipped_due_to == ["detect_peaks"]
    # the independent branch and all upstream still complete
    assert report.results["summary_stats"].status is StepStatus.SUCCESS
    for upstream in ("fetch", "clean", "validate", "aggregate"):
        assert report.results[upstream].status is StepStatus.SUCCESS


def test_retry_eventually_succeeds_is_not_a_failure():
    calls = {"n": 0}

    def flaky() -> None:
        calls["n"] += 1
        if calls["n"] < 2:
            raise RuntimeError("transient")

    state = PipelineState()
    steps = build_pipeline(state, actions={"validate": flaky}, retries={"validate": 3})
    report = Engine().run(steps)
    assert report.status is StepStatus.SUCCESS
    assert report.results["validate"].attempts == 2


# --------------------------------------------------------------------------- #
# Fetch path: monkeypatched urlopen (no network)
# --------------------------------------------------------------------------- #
class _FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def _fake_urlopen_factory(recorder):
    rows = load_vendored_rows()

    def fake_urlopen(url, timeout=None):
        recorder.append(url)
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        offset = int(query["offset"][0])
        length = int(query["length"][0])
        page = rows[offset : offset + length]
        payload = {"rows": [{"row_idx": offset + i, "row": r} for i, r in enumerate(page)]}
        return _FakeResponse(json.dumps(payload).encode("utf-8"))

    return fake_urlopen


def test_fetch_rows_uses_urlopen_and_paginates(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(real_data, "urlopen", _fake_urlopen_factory(calls))

    fetched = fetch_rows(length=168)
    vendored = load_vendored_rows()

    assert fetched == vendored
    assert len(fetched) == 168
    assert set(fetched[0]) == set(COLUMNS)
    # 168 rows over a 100-row page limit -> two requests, at offsets 0 and 100.
    assert len(calls) == 2
    offsets = [
        int(urllib.parse.parse_qs(urllib.parse.urlparse(u).query)["offset"][0]) for u in calls
    ]
    assert offsets == [0, 100]


def test_fetch_rows_stops_when_dataset_is_exhausted(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(real_data, "urlopen", _fake_urlopen_factory(calls))
    # Ask for more than the vendored slice holds; it must stop, not loop.
    fetched = fetch_rows(length=1000)
    assert len(fetched) == 168


def test_rows_url_encodes_dataset_and_window():
    url = rows_url(100, 68)
    query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    assert query["dataset"][0] == real_data.DATASET
    assert query["offset"][0] == "100"
    assert query["length"][0] == "68"


# --------------------------------------------------------------------------- #
# Benchmark entry point
# --------------------------------------------------------------------------- #
def test_edges_cover_every_step_exactly_once():
    assert set(EDGES) == set(EXPECTED_ORDER)


def test_benchmark_main_prints_real_output_and_demos(capsys):
    real_benchmark.main()
    out = capsys.readouterr().out
    assert "week total     : 4,463,717 MWh" in out
    assert "a cycle is rejected" in out
    assert "attempted 3 times" in out
    assert "independent branch" in out
