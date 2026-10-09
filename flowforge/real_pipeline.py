"""A real data pipeline, expressed as a flowforge DAG.

The pipeline turns a week of *real* hourly electricity-demand readings for Spain
(see :mod:`flowforge.real_data`) into a daily report. It is a genuine multi-stage
workflow with real dependencies and real parallelism, run by the project's own
:class:`~flowforge.engine.Engine`::

    fetch ──▶ clean ────┐
          └─▶ validate ─┴─▶ aggregate ──▶ summary_stats ─┐
                                        └▶ detect_peaks ──┴─▶ report

``clean`` and ``validate`` are independent (both consume ``fetch``) and run in the
same layer; so do ``summary_stats`` and ``detect_peaks`` (both consume
``aggregate``). flowforge discovers that parallelism from the dependencies alone.

Steps are zero-argument callables, as the engine requires, so they communicate
through a shared :class:`PipelineState` workspace — the same way a CI runner shares
a build directory between stages. Each step performs a real transform on the real
numbers; nothing is mocked.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from .models import Step
from .real_data import COLUMNS, load_vendored_rows

HOUR_SECONDS = 3600
MAX_LOAD_NULL_FRACTION = 0.10


class PipelineError(ValueError):
    """Raised by a pipeline step when the real data violates an invariant."""


# --------------------------------------------------------------------------- #
# Records
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Observation:
    """One cleaned hourly reading (megawatts, except ``price`` in EUR/MWh)."""

    ts: datetime
    load_mw: float
    solar_mw: float
    wind_mw: float
    price: float


@dataclass(frozen=True)
class DailyAggregate:
    """Demand rolled up from 24 hourly readings to one calendar day."""

    date: str
    hours: int
    total_load_mwh: float  # sum of hourly MW == energy over the day in MWh
    mean_load_mw: float
    peak_hour: int
    peak_load_mw: float
    renewable_mwh: float  # solar + onshore wind generation over the day
    renewable_share: float  # renewable_mwh / total_load_mwh
    mean_price: float


@dataclass(frozen=True)
class ValidationReport:
    rows: int
    days: int
    load_null_fraction: float
    hourly_cadence: bool


@dataclass(frozen=True)
class Summary:
    days: int
    week_total_mwh: float
    mean_daily_mwh: float
    highest_demand_day: str
    highest_demand_mwh: float
    lowest_demand_day: str
    lowest_demand_mwh: float
    mean_renewable_share: float


@dataclass(frozen=True)
class Peaks:
    per_day: dict[str, tuple[int, float]]  # date -> (peak hour, peak MW)
    week_peak_ts: datetime
    week_peak_mw: float


@dataclass
class PipelineState:
    """The shared workspace that steps read from and write to."""

    raw: list[dict] = field(default_factory=list)
    observations: list[Observation] = field(default_factory=list)
    validation: ValidationReport | None = None
    daily: list[DailyAggregate] = field(default_factory=list)
    summary: Summary | None = None
    peaks: Peaks | None = None
    report: str = ""


# --------------------------------------------------------------------------- #
# Transforms (pure functions on the real data)
# --------------------------------------------------------------------------- #
def _interpolate(values: list[float | None]) -> list[float]:
    """Linearly interpolate interior ``None`` gaps; carry the edge if one-sided."""
    out: list[float | None] = list(values)
    n = len(out)
    for i in range(n):
        if out[i] is not None:
            continue
        lo = i - 1
        while lo >= 0 and values[lo] is None:
            lo -= 1
        hi = i + 1
        while hi < n and values[hi] is None:
            hi += 1
        left = values[lo] if lo >= 0 else None
        right = values[hi] if hi < n else None
        if left is None and right is None:
            raise PipelineError("cannot interpolate an entirely empty column")
        if left is None:
            out[i] = right
        elif right is None:
            out[i] = left
        else:
            out[i] = left + (right - left) * (i - lo) / (hi - lo)
    return [float(v) for v in out]  # type: ignore[arg-type]


def clean_observations(raw: list[dict]) -> list[Observation]:
    """Parse timestamps, coerce numbers, and fill interior gaps by interpolation."""
    rows = sorted(raw, key=lambda r: r["time"])
    ts = [datetime.fromisoformat(r["time"]) for r in rows]
    load = _interpolate([r["total load actual"] for r in rows])
    solar = _interpolate([r["generation solar"] for r in rows])
    wind = _interpolate([r["generation wind onshore"] for r in rows])
    price = _interpolate([r["price actual"] for r in rows])
    return [
        Observation(t, lo, so, wi, pr)
        for t, lo, so, wi, pr in zip(ts, load, solar, wind, price, strict=True)
    ]


def validate_raw(raw: list[dict]) -> ValidationReport:
    """Gate the raw rows on schema and integrity; raise :class:`PipelineError`.

    Runs in parallel with :func:`clean_observations` (both depend only on fetch).
    """
    if not raw:
        raise PipelineError("no rows fetched")
    for i, row in enumerate(raw):
        missing = [c for c in COLUMNS if c not in row]
        if missing:
            raise PipelineError(f"row {i} is missing columns: {', '.join(missing)}")
    if len(raw) % 24 != 0:
        raise PipelineError(f"expected whole days (multiple of 24 hours), got {len(raw)} rows")

    times = sorted(datetime.fromisoformat(r["time"]) for r in raw)
    cadence = all(
        (b - a).total_seconds() == HOUR_SECONDS
        for a, b in zip(times, times[1:], strict=False)
    )
    if not cadence:
        raise PipelineError("rows are not an unbroken hourly series")

    load_nulls = sum(1 for r in raw if r["total load actual"] is None)
    null_fraction = load_nulls / len(raw)
    if null_fraction >= MAX_LOAD_NULL_FRACTION:
        raise PipelineError(
            f"too many missing demand readings: {null_fraction:.1%} >= "
            f"{MAX_LOAD_NULL_FRACTION:.0%}"
        )
    return ValidationReport(
        rows=len(raw),
        days=len(raw) // 24,
        load_null_fraction=null_fraction,
        hourly_cadence=cadence,
    )


def aggregate_daily(observations: list[Observation]) -> list[DailyAggregate]:
    """Roll hourly readings up to one :class:`DailyAggregate` per calendar day."""
    by_day: dict[str, list[Observation]] = defaultdict(list)
    for obs in observations:
        by_day[obs.ts.date().isoformat()].append(obs)

    daily: list[DailyAggregate] = []
    for date in sorted(by_day):
        hours = by_day[date]
        total = sum(o.load_mw for o in hours)
        peak = max(hours, key=lambda o: o.load_mw)
        renewable = sum(o.solar_mw + o.wind_mw for o in hours)
        daily.append(
            DailyAggregate(
                date=date,
                hours=len(hours),
                total_load_mwh=total,
                mean_load_mw=total / len(hours),
                peak_hour=peak.ts.hour,
                peak_load_mw=peak.load_mw,
                renewable_mwh=renewable,
                renewable_share=renewable / total,
                mean_price=sum(o.price for o in hours) / len(hours),
            )
        )
    return daily


def summarize(daily: list[DailyAggregate]) -> Summary:
    """Summary statistics across the days (depends on ``aggregate``)."""
    if not daily:
        raise PipelineError("no daily aggregates to summarise")
    highest = max(daily, key=lambda d: d.total_load_mwh)
    lowest = min(daily, key=lambda d: d.total_load_mwh)
    week_total = sum(d.total_load_mwh for d in daily)
    return Summary(
        days=len(daily),
        week_total_mwh=week_total,
        mean_daily_mwh=week_total / len(daily),
        highest_demand_day=highest.date,
        highest_demand_mwh=highest.total_load_mwh,
        lowest_demand_day=lowest.date,
        lowest_demand_mwh=lowest.total_load_mwh,
        mean_renewable_share=sum(d.renewable_share for d in daily) / len(daily),
    )


def detect_peaks(observations: list[Observation], daily: list[DailyAggregate]) -> Peaks:
    """Per-day and whole-week demand peaks (depends on ``aggregate``)."""
    if not observations:
        raise PipelineError("no observations to scan for peaks")
    week_peak = max(observations, key=lambda o: o.load_mw)
    return Peaks(
        per_day={d.date: (d.peak_hour, d.peak_load_mw) for d in daily},
        week_peak_ts=week_peak.ts,
        week_peak_mw=week_peak.load_mw,
    )


def build_report(summary: Summary, daily: list[DailyAggregate], peaks: Peaks) -> str:
    """Render the real output as a readable text report."""
    lines = [
        "Spain hourly electricity demand — weekly report",
        f"  window         : {daily[0].date} .. {daily[-1].date} ({summary.days} days)",
        f"  week total     : {summary.week_total_mwh:,.0f} MWh",
        f"  mean daily     : {summary.mean_daily_mwh:,.0f} MWh",
        f"  highest demand : {summary.highest_demand_day} "
        f"({summary.highest_demand_mwh:,.0f} MWh)",
        f"  lowest demand  : {summary.lowest_demand_day} "
        f"({summary.lowest_demand_mwh:,.0f} MWh)",
        f"  week peak hour : {peaks.week_peak_ts.isoformat()} "
        f"({peaks.week_peak_mw:,.0f} MW)",
        f"  mean renewables: {summary.mean_renewable_share:.1%} of demand "
        "(solar + onshore wind)",
        "  per-day:",
    ]
    for d in daily:
        lines.append(
            f"    {d.date}  total {d.total_load_mwh:>9,.0f} MWh  "
            f"mean {d.mean_load_mw:>8,.0f} MW  "
            f"peak {d.peak_load_mw:>8,.0f} MW @ {d.peak_hour:02d}:00  "
            f"renew {d.renewable_share:5.1%}"
        )
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# DAG wiring — one source of truth for the graph shape
# --------------------------------------------------------------------------- #
#: step name -> its dependencies. Reused by the pipeline and the guarantee demos.
EDGES: dict[str, tuple[str, ...]] = {
    "fetch": (),
    "clean": ("fetch",),
    "validate": ("fetch",),
    "aggregate": ("clean", "validate"),
    "summary_stats": ("aggregate",),
    "detect_peaks": ("aggregate",),
    "report": ("summary_stats", "detect_peaks"),
}


def build_pipeline(
    state: PipelineState,
    *,
    loader: Callable[[], list[dict]] = load_vendored_rows,
    actions: dict[str, Callable[[], object]] | None = None,
    retries: dict[str, int] | None = None,
    extra_edges: dict[str, tuple[str, ...]] | None = None,
) -> list[Step]:
    """Wire the real transforms into flowforge :class:`Step` objects.

    The steps share ``state``. ``actions`` overrides a step's callable and
    ``retries`` its ``max_retries`` (both used by the guarantee demos to inject a
    fault); ``extra_edges`` overrides the dependency wiring (used to build a cyclic
    graph). By default this returns the complete, fully-working pipeline.
    """

    def fetch() -> None:
        state.raw = loader()

    def clean() -> None:
        state.observations = clean_observations(state.raw)

    def validate() -> None:
        state.validation = validate_raw(state.raw)

    def aggregate() -> None:
        state.daily = aggregate_daily(state.observations)

    def summary_stats() -> None:
        state.summary = summarize(state.daily)

    def detect() -> None:
        state.peaks = detect_peaks(state.observations, state.daily)

    def report() -> None:
        assert state.summary is not None and state.peaks is not None
        state.report = build_report(state.summary, state.daily, state.peaks)

    default_actions: dict[str, Callable[[], object]] = {
        "fetch": fetch,
        "clean": clean,
        "validate": validate,
        "aggregate": aggregate,
        "summary_stats": summary_stats,
        "detect_peaks": detect,
        "report": report,
    }
    actions = {**default_actions, **(actions or {})}
    retries = retries or {}
    edges = {**EDGES, **(extra_edges or {})}
    return [
        Step(
            name=name,
            action=actions[name],
            depends_on=edges[name],
            max_retries=retries.get(name, 0),
        )
        for name in edges
    ]
