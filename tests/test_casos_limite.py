"""Casos límite propios: bordes de ventana, de lateness, entradas inválidas y
equivalencia entre el oráculo en Python y el pipeline Beam."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

DATA_PATH = Path(__file__).parents[1] / "data" / "payments.jsonl"


def load_events() -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in DATA_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def payment(
    event_id: str,
    event_time: str,
    arrival_time: str,
    *,
    merchant_id: str = "m-a",
    amount: int = 10,
    status: str = "CONFIRMED",
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "merchant_id": merchant_id,
        "event_time": event_time,
        "arrival_time": arrival_time,
        "amount": amount,
        "status": status,
    }


# --- Tiempo de evento -------------------------------------------------------


def test_parse_utc_normalizes_explicit_offset_to_utc(solution):
    parsed = solution.parse_utc("2026-07-24T10:00:05-03:00")

    assert parsed == datetime(2026, 7, 24, 13, 0, 5, tzinfo=UTC)
    assert parsed.utcoffset().total_seconds() == 0


@pytest.mark.parametrize(
    "raw_value",
    ["", "   ", "no-es-una-fecha", "2026-07-24T13:00:05", None],
)
def test_parse_utc_rejects_invalid_or_naive_values(solution, raw_value):
    with pytest.raises(ValueError):
        solution.parse_utc(raw_value)


# --- Ventanas ---------------------------------------------------------------


def test_window_start_is_inclusive_and_end_is_exclusive(solution):
    on_boundary = datetime(2026, 7, 24, 13, 1, 0, tzinfo=UTC)
    just_before = datetime(2026, 7, 24, 13, 0, 59, 999_999, tzinfo=UTC)

    assert solution.assign_fixed_window(on_boundary, 60)[0] == on_boundary
    assert solution.assign_fixed_window(just_before, 60) == (
        datetime(2026, 7, 24, 13, 0, tzinfo=UTC),
        on_boundary,
    )


def test_assign_fixed_window_rejects_naive_timestamp(solution):
    with pytest.raises(ValueError):
        solution.assign_fixed_window(datetime(2026, 7, 24, 13, 0, 42), 60)


# --- Lateness ---------------------------------------------------------------


def test_lateness_boundary_one_second_each_side(solution):
    # Ventana [13:00, 13:01) con 120 s de lateness: cierra a las 13:03:00.
    inside = payment("p-in", "2026-07-24T13:00:10Z", "2026-07-24T13:02:59Z")
    outside = payment("p-out", "2026-07-24T13:00:20Z", "2026-07-24T13:03:01Z")

    totals, audit = solution.summarize_payments([inside, outside])
    by_id = {row["event_id"]: row for row in audit}

    assert by_id["p-in"]["accepted"] and by_id["p-in"]["revision"]
    assert by_id["p-out"]["too_late"] and by_id["p-out"]["reason"] == "too_late"
    assert [row["total"] for row in totals] == [10]


def test_on_time_event_is_not_a_revision(solution):
    _totals, audit = solution.summarize_payments(
        [payment("p-1", "2026-07-24T13:00:10Z", "2026-07-24T13:00:30Z")]
    )

    assert audit[0]["accepted"] is True
    assert audit[0]["revision"] is False


# --- Deduplicación ----------------------------------------------------------


def test_non_confirmed_event_does_not_consume_dedup_state(solution):
    # El mismo event_id llega primero PENDING y después CONFIRMED: la versión
    # confirmada tiene que contar, porque el pendiente nunca entró al estado.
    events = [
        payment("p-1", "2026-07-24T13:00:10Z", "2026-07-24T13:00:11Z", status="PENDING"),
        payment("p-1", "2026-07-24T13:00:10Z", "2026-07-24T13:00:40Z"),
    ]

    totals, audit = solution.summarize_payments(events)

    assert [row["reason"] for row in audit] == ["status_pending", "accepted"]
    assert totals[0]["total"] == 10


def test_disabling_deduplication_double_counts(solution):
    events = [
        payment("p-1", "2026-07-24T13:00:10Z", "2026-07-24T13:00:11Z"),
        payment("p-1", "2026-07-24T13:00:10Z", "2026-07-24T13:00:20Z"),
    ]

    with_dedup, _ = solution.summarize_payments(events)
    without_dedup, _ = solution.summarize_payments(events, deduplicate=False)

    assert with_dedup[0]["total"] == 10
    assert without_dedup[0]["total"] == 20


# --- Sink idempotente -------------------------------------------------------


def test_late_revision_overwrites_previous_row_in_upsert_sink(solution):
    on_time = {"merchant_id": "m-a", "window_start": "2026-07-24T13:00:00+00:00", "total": 30}
    late = {**on_time, "total": 35}

    upsert_rows, _ = solution.simulate_sink_retries([on_time, late], attempts=1)
    append_rows, _ = solution.simulate_sink_retries(
        [on_time, late], attempts=1, idempotent=False
    )

    assert [row["total"] for row in upsert_rows] == [35]
    assert sum(row["total"] for row in append_rows) == 65


def test_sink_rejects_zero_attempts(solution):
    with pytest.raises(ValueError):
        solution.simulate_sink_retries([], attempts=0)


# --- Oráculo vs. Beam -------------------------------------------------------


def test_beam_batch_pipeline_matches_oracle_without_lateness_drops(solution):
    # En batch el watermark salta a +inf, así que Beam no descarta por
    # lateness. Con una tolerancia enorme el oráculo tiene que coincidir fila
    # por fila con el pipeline sobre el dataset real.
    events = load_events()
    expected, _ = solution.summarize_payments(events, allowed_lateness_seconds=10**6)

    with BeamTestPipeline() as pipeline:
        output = solution.build_windowed_totals_pipeline(pipeline, events)
        assert_that(output, equal_to(expected))
