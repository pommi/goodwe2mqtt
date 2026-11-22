"""Tests for aligning poll intervals to the wall clock.

An interval that divides 60 seconds evenly lands on the minute (:00, :10, :20), so the
cadence is the same in every hour and readings line up with whole minutes. The helper
decides the next boundary; the main loop schedules its reads against it.
"""
from datetime import datetime, timedelta
import pytest
import goodwe2mqtt
from goodwe2mqtt import next_interval_boundary


# ---------------------------------------------------------------------------
# next_interval_boundary: the arithmetic
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("interval, now, expected", [
    # Divides a minute: land on the minute, in every hour.
    (10, datetime(2026, 11, 22, 0, 0, 2),   datetime(2026, 11, 22, 0, 0, 10)),
    (10, datetime(2026, 11, 22, 0, 0, 0),   datetime(2026, 11, 22, 0, 0, 10)),
    (10, datetime(2026, 11, 22, 13, 47, 13), datetime(2026, 11, 22, 13, 47, 20)),
    (1,  datetime(2026, 11, 22, 0, 0, 2, 500000), datetime(2026, 11, 22, 0, 0, 3)),
    (2,  datetime(2026, 11, 22, 0, 0, 3),   datetime(2026, 11, 22, 0, 0, 4)),
    (5,  datetime(2026, 11, 22, 13, 47, 3), datetime(2026, 11, 22, 13, 47, 5)),
    (15, datetime(2026, 11, 22, 13, 59, 50), datetime(2026, 11, 22, 14, 0, 0)),
    (30, datetime(2026, 11, 22, 13, 59, 45), datetime(2026, 11, 22, 14, 0, 0)),
    (60, datetime(2026, 11, 22, 13, 59, 45), datetime(2026, 11, 22, 14, 0, 0)),
    # Longer than a minute: a multiple of itself since the epoch, stable across hours.
    (300, datetime(2026, 11, 22, 13, 59, 45), datetime(2026, 11, 22, 14, 0, 0)),
])
def test_boundary_lands_on_the_interval(interval, now, expected):
    assert next_interval_boundary(timedelta(seconds=interval), now) == expected


def test_boundary_is_always_in_the_future():
    """A boundary at or before now would mean a zero or negative sleep."""
    for seconds in (1, 2, 5, 10, 15, 20, 30, 60, 300, 7):
        now = datetime(2026, 11, 22, 0, 0, 0)
        assert next_interval_boundary(timedelta(seconds=seconds), now) > now


def test_boundary_never_skips_a_whole_interval():
    """Even just past a boundary, the next one is one interval away, not two."""
    now = datetime(2026, 11, 22, 0, 0, 10, 1)
    nxt = next_interval_boundary(timedelta(seconds=10), now)
    assert nxt == datetime(2026, 11, 22, 0, 0, 20)
    assert (nxt - now).total_seconds() <= 10


def test_interval_that_does_not_divide_a_minute_still_advances():
    """7 s cannot be aligned to the minute, but the cadence must still be 7 s."""
    now = datetime(2026, 11, 22, 0, 0, 2)
    nxt = next_interval_boundary(timedelta(seconds=7), now)
    assert nxt > now
    assert (nxt - now).total_seconds() <= 7


def test_zero_interval_does_not_loop_forever():
    now = datetime(2026, 11, 22, 0, 0, 2)
    assert next_interval_boundary(timedelta(0), now) == now


# ---------------------------------------------------------------------------
# main_loop: the scheduled time is used
# ---------------------------------------------------------------------------

def _make_gw():
    from unittest.mock import patch
    from conftest import suppress_ensure_future
    with patch("asyncio.ensure_future", side_effect=suppress_ensure_future):
        return goodwe2mqtt.Goodwe_MQTT(
            serial_number="SN_ALIGN",
            ip_address="127.0.0.1",
            mqtt_broker_ip="127.0.0.1",
            mqtt_broker_port=1883,
            mqtt_username=None,
            mqtt_password=None,
            mqtt_topic_prefix="goodwe2mqtt",
            mqtt_control_topic_postfix="control",
            mqtt_runtime_data_topic_postfix="runtime_data",
            mqtt_runtime_data_interval_seconds=10,
            mqtt_fast_runtime_data_topic_postfix="fast_runtime_data",
            mqtt_fast_runtime_data_interval_seconds=10,
            mqtt_grid_export_limit_topic_postfix="grid_export_limit",
        )


@pytest.mark.asyncio
async def test_main_loop_sleeps_until_the_boundary_not_a_fixed_interval():
    """The loop asks the helper for the next boundary and sleeps until it."""
    from unittest.mock import AsyncMock, MagicMock, patch
    import asyncio

    gw = _make_gw()
    gw.inverter = MagicMock()
    gw.read_settings_data = AsyncMock(return_value={"a": 1})
    gw.read_runtime_data = AsyncMock(return_value={"serial_number": "SN_ALIGN"})

    mock_client = MagicMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.publish = AsyncMock()

    boundaries = []
    sleeps = []

    def fake_boundary(interval, now=None):
        # Two boundaries, so the loop runs twice before being cancelled.
        target = datetime.now() + timedelta(seconds=10)
        boundaries.append(interval)
        return target

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) >= 2:
            raise asyncio.CancelledError

    with patch("goodwe2mqtt.aiomqtt.Client", return_value=mock_client), \
         patch("goodwe2mqtt.next_interval_boundary", side_effect=fake_boundary), \
         patch("goodwe2mqtt.asyncio.sleep", side_effect=fake_sleep):
        await gw.main_loop()

    assert len(boundaries) >= 2
    # The fast interval (10 s) is the one aligned, not the runtime interval.
    assert all(b == timedelta(seconds=10) for b in boundaries)
    # Sleeping to a target 10 s ahead means a sleep near 10 s, not a fixed poll.
    assert all(0 < s <= 10 for s in sleeps), sleeps
