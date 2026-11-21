"""Tests for quieting the log while an inverter is unreachable.

A solar inverter goes unreachable when there is no sunlight. With the default settings the
daemon logs an error and a warning every few seconds all night. With quiet_when_offline set,
the first failure of a run is reported once and the rest drop to DEBUG, so the log stays
readable without hiding the fact that the inverter is gone.

`conftest.py` replaces the `logger` module with a MagicMock, so these tests assert on the
calls made to it rather than on captured log records.
"""

from unittest.mock import AsyncMock, patch

import pytest

import goodwe2mqtt
from conftest import suppress_ensure_future


def _make_gw(quiet_when_offline):
    with patch("asyncio.ensure_future", side_effect=suppress_ensure_future):
        gw = goodwe2mqtt.Goodwe_MQTT(
            serial_number="TEST_SN",
            ip_address="192.168.1.100",
            mqtt_broker_ip="127.0.0.1",
            mqtt_broker_port=1883,
            mqtt_username="",
            mqtt_password="",
            mqtt_topic_prefix="goodwe2mqtt",
            mqtt_control_topic_postfix="control",
            mqtt_runtime_data_topic_postfix="runtime_data",
            mqtt_runtime_data_interval_seconds=5,
            mqtt_fast_runtime_data_topic_postfix="fast_runtime_data",
            mqtt_fast_runtime_data_interval_seconds=1,
            mqtt_grid_export_limit_topic_postfix="grid_export_limit",
            quiet_when_offline=quiet_when_offline,
        )
    # The constructor logs its own banner; clear it so assertions see only the poll's logs.
    goodwe2mqtt.log.reset_mock()
    return gw


@pytest.fixture
def log_mock():
    """The mocked logger, with its call history cleared."""
    log = goodwe2mqtt.log
    log.reset_mock()
    return log


def _messages(mock_method):
    return [c.args[0] for c in mock_method.call_args_list if c.args]


@pytest.mark.asyncio
async def test_quiet_reports_first_failure_then_drops_to_debug(log_mock):
    """The first failed read is reported once; the rest are DEBUG only."""
    gw = _make_gw(quiet_when_offline=True)
    gw.inverter = AsyncMock()
    gw.inverter.read_runtime_data.side_effect = RuntimeError("No valid response received")

    for _ in range(3):
        assert await gw.read_runtime_data() is None

    log_mock.error.assert_not_called()
    log_mock.warning.assert_not_called()

    infos = _messages(log_mock.info)
    assert len(infos) == 1, f"expected one INFO for the first failure, got {infos}"
    assert "failed" in infos[0]

    debugs = _messages(log_mock.debug)
    assert len(debugs) == 2, f"the repeat failures should be DEBUG, got {debugs}"


@pytest.mark.asyncio
async def test_quiet_logs_recovery_once(log_mock):
    """The first successful read after a run of failures is announced once."""
    gw = _make_gw(quiet_when_offline=True)
    gw.inverter = AsyncMock()
    gw.inverter.read_runtime_data.side_effect = [
        RuntimeError("gone"),
        RuntimeError("gone"),
        {"ppv": 1},
        {"ppv": 2},
    ]

    for _ in range(4):
        await gw.read_runtime_data()

    recoveries = [m for m in _messages(log_mock.info) if "responding again" in m]
    assert len(recoveries) == 1, "recovery should be announced once, not on every success"


@pytest.mark.asyncio
async def test_default_keeps_logging_every_failure(log_mock):
    """Without the setting, behaviour is unchanged: every failure is an error."""
    gw = _make_gw(quiet_when_offline=False)
    gw.inverter = AsyncMock()
    gw.inverter.read_runtime_data.side_effect = RuntimeError("No valid response received")

    for _ in range(3):
        await gw.read_runtime_data()

    assert len(_messages(log_mock.error)) == 3, "the default must keep reporting every failure"


@pytest.mark.asyncio
async def test_quiet_does_not_hide_a_successful_read(log_mock):
    """A healthy inverter produces no offline chatter at all."""
    gw = _make_gw(quiet_when_offline=True)
    gw.inverter = AsyncMock()
    gw.inverter.read_runtime_data.return_value = {"ppv": 1}

    for _ in range(3):
        assert await gw.read_runtime_data() is not None

    assert [m for m in _messages(log_mock.info) if "failed" in m] == []
    assert [m for m in _messages(log_mock.info) if "responding again" in m] == []
    log_mock.error.assert_not_called()


def test_str_reports_the_setting():
    gw = _make_gw(quiet_when_offline=True)
    assert "quiet_when_offline=true" in str(gw)
