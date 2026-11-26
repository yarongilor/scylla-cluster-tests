"""Tests for sdcm.nemesis.monkey.compaction module."""

from contextlib import nullcontext
from unittest.mock import MagicMock, call, patch

import pytest

from sdcm.exceptions import UnsupportedNemesis, WatcherCallableException
from sdcm.nemesis.monkey.compaction import (
    CorruptThenScrubMonkey,
    MajorCompactionMonkey,
    StartStopCleanupCompaction,
    StartStopMajorCompaction,
    StartStopScrubCompaction,
    StartStopValidationCompaction,
)

MODULE = "sdcm.nemesis.monkey.compaction"
COMMON_MODULE = "sdcm.nemesis.utils.common"

pytestmark = pytest.mark.usefixtures("events")


@pytest.fixture()
def runner(base_runner):
    """``base_runner`` with one non-system table and two test keyspaces."""
    base_runner.cluster.get_non_system_ks_cf_list.return_value = ["ks1.tbl1"]
    base_runner.cluster.get_test_keyspaces.return_value = ["ks1", "ks2"]
    return base_runner


@pytest.fixture()
def compaction_ops():
    """The ``CompactionOps`` of the target node, its REST and nodetool calls mocked."""
    with patch(f"{MODULE}.CompactionOps") as mock_compaction_ops:
        yield mock_compaction_ops.return_value


@pytest.fixture(autouse=True)
def mock_adaptive_timeout():
    """Patch adaptive_timeout for all tests so nodetool calls go through."""
    with (
        patch(f"{MODULE}.adaptive_timeout") as mock_timeout,
        patch(f"{COMMON_MODULE}.adaptive_timeout", mock_timeout),
    ):
        mock_timeout.return_value.__enter__ = MagicMock()
        mock_timeout.return_value.__exit__ = MagicMock(return_value=False)
        yield mock_timeout


@pytest.fixture(autouse=True)
def mock_clear():
    """Stub ``clear_snapshots``, which runs nodetool on the nodes."""
    with patch(f"{MODULE}.clear_snapshots") as mock_clear:
        yield mock_clear


@pytest.mark.parametrize(
    ("monkey_class", "trigger", "trigger_kwargs", "watch_for", "stop", "scrubbed_tables"),
    [
        pytest.param(
            StartStopMajorCompaction,
            "trigger_major_compaction",
            {"keyspace": "ks1", "cf": "tbl1"},
            "User initiated compaction started on behalf of",
            "stop_major_compaction",
            [],
            id="major",
        ),
        pytest.param(
            StartStopScrubCompaction,
            "trigger_scrub_compaction",
            {"keyspace": "ks1", "cf": "tbl1"},
            "Scrubbing",
            "stop_scrub_compaction",
            [("ks1", "tbl1")],
            id="scrub",
        ),
        pytest.param(
            StartStopCleanupCompaction,
            "trigger_cleanup_compaction",
            {"timeout": 600},
            "Cleaning",
            "stop_cleanup_compaction",
            [],
            id="cleanup",
        ),
        pytest.param(
            StartStopValidationCompaction,
            "trigger_validation_compaction",
            {"keyspace": "ks1", "cf": "tbl1"},
            "Scrubbing ",
            "stop_validation_compaction",
            [("ks1", "tbl1")],
            id="validation",
        ),
    ],
)
def test_start_stop_compaction(
    runner, compaction_ops, mock_clear, monkey_class, trigger, trigger_kwargs, watch_for, stop, scrubbed_tables
):
    """Each nemesis starts its own compaction type, stops it once it is logged, and clears the scrub snapshots."""
    monkey_class(runner).disrupt()

    getattr(compaction_ops, trigger).assert_called_once_with(**trigger_kwargs)
    compaction_ops.stop_on_user_compaction_logged.assert_called_once_with(
        node=runner.target_node,
        mark=runner.target_node.mark_log.return_value,
        watch_for=watch_for,
        timeout=360,
        stop_func=getattr(compaction_ops, stop),
    )
    assert mock_clear.call_args_list == [call([runner.target_node], "pre-scrub-", *table) for table in scrubbed_tables]


@pytest.mark.parametrize(
    ("failing_call", "expectation"),
    [
        pytest.param("trigger_major_compaction", nullcontext(), id="interrupted-trigger-is-tolerated"),
        pytest.param(
            "stop_on_user_compaction_logged",
            pytest.raises(WatcherCallableException),
            id="failed-watcher-fails-the-nemesis",
        ),
    ],
)
def test_start_stop_compaction_failures(runner, compaction_ops, failing_call, expectation):
    """The trigger call may fail once its compaction is stopped, but a watcher that did not stop it fails the nemesis."""
    getattr(compaction_ops, failing_call).side_effect = TimeoutError("timed out")

    with expectation:
        StartStopMajorCompaction(runner).disrupt()


def test_start_stop_compaction_unsupported_without_tables(runner, compaction_ops):
    """With no non-system table to compact, the nemesis is skipped before any compaction is started."""
    runner.cluster.get_non_system_ks_cf_list.return_value = []

    with pytest.raises(UnsupportedNemesis, match="No non-system keyspaces"):
        StartStopMajorCompaction(runner).disrupt()

    compaction_ops.trigger_major_compaction.assert_not_called()


def test_corrupt_then_scrub(runner, mock_clear):
    """Every test keyspace is scrubbed with --skip-corrupted and its pre-scrub snapshots are cleared."""
    CorruptThenScrubMonkey(runner).disrupt()

    assert runner.target_node.run_nodetool.call_args_list == [
        call("scrub", args="--skip-corrupted ks1"),
        call("scrub", args="--skip-corrupted ks2"),
    ]
    assert mock_clear.call_args_list == [
        call([runner.target_node], "pre-scrub-", "ks1"),
        call([runner.target_node], "pre-scrub-", "ks2"),
    ]


def test_major_compaction(runner):
    """The nemesis runs nodetool compact on the target node."""
    MajorCompactionMonkey(runner).disrupt()

    runner.target_node.run_nodetool.assert_called_once_with("compact")
