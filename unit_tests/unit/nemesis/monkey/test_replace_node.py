"""Tests for sdcm.nemesis.monkey.replace_node module."""

from unittest.mock import MagicMock, call, patch

import pytest

from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis.monkey.replace_node import DEFAULT_DEAD_NODE_DELAY, GracefulStopTerminateAndReplaceMonkey

MODULE = "sdcm.nemesis.monkey.replace_node"
OLD_IP = "10.0.0.1"
NEW_IP = "10.0.0.9"
HOST_ID = "aaaa-bbbb-cccc"
ALL_UP = {"dc1": {OLD_IP: {"state": "UN"}, "10.0.0.2": {"state": "UN"}}}

pytestmark = pytest.mark.usefixtures("events")


@pytest.fixture()
def new_node():
    node = MagicMock()
    node.name = "node9"
    node.ip_address = NEW_IP
    return node


@pytest.fixture()
def runner(base_runner, new_node):
    """``base_runner`` whose target node is dead after termination and gone after the replace.

    ``get_nodetool_status`` reports every node UN before the disruption, then the old IP as DN,
    then no longer lists it; ``sleep`` is patched out so the dead-node delay does not run for real.
    """
    base_runner.target_node.ip_address = OLD_IP
    base_runner.target_node.host_id = HOST_ID
    base_runner.target_node.rack = 2
    base_runner.target_node._is_zero_token_node = False
    base_runner.target_node.is_seed = False
    base_runner.cluster.params = {"nemesis_dead_node_delay": 7, "nemesis_replace_major_compact_every": 0}
    base_runner.cluster.get_nodetool_status.side_effect = [ALL_UP, {"dc1": {OLD_IP: {"state": "DN"}}}, {"dc1": {}}]
    base_runner.terminate_node = MagicMock()
    base_runner.replace_node = MagicMock(return_value=new_node)
    with patch(f"{MODULE}.time.sleep") as sleep:
        base_runner.sleep = sleep
        yield base_runner


@pytest.fixture()
def survivor(runner):
    """The data node that stays up while the target is replaced."""
    return runner.cluster.data_nodes[1]


@pytest.fixture()
def compacting_runner(runner):
    """``runner`` with the survivor major compaction enabled on every other replace."""
    runner.cluster.params["nemesis_replace_major_compact_every"] = 2
    with patch(f"{MODULE}.adaptive_timeout"):
        yield runner


@pytest.fixture()
def parent(runner, survivor):
    """Attach the compact, stop, terminate and replace mocks to one parent so their order can be asserted."""
    parent = MagicMock()
    parent.attach_mock(survivor.run_nodetool, "compact_survivor")
    parent.attach_mock(runner.target_node.stop_scylla_server, "stop_scylla_server")
    parent.attach_mock(runner.terminate_node, "terminate_node")
    parent.attach_mock(runner.sleep, "sleep")
    parent.attach_mock(runner.replace_node, "replace_node")
    return parent


# ---------------------------------------------------------------------------
# disrupt
# ---------------------------------------------------------------------------


def test_disrupt_stops_gracefully_waits_then_replaces_by_host_id(runner, parent):
    """Scylla is stopped cleanly before the instance is terminated, the node stays dead for the
    configured delay, and only then the replacement is started with the old node's host id and rack."""
    GracefulStopTerminateAndReplaceMonkey(runner).disrupt()

    assert parent.mock_calls == [
        call.stop_scylla_server(verify_down=True),
        call.terminate_node(target_node=runner.target_node),
        call.sleep(7),
        call.replace_node(OLD_IP, HOST_ID, rack=2, is_zero_node=False),
    ]
    runner.cluster.update_seed_provider.assert_not_called()


def test_disrupt_uses_default_delay_when_option_unset(runner):
    """Without 'nemesis_dead_node_delay' the node stays dead for the same 5 minutes NodeTerminateAndReplace uses."""
    runner.cluster.params = {}

    GracefulStopTerminateAndReplaceMonkey(runner).disrupt()

    runner.sleep.assert_called_once_with(DEFAULT_DEAD_NODE_DELAY)


def test_disrupt_fails_when_terminated_node_is_not_down(runner):
    """A node still reported UN after termination means nothing was terminated, so the replace must not start."""
    runner.cluster.get_nodetool_status.side_effect = [ALL_UP, {"dc1": {OLD_IP: {"state": "UN"}}}]

    with pytest.raises(AssertionError, match="should be DN"):
        GracefulStopTerminateAndReplaceMonkey(runner).disrupt()

    runner.replace_node.assert_not_called()


def test_disrupt_skips_when_a_node_is_already_down(runner):
    """With a node already DN (a previous replace that never created its node) the nemesis is skipped
    before stopping anything, so the cluster does not lose a second node and its quorum."""
    runner.cluster.get_nodetool_status.side_effect = [{"dc1": {OLD_IP: {"state": "UN"}, "10.0.0.2": {"state": "DN"}}}]

    with pytest.raises(UnsupportedNemesis, match="10.0.0.2"):
        GracefulStopTerminateAndReplaceMonkey(runner).disrupt()

    runner.target_node.stop_scylla_server.assert_not_called()
    runner.terminate_node.assert_not_called()


def test_disrupt_restores_seed_flag_on_replacement(runner, new_node):
    """Replacing a seed node makes the new node a seed and republishes the seed provider."""
    runner.target_node.is_seed = True

    GracefulStopTerminateAndReplaceMonkey(runner).disrupt()

    new_node.set_seed_flag.assert_called_once_with(True)
    runner.cluster.update_seed_provider.assert_called_once_with()


def test_disrupt_accepts_new_node_reusing_old_ip(runner, new_node):
    """When the replacement gets the old IP back, that IP must show up as UN instead of disappearing."""
    new_node.ip_address = OLD_IP
    runner.cluster.get_nodetool_status.side_effect = [
        ALL_UP,
        {"dc1": {OLD_IP: {"state": "DN"}}},
        {"dc1": {OLD_IP: {"state": "UN"}}},
    ]

    GracefulStopTerminateAndReplaceMonkey(runner).disrupt()

    assert runner.cluster.get_nodetool_status.call_count == 3


def test_disrupt_major_compacts_a_survivor_before_the_stop_on_the_first_run(compacting_runner, parent, survivor):
    """With the option set to 2 the first replace compacts a node other than the target, and does so
    before the target is stopped, so one replica has purged its repaired tombstones when the rebuild starts."""
    GracefulStopTerminateAndReplaceMonkey(compacting_runner).disrupt()

    assert parent.mock_calls[:2] == [call.compact_survivor("compact"), call.stop_scylla_server(verify_down=True)]
    compacting_runner.target_node.run_nodetool.assert_not_called()


def test_disrupt_skips_the_survivor_compaction_on_every_other_run(compacting_runner, survivor, new_node):
    """The second run of the same instance is the control cycle: no survivor is compacted before the replace."""
    nemesis = GracefulStopTerminateAndReplaceMonkey(compacting_runner)
    nemesis.disrupt()
    survivor.run_nodetool.reset_mock()
    compacting_runner.cluster.get_nodetool_status.side_effect = [
        ALL_UP,
        {"dc1": {OLD_IP: {"state": "DN"}}},
        {"dc1": {}},
    ]

    nemesis.disrupt()

    survivor.run_nodetool.assert_not_called()


def test_disrupt_never_compacts_when_option_is_zero(runner, survivor):
    """The default of 0 keeps the replace as it was: no node is compacted before the target is stopped."""
    GracefulStopTerminateAndReplaceMonkey(runner).disrupt()

    survivor.run_nodetool.assert_not_called()
