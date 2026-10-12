# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""Node replacement modelled on the Scylla Cloud "replace dead node" flow."""

import time

from sdcm.nemesis import NemesisBaseClass, target_all_nodes
from sdcm.sct_events.group_common_events import suppress_expected_unavailability_errors
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.decorators import retrying

# Fallback when 'nemesis_dead_node_delay' is unset: the same 5 minutes NodeTerminateAndReplace waits.
DEFAULT_DEAD_NODE_DELAY = 300


@target_all_nodes
class GracefulStopTerminateAndReplaceMonkey(NemesisBaseClass):
    """Stop Scylla gracefully, terminate the instance, leave the cluster degraded, then replace the node.

    Mirrors how a Scylla Cloud node gets replaced (CUSTOMER-750): the node shuts Scylla down
    cleanly, so peers get a gossip shutdown message, then the instance disappears and the
    cluster runs one node short until the "replace dead node" workflow starts a replacement
    by host id. The dead period is ``nemesis_dead_node_delay`` seconds (the customer's was
    ~2h40m), during which writes keep flowing to the two surviving replicas.

    ``NodeTerminateAndReplace`` covers the same replace path but kills the instance cold and
    waits a fixed 5 minutes. The replacement itself is identical: a new instance boots with
    ``replace_node_first_boot`` and, with repair-based node operations, rebuilds every tablet
    from the survivors.
    """

    disruptive = True
    kubernetes = False
    topology_changes = True
    zero_node_changes = True

    def _node_state(self, node_ip: str) -> str | None:
        status = self.runner.cluster.get_nodetool_status()
        states = [val["state"] for dc in status.values() for ip, val in dc.items() if ip == node_ip]
        return states[0] if states else None

    def disrupt(self):
        runner = self.runner
        target_node = runner.target_node
        old_node_ip = target_node.ip_address
        host_id = target_node.host_id
        is_old_node_seed = target_node.is_seed
        rack = target_node.rack
        is_zero_node = target_node._is_zero_token_node
        dead_node_delay = int(runner.cluster.params.get("nemesis_dead_node_delay") or DEFAULT_DEAD_NODE_DELAY)

        InfoEvent(
            message=f"StartEvent - Gracefully stop {target_node.name}, terminate it and keep it dead "
            f"for {dead_node_delay}s before replacing it"
        ).publish()
        with suppress_expected_unavailability_errors():
            with runner.action_log_scope(f"Graceful scylla stop on {target_node.name} node"):
                target_node.stop_scylla_server(verify_down=True)
            runner.terminate_node(target_node=target_node)
            runner.log.info("Cluster runs without %s for %s seconds", target_node.name, dead_node_delay)
            time.sleep(dead_node_delay)
            assert self._node_state(old_node_ip) == "DN", "Terminated node state should be DN"
            InfoEvent(message=f"FinishEvent - {target_node.name} was stopped and terminated").publish()
            new_node = runner.replace_node(old_node_ip, host_id, rack=rack, is_zero_node=is_zero_node)

        try:
            # Wait until the cluster forgets the old node; default 'ring_delay_ms' is 300000.
            @retrying(n=20, sleep_time=20, allowed_exceptions=(AssertionError,))
            def wait_for_old_node_to_be_removed():
                state = self._node_state(old_node_ip)
                if old_node_ip == new_node.ip_address:
                    assert state == "UN", f"New node reusing the old IP should be UN but was: {state}"
                else:
                    assert state is None, f"Old node should be gone from nodetool status but was: {state}"

            wait_for_old_node_to_be_removed()
        finally:
            if is_old_node_seed:
                new_node.set_seed_flag(True)
                runner.cluster.update_seed_provider()
        InfoEvent(message=f"FinishEvent - {new_node.name} replaced {target_node.name}").publish()
