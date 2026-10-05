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

from unittest.mock import MagicMock

import pytest

from sdcm.utils.tablets.common import temporarily_disable_auto_repair

MODULE = "sdcm.utils.tablets.common"


@pytest.fixture()
def db_cluster():
    """Three-node cluster for the wait-only repair guard."""
    cluster = MagicMock()
    cluster.data_nodes = [MagicMock(name=f"node-{i}") for i in range(3)]
    return cluster


def test_wait_only_guard_drains_all_nodes_without_touching_config(db_cluster, monkeypatch):
    """The debug guard must only wait for in-flight repairs on every data node — writing
    auto-repair config here would re-introduce the disable behavior this branch measures without."""
    drained_nodes = []
    monkeypatch.setattr(
        f"{MODULE}.wait_no_active_repair_tasks", lambda nodes, timeout: drained_nodes.extend(nodes) or True
    )
    with temporarily_disable_auto_repair(db_cluster):
        assert drained_nodes == db_cluster.data_nodes
    for node in db_cluster.data_nodes:
        node.set_scylla_config_param.assert_not_called()
        node.get_scylla_config_param.assert_not_called()


def test_wait_only_guard_proceeds_on_drain_timeout(db_cluster, monkeypatch):
    """A drain timeout must not block the nemesis repair — the guard warns and yields anyway."""
    monkeypatch.setattr(f"{MODULE}.wait_no_active_repair_tasks", lambda nodes, timeout: False)
    body_ran = False
    with temporarily_disable_auto_repair(db_cluster):
        body_ran = True
    assert body_ran
