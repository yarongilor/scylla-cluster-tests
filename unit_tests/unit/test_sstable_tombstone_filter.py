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
# Copyright (c) 2025 ScyllaDB

"""Unit tests for the tombstone-aware sstable filtering used by destroy-then-repair nemeses."""

import json
from unittest.mock import MagicMock, patch

import pytest

from sdcm.nemesis import NemesisRunner
from sdcm.exceptions import UnsupportedNemesis
from sdcm.utils.sstable.sstable_utils import (
    SstableUtils,
    _find_json_value,
    _tombstone_histogram_is_empty,
)

SSTABLE = "/var/lib/scylla/data/keyspace1/standard1-abcd/me-1-big-Data.db"


def _make_node():
    """Build a minimal fake DB node good enough for SstableUtils statistics dumping."""
    node = MagicMock()
    node.is_enterprise = True
    node.scylla_version = "2026.0.0"
    node.add_install_prefix.side_effect = lambda path: path
    return node

def _make_sstable_utils(node):
    return SstableUtils(db_node=node, ks_cf="keyspace1.standard1")


def _result(ok=True, stdout="", stderr="", exit_status=0):
    res = MagicMock()
    res.ok = ok
    res.stdout = stdout
    res.stderr = stderr
    res.exit_status = exit_status
    return res


def _statistics_json(histogram):
    return json.dumps({"sstables": {SSTABLE: {"statistics": {"estimated_tombstone_drop_time": histogram}}}})


@pytest.mark.parametrize(
    "histogram, expected_empty",
    [
        ({}, True),
        ([], True),
        (None, True),
        ({"bin": {}}, True),
        ({"elements": []}, True),
        ({"max_bin_size": 100, "bin": {}}, True),  # non-empty wrapper metadata but empty bin
        ({"1739112345": 3}, False),
        ({"bin": {"1739112345": 3}}, False),
        ({"elements": [{"key": 1, "value": 2}]}, False),
    ],
)
def test_tombstone_histogram_is_empty(histogram, expected_empty):
    assert _tombstone_histogram_is_empty(histogram) is expected_empty


def test_find_json_value_locates_nested_key():
    data = {"sstables": {"path": {"statistics": {"estimated_tombstone_drop_time": {"1": 2}}}}}
    assert _find_json_value(data, "estimated_tombstone_drop_time") == {"1": 2}


def test_find_json_value_returns_empty_container_not_none():
    data = {"a": {"estimated_tombstone_drop_time": {}}}
    assert _find_json_value(data, "estimated_tombstone_drop_time") == {}


def test_find_json_value_missing_key():
    assert _find_json_value({"a": {"b": 1}}, "estimated_tombstone_drop_time") is None


def test_has_tombstones_false_for_empty_histogram():
    node = _make_node()
    node.remoter.run.side_effect = [
        _result(exit_status=0),  # test -f
        _result(ok=True, stdout=_statistics_json({})),  # dump-statistics
    ]
    assert _make_sstable_utils(node).sstable_has_tombstones(SSTABLE) is False


def test_has_tombstones_true_for_non_empty_histogram():
    node = _make_node()
    node.remoter.run.side_effect = [
        _result(exit_status=0),
        _result(ok=True, stdout=_statistics_json({"1739112345": 5})),
    ]
    assert _make_sstable_utils(node).sstable_has_tombstones(SSTABLE) is True


def test_has_tombstones_conservative_when_file_missing():
    node = _make_node()
    node.remoter.run.side_effect = [_result(exit_status=1)]  # test -f fails
    assert _make_sstable_utils(node).sstable_has_tombstones(SSTABLE) is True


def test_has_tombstones_conservative_on_dump_failure():
    node = _make_node()
    node.remoter.run.side_effect = [
        _result(exit_status=0),
        _result(ok=False, stderr="boom"),
    ]
    assert _make_sstable_utils(node).sstable_has_tombstones(SSTABLE) is True


def test_has_tombstones_conservative_on_bad_json():
    node = _make_node()
    node.remoter.run.side_effect = [
        _result(exit_status=0),
        _result(ok=True, stdout="not-json"),
    ]
    assert _make_sstable_utils(node).sstable_has_tombstones(SSTABLE) is True


def test_has_tombstones_conservative_when_histogram_field_absent():
    node = _make_node()
    node.remoter.run.side_effect = [
        _result(exit_status=0),
        _result(ok=True, stdout=json.dumps({"sstables": {SSTABLE: {"statistics": {}}}})),
    ]
    assert _make_sstable_utils(node).sstable_has_tombstones(SSTABLE) is True


def test_filter_out_sstables_with_tombstones(monkeypatch):
    node = _make_node()
    sstable_utils = _make_sstable_utils(node)
    clean = "/data/clean-Data.db"
    dirty = "/data/dirty-Data.db"

    monkeypatch.setattr(
        sstable_utils,
        "_get_sstables_tombstone_status",
        lambda sstables: {s: (s == dirty) for s in sstables},
    )

    assert sstable_utils.filter_out_sstables_with_tombstones([clean, dirty]) == [clean]
    assert sstable_utils.filter_out_sstables_with_tombstones([dirty]) == []
    assert sstable_utils.filter_out_sstables_with_tombstones([clean]) == [clean]
    assert sstable_utils.filter_out_sstables_with_tombstones([]) == []


def _batch_statistics_json(status_by_sstable):
    """Build a batched dump-statistics JSON: empty histogram = clean, populated = tombstones."""
    return json.dumps(
        {
            "sstables": {
                sstable: {"statistics": {"estimated_tombstone_drop_time": ({"1739112345": 5} if dirty else {})}}
                for sstable, dirty in status_by_sstable.items()
            }
        }
    )


def test_batch_status_single_process_for_many_sstables():
    node = _make_node()
    clean = ["/data/c1-Data.db", "/data/c2-Data.db"]
    dirty = ["/data/d1-Data.db"]
    node.remoter.run.return_value = _result(
        ok=True,
        stdout=_batch_statistics_json({clean[0]: False, clean[1]: False, dirty[0]: True}),
    )

    sstable_utils = _make_sstable_utils(node)
    status = sstable_utils._get_sstables_tombstone_status(clean + dirty)

    assert status == {clean[0]: False, clean[1]: False, dirty[0]: True}
    # A single batched process handled all three sstables.
    assert node.remoter.run.call_count == 1


def test_batch_status_chunks_respect_batch_size(monkeypatch):
    node = _make_node()
    sstable_utils = _make_sstable_utils(node)
    monkeypatch.setattr(type(sstable_utils), "SSTABLE_DUMP_BATCH_SIZE", 2)
    sstables = [f"/data/{i}-Data.db" for i in range(5)]
    node.remoter.run.side_effect = lambda cmd, **kw: _result(
        ok=True, stdout=_batch_statistics_json({s: False for s in sstables if s in cmd})
    )

    status = sstable_utils._get_sstables_tombstone_status(sstables)

    assert status == {s: False for s in sstables}
    assert node.remoter.run.call_count == 3  # ceil(5 / 2)


def test_batch_status_missing_entry_is_conservative():
    node = _make_node()
    present = "/data/present-Data.db"
    missing = "/data/missing-Data.db"
    node.remoter.run.return_value = _result(ok=True, stdout=_batch_statistics_json({present: False}))

    status = _make_sstable_utils(node)._get_sstables_tombstone_status([present, missing])

    assert status[present] is False
    assert status[missing] is True  # not in dump -> treated as containing tombstones


def test_batch_status_falls_back_to_per_sstable_on_dump_failure():
    node = _make_node()
    s1 = "/data/s1-Data.db"
    s2 = "/data/s2-Data.db"
    node.remoter.run.side_effect = [
        _result(ok=False, stderr="boom"),  # batch dump fails
        _result(exit_status=0),  # s1 test -f
        _result(ok=True, stdout=_statistics_json({})),  # s1 dump (clean)
        _result(exit_status=0),  # s2 test -f
        _result(ok=True, stdout=_statistics_json({"1": 1})),  # s2 dump (dirty)
    ]
    status = _make_sstable_utils(node)._get_sstables_tombstone_status([s1, s2])
    assert status == {s1: False, s2: True}



# ---------------------------------------------------------------------------
# _destroy_data_and_restart_scylla destroy-count / clean-pool selection
# ---------------------------------------------------------------------------

# The method is decorated with @decorate_with_context(...); call the undecorated function
# so the test does not depend on the event-suppression machinery.
_destroy = NemesisRunner._destroy_data_and_restart_scylla.__wrapped__


def _make_nemesis_self(full_sstables, clean_sstables):
    """Build a MagicMock `self` for NemesisRunner._destroy_data_and_restart_scylla.

    ``get_all_sstables`` is mocked to return ``full_sstables`` for the unfiltered call and
    ``clean_sstables`` for the tombstone-filtered call. The rm target files are recorded on
    ``destroyed`` so tests can assert what was actually deleted.
    """
    nem = MagicMock()
    nem.cluster.get_non_system_ks_cf_list.return_value = ["keyspace1.standard1"]
    nem.target_node.name = "node-1"

    def get_all_sstables(tables, node=None, skip_sstables_with_tombstones=False):
        return list(clean_sstables) if skip_sstables_with_tombstones else list(full_sstables)

    nem.get_all_sstables.side_effect = get_all_sstables
    nem.replace_full_file_name_to_prefix.side_effect = lambda one_file, ks_cf_for_destroy: one_file

    destroyed = []
    rm_result = MagicMock()
    rm_result.stderr = ""

    def sudo(cmd):
        destroyed.append(cmd.replace("rm -f ", ""))
        return rm_result

    nem.target_node.remoter.sudo.side_effect = sudo
    return nem, destroyed


def _call_destroy(nem, **kwargs):
    with patch("sdcm.nemesis.DbNodeLogger", MagicMock()):
        _destroy(nem, **kwargs)


def test_destroy_target_is_percent_of_total_and_selects_clean_only():
    # 10 total sstables, 6 tombstone-free. 50% of TOTAL (=5) must be destroyed, all from the clean pool.
    clean = [f"/clean/{i}-Data.db" for i in range(6)]
    dirty = [f"/dirty/{i}-Data.db" for i in range(4)]
    nem, destroyed = _make_nemesis_self(full_sstables=clean + dirty, clean_sstables=clean)

    _call_destroy(nem, sstables_to_destroy_perc=50, skip_sstables_with_tombstones=True)

    assert len(destroyed) == 5  # 50% of 10 total, NOT 50% of the 6 clean ones (=3)
    assert set(destroyed).issubset(set(clean))  # only tombstone-free sstables were deleted
    assert set(destroyed).isdisjoint(set(dirty))


def test_destroy_count_capped_by_available_clean_sstables():
    # 10 total → target 5, but only 3 clean sstables exist → destroy at most 3.
    clean = [f"/clean/{i}-Data.db" for i in range(3)]
    dirty = [f"/dirty/{i}-Data.db" for i in range(7)]
    nem, destroyed = _make_nemesis_self(full_sstables=clean + dirty, clean_sstables=clean)

    _call_destroy(nem, sstables_to_destroy_perc=50, skip_sstables_with_tombstones=True)

    assert len(destroyed) == 3
    assert set(destroyed) == set(clean)


def test_destroy_without_filtering_uses_full_pool():
    # skip_sstables_with_tombstones=False (e.g. rebuild nemeses): no filtering, 50% of total destroyed.
    full = [f"/s/{i}-Data.db" for i in range(10)]
    nem, destroyed = _make_nemesis_self(full_sstables=full, clean_sstables=[])

    _call_destroy(nem, sstables_to_destroy_perc=50, skip_sstables_with_tombstones=False)

    assert len(destroyed) == 5
    assert set(destroyed).issubset(set(full))
    # get_all_sstables must be called without requesting tombstone filtering.
    assert all(
        call.kwargs.get("skip_sstables_with_tombstones", False) is False
        for call in nem.get_all_sstables.call_args_list
    )


def test_destroy_raises_when_no_clean_sstables():
    nem, _ = _make_nemesis_self(full_sstables=["/s/0-Data.db"], clean_sstables=[])

    with pytest.raises(UnsupportedNemesis):
        _call_destroy(nem, sstables_to_destroy_perc=50, skip_sstables_with_tombstones=True)

