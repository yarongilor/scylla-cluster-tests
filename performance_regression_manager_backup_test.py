#!/usr/bin/env python
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
import time

from performance_regression_test import PerformanceRegressionTest
from sdcm.mgmt.operations import ManagerTestFunctionsMixIn
from sdcm.sct_events import Severity
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events.loaders import CassandraStressEvent
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.decorators import latency_calculator_decorator


class PerformanceRegressionManagerBackupTest(PerformanceRegressionTest, ManagerTestFunctionsMixIn):
    """
    Test Scylla Manager Backup performance regression.
    It assumes a relevant Scylla Manager configuration is set up in the YAML file.
    And specifically, a Scylla Manager backup Nemesis
    """

    @latency_calculator_decorator(legend="Stress Baseline", cycle_name="Stress Baseline")
    def _measure_stress_baseline(self, sleep_duration: int = 1800):
        InfoEvent(f'Start steady state test duration ({sleep_duration})').publish()
        time.sleep(sleep_duration)

    def _test_stress_steady_state(self, stress_cmd: str):
        self.run_stress_thread(stress_cmd=stress_cmd, stress_num=1, stats_aggregate_cmds=False)
        self._measure_stress_baseline()
        with EventsSeverityChangerFilter(new_severity=Severity.NORMAL,
                                         event_class=CassandraStressEvent,
                                         extra_time_to_expiration=60):
            self.loaders.kill_stress_thread()

    def test_manager_backup(self):
        keyspace = 'keyspace1'
        table = 'standard1'
        stress_cmd = self.params.get('stress_cmd_m')
        self.run_fstrim_on_all_db_nodes()
        self.preload_data()
        self._test_stress_steady_state(stress_cmd=stress_cmd)
        self.align_cluster_data_state(keyspace, table)
        self.run_workload(stress_cmd=stress_cmd, nemesis=True, sub_type='mixed')
