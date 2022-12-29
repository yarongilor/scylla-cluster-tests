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
# Copyright (c) 2020 ScyllaDB

# This is stress longevity test that runs light weight transactions in parallel with different node operations:
# disruptive and not disruptive
#
# After the test is finished will be performed the data validation.

import time
from unittest.mock import MagicMock

from longevity_test import LongevityTest
from sdcm.sct_events import Severity
from sdcm.sct_events.health import DataValidatorEvent
from sdcm.utils.data_validator import LongevityDataValidator
from sdcm.sct_events.group_common_events import ignore_mutation_write_errors


class LWTLongevityTest(LongevityTest):
    BASE_TABLE_PARTITION_KEYS = ['domain', 'published_date']

    def __init__(self, *args):
        super().__init__(*args)
        self.data_validator = None

    def run_prepare_write_cmd(self):
        # `mutation_write_*' errors are thrown when system is overloaded and got timeout on
        # operations on system.paxos table.
        #
        # Decrease severity of this event during prepare.  Shouldn't impact on test result.
        with ignore_mutation_write_errors():
            super().run_prepare_write_cmd()

    def start_nemesis(self):
        self.db_cluster.start_nemesis()

    def test_lwt_longevity(self):
        with ignore_mutation_write_errors():
            self.test_custom_time()

