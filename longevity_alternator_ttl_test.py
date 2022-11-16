from longevity_test import LongevityTest
from test_lib.compaction import CompactionStrategy
from test_lib.scylla_bench_tools import create_scylla_bench_table_query


class AlternatorTtlLongevityTest(LongevityTest):

    def test_custom_time_repeat_stress_cmd(self):
        """
        This test run the original test_custom_time first.
        Then after finished it re-execute the same stress_cmd again.
        An example for a relevant scenario for that is for Alternator TTL that requires overwriting existing TTL data
        after a while, where some items are already expired and/or scanned.
        """

        # Run the stress_cmd a first time
        self.test_custom_time()

        # Rerun the stress_cmd a second time
        stress_cmd = self.params.get('stress_cmd')
        if stress_cmd:
            stress_queue = []
            params = {'stress_cmd': stress_cmd, 'round_robin': self.params.get('round_robin')}
            self._run_all_stress_cmds(stress_queue, params)

            for stress in stress_queue:
                self.verify_stress_thread(cs_thread_pool=stress)

    def test_custom_time_validate_expired_data(self):
        """
        #  TODO: implement the below:
        1. This test run the original test_custom_time first.
        2. After finished it waits for gc-grace-seconds.
        3. Wait for TTL-scan interval.
        4. Select a node and run major compaction (wait for completion).
        5. Verify a 'select' query returns no data for the Alternator TTL table.
        6. Print statistics of cfstat like number of sstables and nodetool status.
        """
        pass
