from longevity_test import LongevityTest


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

    def test_disable_enable_ttl_scan(self):
        """
        1. Run pre-create schema for Alternator, disabling TTL for tables.
        2. Run the original test_custom_time prepare step.
        3. Enable TTL for tables.
        4. Wait for TTL-scan intervals to run
        5. Run a background read stress while data is being expired.
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements

        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    tester_obj=self)
        stress_queue = []
        # prepare write workload
        prepare_write_cmd = self.params.get('prepare_write_cmd')
        keyspace_num = 1
        self.pre_create_alternator_tables()
        # Disable TTL
        self.alternator.modify_alternator_ttl_spec(enabled=False, node=self.db_cluster.nodes[0])

        # Run write stress
        self.run_prepare_write_cmd()
        stress_cmd = self.params.get('stress_cmd')
        if stress_cmd:
            params = {'keyspace_num': keyspace_num, 'stress_cmd': stress_cmd,
                      'round_robin': self.params.get('round_robin')}
            self._run_all_stress_cmds(stress_queue, params)

        if not prepare_write_cmd or not self.params.get('nemesis_during_prepare'):
            self.db_cluster.start_nemesis()

        for stress in stress_queue:
            self.verify_stress_thread(cs_thread_pool=stress)

        # Enable TTL
        self.alternator.modify_alternator_ttl_spec(enabled=True, node=self.db_cluster.nodes[0])
        stress_queue = []

        # Run read stress as a background thread while TTL scans are going over existing data.
        stress_read_cmd = self.params.get('stress_read_cmd')
        if stress_read_cmd:
            params = {'keyspace_num': keyspace_num, 'stress_cmd': stress_read_cmd}
            self._run_all_stress_cmds(stress_queue, params)

        for stress in stress_queue:
            self.verify_stress_thread(cs_thread_pool=stress)
