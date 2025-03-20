import datetime
import re
import time
from functools import partial

from longevity_large_partition_test import LargePartitionLongevityTest
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.common import ParallelObject, skip_optional_stage
from sdcm.utils.compaction_ops import CompactionOps
from sdcm.utils.sstable.sstable_utils import SstableUtils


class ReplicaLoadSheddingTest(LargePartitionLongevityTest):
    keyspace = 'scylla_bench'
    table = 'test'
    ks_cf = f'{keyspace}.{table}'
    MAX_PARTITIONS_NUM = 8
    def test_replica_load_shedding(self):
        """
        Test replica load shedding by overloading a single node with scylla-bench.

        test steps:
        -----------
        Run a prepare step to load s-b dataset on cluster nodes.
        Make sure the number of cluster nodes is bigger than s-b replication-factor.
        This way not all partitions are replicated on all nodes.
        Get several partitions that are located on a specific node.
        This is done by running an sstable dump on node and search s-b partition keys in output.
        Run an s-b stress command for each of the partition received.
        """
        self.pre_create_large_partitions_schema(rf=2)
        InfoEvent(message="Starting prepare step").publish()
        self.run_prepare_write_cmd()
        stress_queue = []
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    tester_obj=self)

        # Run read stress
        stress_read_cmd = self.params.get('stress_read_cmd')
        ## Get partitions that are located on a specific node
        overloaded_node = self.db_cluster.nodes[0]
        InfoEvent(message=f"Selected overloaded node: {overloaded_node.name} {overloaded_node.ip_address}").publish()
        compaction_ops = CompactionOps(cluster=self.db_cluster)
        compaction_ops.disable_autocompaction_on_ks_cf(node=overloaded_node, keyspace=self.keyspace, cf=self.table)
        sstable_utils = SstableUtils(db_node=overloaded_node, ks_cf=self.ks_cf)
        InfoEvent(message=f"Get node {overloaded_node.name} sstables").publish()
        sstables = sstable_utils.get_sstables()
        partition_keys = []
        while len(partition_keys) < self.MAX_PARTITIONS_NUM and sstables:
            sstable = sstables.pop()
            InfoEvent(message=f"Get partition keys from sstable: {sstable}").publish()
            partition_keys.extend(
                sstable_utils.get_partition_keys(sstable=sstable, max_partitions_num=self.MAX_PARTITIONS_NUM))
        compaction_ops.enable_autocompaction_on_ks_cf(node=overloaded_node, keyspace=self.keyspace, cf=self.table)
        stress_read_cmds = [stress_read_cmd.format(key) for key in partition_keys]
        params = {'keyspace_num': 1, 'stress_cmd': stress_read_cmds, 'round_robin': self.params.get('round_robin')}
        InfoEvent(message="Starting read stress").publish()
        self._run_all_stress_cmds(stress_queue, params)

        # # Run write stress
        # InfoEvent(message="Starting write stress").publish()
        # stress_cmd = self.params.get('stress_cmd')
        # params = {'stress_cmd': stress_cmd, 'round_robin': self.params.get('round_robin')}
        # self._run_all_stress_cmds(stress_queue, params)

        # Start nemesis
        self.db_cluster.start_nemesis()

        # Wait for stress completion
        for stress in stress_queue:
            InfoEvent(message=f"Waiting for stress completion: {stress}").publish()
            self.verify_stress_thread(stress)
