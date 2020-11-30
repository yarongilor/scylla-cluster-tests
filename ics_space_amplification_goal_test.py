import math
import time
from textwrap import dedent

from longevity_test import LongevityTest
from sdcm.cluster import SCYLLA_DIR, BaseNode
from sdcm.sct_events import InfoEvent
from test_lib.compaction import CompactionStrategy, LOGGER

KB_SIZE = 2 ** 10
MB_SIZE = KB_SIZE * 1024
GB_SIZE = MB_SIZE * 1024
FS_SIZE_METRIC = 'node_filesystem_size_bytes'
FS_SIZE_METRIC_OLD = 'node_filesystem_size'
AVAIL_SIZE_METRIC = 'node_filesystem_avail_bytes'
AVAIL_SIZE_METRIC_OLD = 'node_filesystem_avail'


class IcsSpaceAmplificationTest(LongevityTest):

    def _get_used_capacity_gb(self, node):  # pylint: disable=too-many-locals
        #  example: node_filesystem_size_bytes{mountpoint="/var/lib/scylla",
        #  instance=~".*?10.0.79.46.*?"}-node_filesystem_avail_bytes{mountpoint="/var/lib/scylla",
        #  instance=~".*?10.0.79.46.*?"}
        node_capacity_query_postfix = generate_node_capacity_query_postfix(node)
        filesystem_capacity_query = f'{FS_SIZE_METRIC}{node_capacity_query_postfix}'
        used_capacity_query = f'{filesystem_capacity_query}-{AVAIL_SIZE_METRIC}{node_capacity_query_postfix}'
        self.log.debug(f"filesystem_capacity_query: {filesystem_capacity_query}")

        fs_size_res = self.prometheus_db.query(query=filesystem_capacity_query, start=int(time.time()) - 5,
                                               end=int(time.time()))
        if not fs_size_res:
            self.log.warning(f"No results from Prometheus query: {filesystem_capacity_query}")
            return 0
            # assert fs_size_res, "No results from Prometheus"
        if not fs_size_res[0]:  # if no returned values - try the old metric names.
            filesystem_capacity_query = f'{FS_SIZE_METRIC_OLD}{node_capacity_query_postfix}'
            used_capacity_query = f'{filesystem_capacity_query}-{AVAIL_SIZE_METRIC_OLD}{node_capacity_query_postfix}'
            self.log.debug(f"filesystem_capacity_query: {filesystem_capacity_query}")
            fs_size_res = self.prometheus_db.query(query=filesystem_capacity_query, start=int(time.time()) - 5,
                                                   end=int(time.time()))

        assert fs_size_res[0], "Could not resolve capacity query result."
        self.log.debug(f"used_capacity_query: {used_capacity_query}")
        used_cap_res = self.prometheus_db.query(query=used_capacity_query, start=int(time.time()) - 5,
                                                end=int(time.time()))
        assert used_cap_res, "No results from Prometheus"
        used_size_mb = float(used_cap_res[0]["values"][0][1]) / float(MB_SIZE)
        used_size_gb = round(float(used_size_mb / 1024), 2)
        self.log.debug(
            f"The used filesystem capacity on node {node.public_ip_address} is: {used_size_mb} MB/ {used_size_gb} GB")
        return used_size_gb

    def _get_filesystem_available_size_list(self, node, start_time):
        """
        :returns a list of file-system free-capacity on point in time from 'start_time' up to now.
        """
        node_capacity_query_postfix = generate_node_capacity_query_postfix(node)
        available_capacity_query = f'{AVAIL_SIZE_METRIC}{node_capacity_query_postfix}'
        available_size_res = self.prometheus_db.query(query=available_capacity_query, start=int(start_time),
                                                      end=int(time.time()))
        assert available_size_res, "No results from Prometheus"
        if not available_size_res[0]:  # if no returned values - try the old metric names.
            available_capacity_query = f'{AVAIL_SIZE_METRIC_OLD}{node_capacity_query_postfix}'
            available_size_res = self.prometheus_db.query(query=available_capacity_query, start=int(start_time),
                                                          end=int(time.time()))
        assert available_size_res[0], "Could not resolve available-size query result."
        return available_size_res[0]["values"]

    def _get_filesystem_total_size_gb(self, node):
        """
        :param node:
        :return:
        """
        fs_size_metric = 'node_filesystem_size_bytes'
        fs_size_metric_old = 'node_filesystem_size'
        node_capacity_query_postfix = generate_node_capacity_query_postfix(node)
        filesystem_capacity_query = f'{fs_size_metric}{node_capacity_query_postfix}'
        fs_size_res = self.prometheus_db.query(query=filesystem_capacity_query, start=int(time.time()) - 5,
                                               end=int(time.time()))
        assert fs_size_res, "No results from Prometheus"
        if not fs_size_res[0]:  # if no returned values - try the old metric names.
            filesystem_capacity_query = f'{fs_size_metric_old}{node_capacity_query_postfix}'
            self.log.debug(f"filesystem_capacity_query: {filesystem_capacity_query}")
            fs_size_res = self.prometheus_db.query(query=filesystem_capacity_query, start=int(time.time()) - 5,
                                                   end=int(time.time()))
        assert fs_size_res[0], "Could not resolve capacity query result."
        self.log.debug(f"fs_size_res: {fs_size_res}")
        fs_size_gb = float(fs_size_res[0]["values"][0][1]) / float(GB_SIZE)
        return fs_size_gb

    def _get_max_used_capacity_over_time_gb(self, node, start_time) -> float:
        """

        :param node: DB node under test.
        :param start_time: the start interval to search max-used-capacity from.
        :return:
        """
        fs_size_gb = self._get_filesystem_total_size_gb(node=node)
        end_time = time.time()
        time_interval_minutes = int(math.ceil((end_time - start_time) / 60))  # convert time to minutes and round up.
        min_available_capacity_gb = min(
            [int(val[1]) for val in
             self._get_filesystem_available_size_list(node=node, start_time=start_time)]) / GB_SIZE
        max_used_capacity_gb = fs_size_gb - min_available_capacity_gb
        self.log.debug(
            f"The maximum used filesystem capacity of {node.private_ip_address} for the last {time_interval_minutes}"
            f" minutes is: {max_used_capacity_gb} GB/ {fs_size_gb} GB")
        return max_used_capacity_gb

    def _get_nodes_space_ampl_over_time_gb(self, dict_nodes_initial_capacity, start_time,
                                           written_data_size_gb=0):
        dict_nodes_space_amplification = {}
        dict_nodes_used_capacity = self._get_nodes_used_capacity()
        for node in self.db_cluster.nodes:
            node_initial_capacity = dict_nodes_initial_capacity[node.private_ip_address]
            node_used_capacity = dict_nodes_used_capacity[node.private_ip_address]
            node_max_used_capacity_gb = self._get_max_used_capacity_over_time_gb(node=node,
                                                                                 start_time=start_time)
            dict_nodes_space_amplification[node.private_ip_address] = \
                round(node_max_used_capacity_gb - written_data_size_gb - node_initial_capacity, 2)
            self.log.info(
                f"Node {node.private_ip_address} used capacity changed from {node_initial_capacity}"
                f" to {node_used_capacity}.")
            self.log.info(f"Space amplification is: {dict_nodes_space_amplification[node.private_ip_address]} GB")
        return dict_nodes_space_amplification

    def measure_nodes_space_amplification_after_write(self, dict_nodes_initial_capacity, written_data_size_gb,
                                                      start_time):
        self.log.info(f"Space amplification results after a write of: {written_data_size_gb} are:")
        dict_nodes_space_amplification = self._get_nodes_space_ampl_over_time_gb(
            dict_nodes_initial_capacity=dict_nodes_initial_capacity,
            written_data_size_gb=written_data_size_gb, start_time=start_time)
        InfoEvent(message=f"Space amplification results after a write of: {written_data_size_gb} are: "
                          f"{dict_nodes_space_amplification}")

    def _get_nodes_used_capacity(self):
        """
        :rtype: dictionary with capacity per node-ip
        """
        dict_nodes_used_capacity = {}
        for node in self.db_cluster.nodes:
            dict_nodes_used_capacity[node.private_ip_address] = self._get_used_capacity_gb(node=node)
        return dict_nodes_used_capacity

    def _alter_table_compaction(self, compaction_strategy=CompactionStrategy.INCREMENTAL, table_name='standard1',
                                keyspace_name='keyspace1',
                                additional_compaction_params: dict = None):
        """
         Alters table compaction like: ALTER TABLE mykeyspace.mytable WITH
                                        compaction = {'class' : 'IncrementalCompactionStrategy'}
        """

        base_query = f"ALTER TABLE {keyspace_name}.{table_name} WITH compaction = "
        dict_requested_compaction = {'class': compaction_strategy.value}
        if additional_compaction_params:
            dict_requested_compaction.update(additional_compaction_params)

        full_alter_query = base_query + str(dict_requested_compaction)
        LOGGER.debug(f"Alter table query is: {full_alter_query}")
        node1: BaseNode = self.db_cluster.nodes[0]
        node1.run_cqlsh(cmd=full_alter_query)
        InfoEvent(message=f"Altered table by: {full_alter_query}")

    def _set_enforce_min_threshold_true(self):

        yaml_file = "/etc/scylla/scylla.yaml"
        tmp_yaml_file = "/tmp/scylla.yaml"
        set_enforce_min_threshold = dedent("""
                                                    grep -v compaction_enforce_min_threshold {0} > {1}
                                                    echo compaction_enforce_min_threshold: true >> {1}
                                                    cp -f {1} {0}
                                                """.format(yaml_file, tmp_yaml_file))
        for node in self.db_cluster.nodes:  # set compaction_enforce_min_threshold on all nodes
            node.remoter.run('sudo bash -cxe "%s"' % set_enforce_min_threshold)
            self.log.debug("Scylla YAML configuration read from: {} {} is:".format(node.public_ip_address, yaml_file))
            node.remoter.run('sudo cat {}'.format(yaml_file))

            node.stop_scylla_server()
            node.start_scylla_server()

    def test_ics_space_amplification_goal(self):  # pylint: disable=too-many-locals
        """
        (1) writing new data. wait for compactions to finish.
        (2) over-writing existing data.
        (3) measure space amplification after over-writing with SAG=None,1.5,1.2,None
        """

        self._set_enforce_min_threshold_true()
        # (1) writing new data.
        prepare_write_cmd = self.params.get('prepare_write_cmd')
        InfoEvent(message=f"Starting C-S prepare load: {prepare_write_cmd}")
        self.run_prepare_write_cmd()
        InfoEvent(message="Wait for compactions to finish after write is done.")
        self.wait_no_compactions_running()

        stress_cmd = self.params.get('stress_cmd', default=None)
        sag_testing_values = [None, '1.5', '1.2', '1.5', None]
        column_size = 205
        num_of_columns = 5
        # the below number is 1TB (yaml stress cmd total write) in bytes / 205 (column_size) / 5 (num_of_columns)
        overwrite_ops_num = 1072694271
        total_data_to_overwrite_gb = round(overwrite_ops_num * column_size * num_of_columns / (1024 ** 3), 2)
        min_threshold = '4'

        # (2) over-writing existing data.
        for sag in sag_testing_values:
            dict_nodes_capacity_before_overwrite_data = self._get_nodes_used_capacity()
            InfoEvent(
                message=f"Nodes used capacity before start overwriting data:"
                        f" {dict_nodes_capacity_before_overwrite_data}")
            additional_compaction_params = {'min_threshold': min_threshold}
            if sag:
                additional_compaction_params.update({'space_amplification_goal': sag})
            # (3) Altering compaction with SAG=None,1.5,1.2,1.5,None
            self._alter_table_compaction(additional_compaction_params=additional_compaction_params)
            stress_queue = list()
            InfoEvent(message=f"Starting C-S over-write load: {stress_cmd}")

            start_time = time.time()
            params = {'keyspace_num': 1, 'stress_cmd': stress_cmd,
                      'round_robin': self.params.get('round_robin')}
            self._run_all_stress_cmds(stress_queue, params)

            for stress in stress_queue:
                self.verify_stress_thread(cs_thread_pool=stress)

            InfoEvent(message="Wait for compactions to finish after over-write is done.")
            self.wait_no_compactions_running()
            # (3) measure space amplification for the re-written data
            self.measure_nodes_space_amplification_after_write(
                dict_nodes_initial_capacity=dict_nodes_capacity_before_overwrite_data,
                written_data_size_gb=total_data_to_overwrite_gb, start_time=start_time)

        InfoEvent(message=f"Space-amplification-goal testing cycles are done.")


def generate_node_capacity_query_postfix(node):
    return f'{{mountpoint="{SCYLLA_DIR}", instance=~".*?{node.private_ip_address}.*?"}}'
