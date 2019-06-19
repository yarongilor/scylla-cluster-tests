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
# Copyright (c) 2016 ScyllaDB
import datetime

import time
from avocado import main
from enum import Enum

from sdcm.tester import ClusterTester

class CompactionStrategy(Enum):
    STCS = "SizeTieredCompactionStrategy"
    LCS = "LeveledCompactionStrategy"
    ICS = "IncrementalCompactionStrategy"


class IncrementalCompactionTest(ClusterTester):
    """
    Test Scylla Manager operations on Scylla cluster.

    :avocado: enable
    """

    def _pre_create_schema(self, keyspace_num=1, in_memory=False, scylla_encryption_options=None, rf=3, compaction=None):
        """

        :param self:
        :param keyspace_num:
        :param in_memory:
        :param scylla_encryption_options:
        :return:
        """
        node = self.db_cluster.nodes[0]
        with self.cql_connection_patient(node) as session:
            self.log.debug('Pre Creating Schema for c-s with {} keyspaces and {} compaction and RF of - {} . '.format(keyspace_num, compaction, rf))

            for i in xrange(1, keyspace_num + 1):
                keyspace_name = 'keyspace{}'.format(i)
                self.create_ks(session, keyspace_name, rf=rf)
                self.log.debug('{} Created'.format(keyspace_name))
                self.create_cf(session, 'standard1', key_type='blob', read_repair=0.0, compact_storage=True,
                               columns={'"C0"': 'blob', '"C1"': 'blob', '"C2"': 'blob', '"C3"': 'blob',
                                        '"C4"': 'blob'},
                               in_memory=in_memory, scylla_encryption_options=scylla_encryption_options, compaction=compaction)

    def test_compaction_space_amplification(self):
        """

            Test steps:
            1) Run cassandra stress on cluster.
        """
        # Util functions ===============================================================================================

        def _get_stress_elapsed_time_sec():
            return int((datetime.datetime.now() - self.start_time).seconds)


        # Util functions ===============================================================================================

        self.log.info('Starting c-s write workload')
        stress_cmd = self.params.get('stress_cmd')
        cs_ops_limit_mb = self.params.get('cs_ops_limit_mb')
        cs_col_size_mb = self.params.get('cs_col_size_mb')
        throughput_kb_sec = cs_ops_limit_mb * cs_col_size_mb * 1024
        self.log.debug("throughput_kb_sec: {} cs_ops_limit_mb: {} cs_col_size_mb: {}".format(throughput_kb_sec, cs_ops_limit_mb, cs_col_size_mb))
        keyspace_num = self.params.get('keyspace_num', default=1)
        pre_create_schema = self.params.get('pre_create_schema', default=False)
        if pre_create_schema:
            rf = min(3, len(self.db_cluster.nodes))
            self._pre_create_schema(keyspace_num, compaction=CompactionStrategy.ICS.value, rf=rf)
        stress_cmd_queue = self.run_stress_thread(stress_cmd=stress_cmd, duration=10000)
        self.start_time = datetime.datetime.now()
        self.wait_data_dir_reaching(20, node=self.db_cluster.nodes[0])
        cycles = 2000
        sleep_initial_time = 30
        time.sleep(sleep_initial_time)
        sleep_interval = 10
        
        def _print_debug_node(node, msg):
            self.log.debug(msg="[{}] {}".format(node.private_ip_address, msg))
        for idx in range(cycles):
            self.log.debug("==================>  Cycle #{}  <================".format(idx))
            time.sleep(sleep_interval)
            for node in self.db_cluster.nodes:
                used_size_mb = int(self.get_used_capacity(node=node))
                used_size_kb = used_size_mb * 1024
                _print_debug_node(node=node, msg="Filesystem used capacity is: {}KB ({}MB)".format(used_size_kb, used_size_mb))
                elapsed_seconds = _get_stress_elapsed_time_sec()
                neto_expected_capacity_kb = throughput_kb_sec * elapsed_seconds
                _print_debug_node(node=node, msg="Estimated Neto capacity (after {} seconds) is: {}KB ({}MB)".format(elapsed_seconds, neto_expected_capacity_kb, neto_expected_capacity_kb/1024))
                delta_capacity_kb = used_size_kb - neto_expected_capacity_kb
                _print_debug_node(node=node, msg="Delta used capacity is: {}KB ({}MB)".format(delta_capacity_kb, delta_capacity_kb/1024))
                _print_debug_node(node=node, msg="Space amplification percentage is: {}".format(used_size_kb/neto_expected_capacity_kb*100))



            # try:
            #     res = session.execute(count_table_rows_query)
            # except Exception as e:
            #     self.log.warning("CQL session got exception: {}".format(e))
            #     session = _get_cql_session_and_use_keyspace(node=node, keyspace=KEYSPACE_NAME)
            #     continue

            # self.log.debug("res object type is: {}".format(type(res)))
            # self.log.debug("Number of rows query for table: {}.{} is:".format(KEYSPACE_NAME, TABLE_NAME))
            # for row in res:
            #     self.log.debug(row)
            #     # self.log.debug(type(row))
            #     str_row = str(row)
            #     str_row_strip_prefix = str_row.strip("Row(count=").strip("count=")
            #     str_row_stripp_all = str_row_strip_prefix.strip(")")
            #     int_row = int(str_row_stripp_all)
            #     self.log.debug(str_row)
            #     self.log.debug(str_row_strip_prefix)
            #     self.log.debug(str_row_stripp_all)
            #     self.log.debug(int_row)
            #
            #     rows_num = int(str(row).strip("Row(count=").strip(")"))
            #     self.log.debug("Average capacity per row is: {}".format(float(used_size_kb)/float(int_row)))


            # self.log.debug("current_rows():")
            # self.log.debug(res.current_rows)
        # q = "sum(rate(scylla_hints_manager_sent{}[15s]))"
        # node_fs_size_query = "(sum(node_filesystem_size{mountpoint="$mount_point", instance=~"$node"})"
        # node_filesystem_avail{mountpoint="/"}/node_filesystem_size{mountpoint="/"}*100
        # now = time.time()
        # check status of sending hints during last minute range
        # results = self.prometheusDB.query(query=q, start=now - 60, end=now)


        # self.log.info('Sleeping for 60s to let cassandra-stress run...')
        # time.sleep(60)
        # self.log.debug("test_yg_dbg: initialize nemesis")
        # self.db_cluster.add_nemesis(nemesis=InvokeSecondRebuildMonkey, tester_obj=self)
        # self.db_cluster.start_nemesis()

    def get_used_capacity(self, node):
        # (sum(node_filesystem_size{mountpoint="/var/lib/scylla"})-sum(node_filesystem_avail{mountpoint="/var/lib/scylla"}))
        filesystem_capacity_query = 'sum(node_filesystem_size{{mountpoint="{0.scylla_dir}", ' \
            'instance=~"{1.private_ip_address}"}})'.format(self, node)

        self.log.debug("filesystem_capacity_query: {}".format(filesystem_capacity_query))

        fs_size_res = self.prometheusDB.query(query=filesystem_capacity_query, start=time.time(), end=time.time())
        kb_size = 2 ** 10
        mb_size = kb_size * 1024
        gb_size = mb_size * 1024
        fs_size_gb = int(fs_size_res[0]["values"][0][1]) / gb_size
        self.log.debug("fs_cap_res: {}".format(fs_size_res))
        used_capacity_query = '(sum(node_filesystem_size{{mountpoint="{0.scylla_dir}", ' \
            'instance=~"{1.private_ip_address}"}})-sum(node_filesystem_avail{{mountpoint="{0.scylla_dir}", ' \
            'instance=~"{1.private_ip_address}"}}))'.format(self, node)

        self.log.debug("used_capacity_query: {}".format(used_capacity_query))

        used_cap_res = self.prometheusDB.query(query=used_capacity_query, start=time.time(), end=time.time())
        self.log.debug("used_cap_res: {}".format(used_cap_res))

        assert used_cap_res, "No results from Prometheus"
        used_size_mb = int(used_cap_res[0]["values"][0][1]) / mb_size
        self.log.debug("The used filesystem capacity is: {} MB/ {} GB".format(used_size_mb, fs_size_gb))
        return used_size_mb


if __name__ == '__main__':
    main()
