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
import math
import os
import re
import time

from sdcm.sct_events import EVENTS_PROCESSES
from sdcm.send_email import LongevityEmailReporter
from sdcm.tester import ClusterTester
from sdcm.utils.common import format_timestamp, retrying

KB_SIZE = 2 ** 10
MB_SIZE = KB_SIZE * 1024
GB_SIZE = MB_SIZE * 1024


class IncrementalCompactionTest(ClusterTester):
    """
    Test Incremental Compaction Strategy test cases.
    """

    def _run_all_stress_cmds(self, stress_queue, params):
        stress_cmds = params['stress_cmd']
        if not isinstance(stress_cmds, list):
            stress_cmds = [stress_cmds]
        # In some cases we want the same stress_cmd to run several times (can be used with round_robin or not).
        stress_multiplier = self.params.get('stress_multiplier', default=1)
        if stress_multiplier > 1:
            stress_cmds *= stress_multiplier

        for stress_cmd in stress_cmds:
            params.update({'stress_cmd': stress_cmd})
            self._parse_stress_cmd(stress_cmd, params)

            # Run all stress commands
            self.log.debug('stress cmd: {}'.format(stress_cmd))
            if stress_cmd.startswith('cassandra-stress'):
                stress_queue.append(self.run_stress_thread(**params))
            elif stress_cmd.startswith('scylla-bench'):
                stress_queue.append(self.run_stress_thread_bench(stress_cmd=stress_cmd, stats_aggregate_cmds=False))
            elif stress_cmd.startswith('bin/ycsb'):
                stress_queue.append(self.run_ycsb_thread(**params))
            time.sleep(10)

            # Remove "user profile" param for the next command
            if 'profile' in params:
                del params['profile']

            if 'keyspace_name' in params:
                del params['keyspace_name']

        return stress_queue

    def _parse_stress_cmd(self, stress_cmd, params):
        # Due to an issue with scylla & cassandra-stress - we need to create the counter table manually
        if 'counter_' in stress_cmd:
            self._create_counter_table()

        if 'compression' in stress_cmd:
            if 'keyspace_name' not in params:
                compression_prefix = re.search('compression=(.*)Compressor', stress_cmd).group(1)
                keyspace_name = "keyspace_{}".format(compression_prefix.lower())
                params.update({'keyspace_name': keyspace_name})

        return params

    default_params = {'timeout': 650000}

    @staticmethod
    def _get_keyspace_name(ks_number, keyspace_pref='keyspace'):
        return '{}{}'.format(keyspace_pref, ks_number)

    def _get_fullscan_params(self):
        params = {}
        fullscan = self.params.get('run_fullscan', default=None)
        if fullscan:
            fullscan = fullscan.split(',')
            params['ks.cf'] = fullscan[0].strip()
            params['interval'] = int(fullscan[1].strip())
            self.log.info('Fullscan target: {} Fullscan interval: {}'.format(params['ks.cf'],
                                                                             params['interval']))
        return params

    def test_space_amplification(self):
        """
        Test ICS space amplification of:
        1. new written data
        2. over-written data
        3. major compaction
        """

        stress_queue = list()
        write_queue = list()
        verify_queue = list()

        # prepare write workload
        prepare_write_cmd = self.params.get('prepare_write_cmd', default=None)
        keyspace_num = 1

        compaction_strategy = self.params.get('compaction_strategy', default=None)
        sstable_size_in_mb = self.params.get('sstable_size_in_mb', default=None)
        self._pre_create_schema(compaction_strategy=compaction_strategy, sstable_size_in_mb=sstable_size_in_mb)


        self._pre_create_schema(keyspace_num, scylla_encryption_options=self.params.get('scylla_encryption_options', None))
        self._run_all_stress_cmds(stress_queue=write_queue, params={'stress_cmd': prepare_write_cmd, 'keyspace_num': keyspace_num})

        # Wait on the queue till all threads come back.
        for stress in write_queue:
            self.verify_stress_thread(cs_thread_pool=stress)

    def test_batch_custom_time(self):
        """
        The test runs like test_custom_time but desgined for running multiple stress commands in batches.
        It take the keyspace_num and calculates the number of batches to run based on batch_size.
        For every batch, it runs the stress and verify them and only then moves to the next batch.

        Test assumes:
        - pre_create_schema (The test pre-creating the schema for all batches)
        - round_robin
        - No nemesis during prepare

        :param keyspace_num: Number of keyspaces to be batched (in future it can be enahnced with number of tables).
        :param batch_size: Number of stress commands to run together in a batch.
        """
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(), tester_obj=self)

        total_stress = self.params.get('keyspace_num')  # In future it may be 1 keyspace but multiple tables in it.
        batch_size = self.params.get('batch_size', default=1)

        prepare_write_cmd = self.params.get('prepare_write_cmd', default=None)
        if prepare_write_cmd:
            self._run_stress_in_batches(total_stress=total_stress, batch_size=batch_size,
                                        stress_cmd=prepare_write_cmd)

        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))

        stress_cmd = self.params.get('stress_cmd', default=None)
        self._run_stress_in_batches(total_stress=batch_size, batch_size=batch_size,
                                    stress_cmd=stress_cmd)

    def _run_stress_in_batches(self, total_stress, batch_size, stress_cmd):
        stress_queue = list()
        self._pre_create_schema(keyspace_num=total_stress)

        num_of_batches = int(total_stress / batch_size)
        for batch in xrange(0, num_of_batches):
            for i in xrange(1 + batch * batch_size, (batch + 1) * batch_size + 1):
                keyspace_name = self._get_keyspace_name(i)
                self._run_all_stress_cmds(stress_queue, params={'stress_cmd': stress_cmd,
                                                                'keyspace_name': keyspace_name, 'round_robin': True})
            for stress in stress_queue:
                self.verify_stress_thread(cs_thread_pool=stress)

    def _create_counter_table(self):
        """
        workaround for the issue https://github.com/scylladb/scylla-tools-java/issues/32
        remove when resolved
        """
        node = self.db_cluster.nodes[0]
        with self.cql_connection_patient(node) as session:
            session.execute("""
                CREATE KEYSPACE IF NOT EXISTS keyspace1
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = true;
            """)
            session.execute("""
                CREATE TABLE IF NOT EXISTS keyspace1.counter1 (
                    key blob PRIMARY KEY,
                    "C0" counter,
                    "C1" counter,
                    "C2" counter,
                    "C3" counter,
                    "C4" counter
                ) WITH COMPACT STORAGE
                    AND bloom_filter_fp_chance = 0.01
                    AND caching = '{"keys":"ALL","rows_per_partition":"ALL"}'
                    AND comment = ''
                    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
                    AND compression = {}
                    AND dclocal_read_repair_chance = 0.1
                    AND default_time_to_live = 0
                    AND gc_grace_seconds = 864000
                    AND max_index_interval = 2048
                    AND memtable_flush_period_in_ms = 0
                    AND min_index_interval = 128
                    AND read_repair_chance = 0.0
                    AND speculative_retry = '99.0PERCENTILE';
            """)

    def _pre_create_schema(self, keyspace_num=1, in_memory=False, scylla_encryption_options=None,
                           compaction_strategy=None, sstable_size_in_mb=None):

        """
        For cases we are testing many keyspaces and tables, It's a possibility that we will do it better and faster than
        cassandra-stress.
        """

        self.log.debug('Pre Creating Schema for c-s with {} keyspaces'.format(keyspace_num))
        for i in xrange(1, keyspace_num+1):
            keyspace_name = 'keyspace{}'.format(i)
            self.create_keyspace(keyspace_name=keyspace_name, replication_factor=3)
            self.log.debug('{} Created'.format(keyspace_name))
            self.create_table(name='standard1', keyspace_name=keyspace_name, key_type='blob', read_repair=0.0, compact_storage=True,
                              columns={'"C0"': 'blob'},
                              in_memory=in_memory, scylla_encryption_options=scylla_encryption_options)


    @retrying(n=80, sleep_time=60, allowed_exceptions=(AssertionError,))
    def _wait_no_compactions_running(self):
        q = "sum(scylla_compaction_manager_compactions{})"
        now = time.time()
        results = self.prometheusDB.query(query=q, start=now - 60, end=now)
        self.log.debug("scylla_compaction_manager_compactions: %s" % results)
        # if all are zeros the result will be False, otherwise there are still compactions
        if results:
            assert any([float(v[1]) for v in results[0]["values"]]) is False, \
                "Waiting until all compactions settle down"

    def _get_used_capacity_gb(self, node):
        """
        :param node:
        :return: the file-system used-capacity on node (in GB)
        """
        used_capacity = self._get_filesystem_total_size_gb(node=node) - self._get_filesystem_available_size_gb(
            node=node)
        self.log.debug("Node {} used capacity is: {} GB".format(node.private_ip_address, used_capacity))
        return used_capacity

    def _get_prometheus_query_numeric_values_list(self, query, start_time=None):
        start_time = start_time or time.time()
        res = self.prometheusDB.query(query=query, start=start_time, end=time.time())
        return res[0]["values"]

    def _get_prometheus_query_numeric_value_gb(self, query):
        res = self._get_prometheus_query_numeric_values_list(query=query)
        return int(res[0][1]) / GB_SIZE

    def _get_filesystem_available_size_gb(self, node, start_time=None):
        """
        :param node:
        :return:
        """
        filesystem_available_size_query = 'sum(node_filesystem_avail{{mountpoint="{0.scylla_dir}", ' \
                                          'instance=~"{1.private_ip_address}"}})'.format(self, node)
        return self._get_prometheus_query_numeric_value_gb(query=filesystem_available_size_query)

    def _get_filesystem_available_size_list(self, node, start_time):
        """
        :param node:
        :return:
        """
        filesystem_available_size_query = 'sum(node_filesystem_avail{{mountpoint="{0.scylla_dir}", ' \
                                          'instance=~"{1.private_ip_address}"}})'.format(self, node)
        return self._get_prometheus_query_numeric_values_list(query=filesystem_available_size_query,
                                                              start_time=start_time)

    def _get_filesystem_total_size_gb(self, node):
        """
        :param node:
        :return:
        """
        filesystem_capacity_query = 'sum(node_filesystem_size{{mountpoint="{0.scylla_dir}", ' \
                                    'instance=~"{1.private_ip_address}"}})'.format(self, node)
        return self._get_prometheus_query_numeric_value_gb(query=filesystem_capacity_query)

    def _get_max_used_capacity_over_time_gb(self, node, start_time):
        """

        :param node:
        :param start_time: the start interval to search max-used-capacity from.
        :return:
        """
        fs_size_gb = self._get_filesystem_total_size_gb(node=node)
        end_time = time.time()
        time_interval_minutes = int(math.ceil((end_time - start_time) / 60))  # convert time to minutes and round up.
        min_available_capacity_gb = min([int(val[1]) for val in self._get_filesystem_available_size_list(node=node,
                                                                                                         start_time=start_time)]) / GB_SIZE
        max_used_capacity_gb = fs_size_gb - min_available_capacity_gb
        self.log.debug("The maximum used filesystem capacity of {} for the last {} minutes is: {} GB/ {} GB".format(
            node.private_ip_address, time_interval_minutes, max_used_capacity_gb, fs_size_gb))
        return max_used_capacity_gb

