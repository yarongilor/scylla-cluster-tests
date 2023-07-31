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
# Copyright (c) 2023 ScyllaDB

# pylint: disable=too-many-lines

from __future__ import absolute_import, annotations

import logging
import os
from typing import List

from cassandra import ConsistencyLevel

from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent

LOGGER = logging.getLogger(__name__)


class PartitionsValidationAttributes:  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """
    A class that gathers all data related to partitions-validation.
    It helps Longevity tests that uses "validate_partitions" to
    save and compare a table partitions-rows-number during stress and nemesis.
    """
    PARTITIONS_ROWS_BEFORE = "partitions_rows_before"
    PARTITIONS_ROWS_AFTER = "partitions_rows_after"

    def __init__(self, tester, table_name: str, primary_key_column: str, limit_rows_number: int = 0,  # pylint: disable=too-many-arguments
                 max_partitions_in_test_table: str | None = None,
                 partition_range_with_data_validation: str | None = None, validate_partitions: bool = False):
        """
        limit_rows_number is a limit for querying rows per partition.
        When running a health-check and calling "validate_partitions",
        it would nor read more than this number of rows-per-partition.
        The default is NO limit_rows_number, marked by '0'.
        """
        self.tester = tester
        self.table_name = table_name
        self.primary_key_column = primary_key_column
        self.partition_range_with_data_validation = partition_range_with_data_validation
        self.max_partitions_in_test_table = max_partitions_in_test_table
        self.partitions_rows_collected = False
        self._init_partition_range()
        self.limit_rows_number = limit_rows_number
        self.partitions_dict_before = None
        self.validate_partitions = validate_partitions

    def _init_partition_range(self):
        if self.partition_range_with_data_validation:
            partition_range_splitted = self.partition_range_with_data_validation.split('-')
            self.partition_start_range = int(partition_range_splitted[0])
            self.partition_end_range = int(partition_range_splitted[1])

    @property
    def db_cluster(self):
        return self.tester.db_cluster

    def get_count_pk_rows_query(self, key: str, ignore_limit_rows_number: bool = False) -> str:
        limit_query = f' LIMIT {self.limit_rows_number}' if not ignore_limit_rows_number and self.limit_rows_number else ''
        count_pk_rows_cmd = f'select count(*) from {self.table_name} where ' \
                            f'{self.primary_key_column} = {key}' \
                            f'{limit_query}' \
                            ' using timeout 5m'
        return count_pk_rows_cmd

    def collect_partitions_info(self, ignore_limit_rows_number: bool = False) -> dict | None:
        # Get and save how many rows in each partition.
        # It may be used for validation data in the end of test.
        # By default, the count is limited to partitions_attributes.limit_rows_number (if exist),
        # Unless ignore_limit_rows_number is True.

        error_message = "Failed to collect partition info. Error details: {}"
        try:
            with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0],
                                                        connect_timeout=600) as session:
                session.default_consistency_level = ConsistencyLevel.QUORUM
                pk_list = sorted(get_partition_keys(ks_cf=self.table_name, session=session,
                                                    pk_name=self.primary_key_column))
        except Exception as exc:  # pylint: disable=broad-except
            TestFrameworkEvent(source=self.__class__.__name__, message=error_message.format(exc),
                               severity=Severity.ERROR).publish()
            return None

        # Collect data about partitions' rows amount.
        partitions = {}
        if self.partition_range_with_data_validation:
            # Count existing partitions that intersects with partition_range_with_data_validation
            pk_list = [partition for partition in pk_list if
                       int(partition) in range(self.partition_start_range,
                                               self.partition_end_range)]
        save_into_file_name = self.PARTITIONS_ROWS_BEFORE \
            if not self.partitions_rows_collected else self.PARTITIONS_ROWS_AFTER
        partitions_stats_file = os.path.join(self.tester.logdir, save_into_file_name)
        LOGGER.debug("%s partition-keys to query are in range: %s - %s", len(pk_list), pk_list[0], pk_list[-1])
        with open(partitions_stats_file, 'a', encoding="utf-8") as stats_file:
            for i in pk_list:
                count_pk_rows_cmd = self.get_count_pk_rows_query(key=i,
                                                                 ignore_limit_rows_number=ignore_limit_rows_number)
                try:
                    with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0],
                                                                connect_timeout=600) as session:
                        pk_rows_num_query_result = self.tester.fetch_all_rows(session=session, default_fetch_size=3000,
                                                                              statement=count_pk_rows_cmd, retries=1,
                                                                              timeout=600,
                                                                              raise_on_exceeded=True,
                                                                              verbose=False)
                        pk_rows_num_result = pk_rows_num_query_result[0].count
                except Exception as exc:  # pylint: disable=broad-except
                    TestFrameworkEvent(source=self.__class__.__name__, message=error_message.format(exc),
                                       severity=Severity.ERROR).publish()
                    return None

                partitions[i] = pk_rows_num_result
                stats_file.write('{i}:{rows}, '.format(i=i, rows=partitions[i]))
        LOGGER.info('File with partitions row data: {}'.format(partitions_stats_file))
        if save_into_file_name == self.PARTITIONS_ROWS_BEFORE:
            self.partitions_rows_collected = True
        return partitions

    def collect_initial_partitions_info(self) -> None:
        LOGGER.debug('Save partitions info before reads')
        self.partitions_dict_before = self.collect_partitions_info(ignore_limit_rows_number=True)

    def validate_rows_per_partitions(self, ignore_limit_rows_number: bool = False):
        """
        Validating partition rows-number is the same before and after running a nemesis/stress.
        The purpose of "ignore_limit_rows_number" is to avoid a "too heavy" scan in a too often occurrence,
        e.g. every health check. So a "too heavy" scan will only run twice in a test - after prepare,
        and at the end of test.
        For example, if self.limit_rows_number is 600,000 and there are 10M rows-per-partition,
        only the first 600,000 rows of each partition will be validation during health-checks.
        By default, there is no limit, only when it is specified in yaml by: data_validation - limit_rows_number.
        """
        if self.validate_partitions and self.partitions_dict_before:
            LOGGER.debug('Validate partitions info')
            partitions_dict_after = self.collect_partitions_info(ignore_limit_rows_number=ignore_limit_rows_number)
            if partitions_dict_after is not None:
                if not ignore_limit_rows_number and self.limit_rows_number:
                    missing_rows = {key: val for key, val in partitions_dict_after.items() if
                                    val < self.limit_rows_number}
                    assert not missing_rows, f"Found missing rows for partitions: {missing_rows}"
                else:
                    self.tester.assertEqual(self.partitions_dict_before,
                                            partitions_dict_after,
                                            msg='Row amount in partitions is not same before and after running of nemesis: '
                                                f' {partitions_dict_after}')


def get_table_clustering_order(ks_cf: str, ck_name: str, session) -> str:
    """
    Returns a clustering order of a table column.
    :param ck_name:
    :param session:
    :param ks_cf:
    :return: clustering-order string - ASC/DESC

    Example query: SELECT clustering_order from system_schema.columns WHERE keyspace_name = 'scylla_bench'
    and table_name = 'test' and column_name = 'ck'
    """
    keyspace, table = ks_cf.split('.')
    cmd = f"SELECT clustering_order from system_schema.columns WHERE keyspace_name = '{keyspace}' " \
          f"and table_name = '{table}' and column_name = '{ck_name}'"
    cql_result = session.execute(cmd)
    clustering_order = cql_result.current_rows[0].clustering_order
    LOGGER.info('Retrieved a clustering-order of: %s for table %s', clustering_order, ks_cf)
    return clustering_order


def get_partition_keys(ks_cf: str, session, pk_name: str = 'pk', limit: int = None) -> List[str]:
    """
    Return list of partitions from a requested table
    :param session:
    :param ks_cf:
    :param limit:
    :param pk_name:
    :return: A list of partition-keys from a requested table.
    """
    cmd = f'select distinct {pk_name} from {ks_cf}'
    if limit:
        cmd += f' limit {limit}'
    cql_result = session.execute(cmd)
    pks_list = [getattr(row, pk_name) for row in cql_result.current_rows]
    return pks_list
