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
from typing import List

LOGGER = logging.getLogger(__name__)


class PartitionsValidationAttributes:  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """
    A class that gathers all data related to partitions-validation.
    It helps Longevity tests that uses "validate_partitions" to
    save and compare a table partitions-rows-number during stress and nemesis.
    """
    PARTITIONS_ROWS_BEFORE = "partitions_rows_before"
    PARTITIONS_ROWS_AFTER = "partitions_rows_after"

    def __init__(self, table_name: str, primary_key_column: str, limit_rows_number: int = 0,  # pylint: disable=too-many-arguments
                 max_partitions_in_test_table: str | None = None,
                 partition_range_with_data_validation: str | None = None, validate_partitions: bool = False):
        """
        limit_rows_number is a limit for querying rows per partition.
        When running a health-check and calling "validate_partitions",
        it would nor read more than this number of rows-per-partition.
        The default is NO limit_rows_number, marked by '0'.
        """
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

    def get_count_pk_rows_query(self, key: str, ignore_limit_rows_number: bool = False) -> str:
        limit_query = f' LIMIT {self.limit_rows_number}' if not ignore_limit_rows_number and self.limit_rows_number else ''
        count_pk_rows_cmd = f'select count(*) from {self.table_name} where ' \
                            f'{self.primary_key_column} = {key}' \
                            f'{limit_query}' \
                            ' using timeout 5m'
        return count_pk_rows_cmd


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
