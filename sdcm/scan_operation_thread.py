import json
import logging
import random
import threading
import time
from abc import abstractmethod
from typing import Optional
# from deepdiff import DeepDiff  # TODO: Uncomment when a new hydra docker with deepdiff is available
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement  # pylint: disable=no-name-in-module

from sdcm.cluster import BaseNode, BaseScyllaCluster, BaseCluster
# from sdcm.sct_events.system import InfoEvent  # TODO: Uncomment when a new hydra docker with deepdiff is available
from sdcm.utils.common import get_partition_keys
from sdcm.sct_events import Severity
from sdcm.sct_events.database import FullScanEvent, FullPartitionScanReversedOrderEvent

ERROR_SUBSTRINGS = ("timed out", "unpack requires", "timeout")


# pylint: disable=too-many-instance-attributes
class ScanOperationThread:
    bypass_cache = ' bypass cache'
    basic_query = 'select * from {}'

    # pylint: disable=too-many-arguments
    def __init__(self, db_cluster: [BaseScyllaCluster, BaseCluster], duration: int, interval: int,
                 termination_event: threading.Event, page_size: int = 100000, **kwargs):
        self.ks_cf = kwargs.get('ks_cf')
        self.db_cluster = db_cluster
        self.page_size = page_size
        self.duration = duration
        self.interval = interval
        self.query_options = None
        self.query_result_data = []
        self.db_node = None
        self.read_pages = 0
        self.termination_event = termination_event
        self.log = logging.getLogger(self.__class__.__name__)
        self._thread = threading.Thread(daemon=True, name=self.__class__.__name__, target=self.run)

    def get_ks_cs(self, db_node: BaseNode):
        ks_cf_list = self.db_cluster.get_non_system_ks_cf_list(db_node)
        if self.ks_cf not in ks_cf_list:
            self.ks_cf = 'random'

        if 'random' in self.ks_cf.lower():
            return random.choice(ks_cf_list)
        return self.ks_cf

    @staticmethod
    def randomly_add_timeout(cmd) -> str:
        if random.choice([True] * 2 + [False]):
            cql_timeout_seconds = str(random.choice([2, 4, 8, 30, 120, 300]))
            cql_timeout_param = f" USING TIMEOUT {cql_timeout_seconds}s"
            cmd += cql_timeout_param
        return cmd

    @abstractmethod
    def randomly_form_cql_statement(self) -> Optional[str]:
        ...

    def create_session(self, db_node: BaseNode):
        credentials = self.db_cluster.get_db_auth()
        username, password = credentials if credentials else (None, None)
        return self.db_cluster.cql_connection_patient(db_node, user=username, password=password)

    def execute_query(self, session, cmd: str):
        self.log.info('Will run command "%s"', cmd)
        return session.execute(SimpleStatement(
            cmd,
            fetch_size=self.page_size,
            consistency_level=ConsistencyLevel.ONE))

    def fetch_result_pages(self, result, read_pages):
        self.log.info('Will fetch up to %s result pages.."', read_pages)
        pages = 0
        while result.has_more_pages and pages <= read_pages:
            result.fetch_next_page()
            if read_pages > 0:
                pages += 1

    def run_scan_operation(self, scan_operation_event, cmd: str = None):  # pylint: disable=too-many-locals
        db_node = self.db_node
        if self.ks_cf == 'random':
            self.ks_cf = self.get_ks_cs(db_node)
        with scan_operation_event(node=db_node.name, ks_cf=self.ks_cf, message="") as operation_event:
            cmd = cmd or self.randomly_form_cql_statement()
            if not cmd:
                return
            with self.create_session(db_node) as session:

                if self.termination_event.is_set():
                    return

                try:
                    result = self.execute_query(session=session, cmd=cmd)
                    self.fetch_result_pages(result=result, read_pages=self.read_pages)
                    operation_event.message = f"{scan_operation_event.__name__} operation ended successfully"
                except Exception as exc:  # pylint: disable=broad-except
                    msg = str(exc)
                    msg = f"{msg} while running Nemesis: {db_node.running_nemesis}" if db_node.running_nemesis else msg
                    operation_event.message = msg

                    if db_node.running_nemesis or any(s in msg.lower() for s in ERROR_SUBSTRINGS):
                        operation_event.severity = Severity.WARNING
                    else:
                        operation_event.severity = Severity.ERROR

    def run_for_a_duration(self, scan_operation_event):
        end_time = time.time() + self.duration
        while time.time() < end_time and not self.termination_event.is_set():
            self.db_node = random.choice(self.db_cluster.nodes)
            self.read_pages = random.choice([100, 1000, 0])
            self.run_scan_operation(scan_operation_event=scan_operation_event)
            time.sleep(self.interval)

    @abstractmethod
    def run(self):
        ...

    def start(self):
        self._thread.start()

    def join(self, timeout=None):
        return self._thread.join(timeout)


class FullScanThread(ScanOperationThread):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.query_options = [self.basic_query, self.basic_query + self.bypass_cache]

    def randomly_form_cql_statement(self) -> Optional[str]:
        cmd = random.choice(self.query_options).format(self.ks_cf)
        return self.randomly_add_timeout(cmd)

    def run(self):
        self.run_for_a_duration(scan_operation_event=FullScanEvent)


class FullPartitionScanThread(ScanOperationThread):
    """
    Run a full scan of a partition, assuming it has a clustering key and multiple rows.
    It runs a reversed query of a partition, then optionally runs a normal partition scan in order
    to validate the reversed-query output data.

    Should support the following query options:  # TODO: finish implementing
    1) ck < ?
    2) ck > ?
    3) ck > ? and ck < ?
    4) order by ck desc
    5) limit <int>
    6) paging
    """
    reversed_query_filter_ck_by = {'lt': ' and {} < {}', 'gt': ' and {} > {}', 'lt_and_gt': ' and {} < {} and {} > {}'}
    reversed_query_order = ' order by ck desc'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.full_partition_scan_params = kwargs
        self.full_partition_scan_params['validate_data'] = json.loads(
            self.full_partition_scan_params.get('validate_data', 'false'))

    def randomly_form_cql_statement(self) -> Optional[tuple[str, str]]:  # pylint: disable=too-many-branches
        with self.create_session(self.db_node) as session:
            ck_name = self.full_partition_scan_params.get('ck_name', 'ck')
            rows_count = self.full_partition_scan_params.get('rows_count', 5000)
            ck_random_min_value = random.randint(a=1, b=rows_count)
            ck_random_max_value = random.randint(a=ck_random_min_value, b=rows_count)
            ck_filter = random.choice(list(self.reversed_query_filter_ck_by.keys()))
            pk_name = self.full_partition_scan_params.get('pk_name', 'pk')
            if pks := get_partition_keys(ks_cf=self.ks_cf, session=session, pk_name=pk_name, limit=10000):
                partition_key = random.choice(pks)
                # Form a random query out of all options, like:
                # select * from scylla_bench.test where pk = 1234 and ck < 4721 and ck > 2549 order by ck desc
                # limit 3467 bypass cache
                normal_query = reversed_query = self.basic_query.format(
                    self.ks_cf) + f' where {pk_name} = {partition_key}'
                query_suffix = limit = ''

                if random.choice([False] + [True]):  # Randomly add a LIMIT
                    limit = random.randint(a=rows_count/5, b=rows_count)
                    query_suffix += f' limit {limit}'
                if random.choice([False] + [True]):
                    query_suffix += self.bypass_cache
                if random.choice([False] + [True] * 3):  # Randomly add CK filtering

                    # example: rows-count = 20, ck > 10, ck < 15, limit = 3 ==> ck_range = [11..14] = 4
                    # ==> limit < ck_range
                    # reversed query is: select * from scylla_bench.test where pk = 1 and ck > 10
                    # order by ck desc limit 5
                    # normal query should be: select * from scylla_bench.test where pk = 1 and ck > 15 limit 5
                    if ck_filter == 'lt_and_gt':
                        # Example: select * from scylla_bench.test where pk = 1 and ck > 10 and ck < 15 order by ck desc
                        reversed_query += self.reversed_query_filter_ck_by[ck_filter].format(ck_name,
                                                                                             ck_random_max_value,
                                                                                             ck_name,
                                                                                             ck_random_min_value)
                        ck_range = ck_random_max_value - ck_random_min_value - 1  # e.g. 15 - 10 - 1 = 4
                        if limit and limit < ck_range:
                            # Example: select * from scylla_bench.test where pk = 1 and ck > 10 and ck < 15
                            # order by ck desc limit 3
                            # output of ck should be: [12,13,14]
                            normal_query += \
                                f' and {ck_name} < {ck_random_max_value} and {ck_name} >= {ck_random_max_value - limit}'
                        else:
                            normal_query = reversed_query
                    else:
                        reversed_query += self.reversed_query_filter_ck_by[ck_filter].format(ck_name,
                                                                                             ck_random_min_value)

                        # example: rows-count = 20, ck > 10, limit = 5 ==> ck_range = 20 - 10 = 10 ==> limit < ck_range
                        # reversed query is: select * from scylla_bench.test where pk = 1 and ck > 10
                        # order by ck desc limit 5
                        # normal query should be: select * from scylla_bench.test where pk = 1 and ck > 15 limit 5
                        if ck_filter == 'gt':
                            ck_range = rows_count - ck_random_min_value
                            if limit and limit < ck_range:
                                normal_query += self.reversed_query_filter_ck_by[ck_filter].format(ck_name,
                                                                                                   rows_count - limit)
                            else:
                                normal_query = reversed_query

                        # example: rows-count = 20, ck < 10, limit = 5 ==> limit < ck_random_min_value (ck_range)
                        # reversed query is: select * from scylla_bench.test where pk = 1 and ck < 10
                        # order by ck desc limit 5
                        # normal query should be: select * from scylla_bench.test where pk = 1 and ck >= 5 limit 5
                        if ck_filter == 'lt':
                            if limit and limit < ck_random_min_value:
                                normal_query += f' and {ck_name} >= {ck_random_min_value - limit}'
                            else:
                                normal_query = reversed_query

                reversed_query += f' order by {ck_name} desc'

                normal_query += query_suffix
                reversed_query += query_suffix
                self.log.info('Randomly formed normal query is: %s', normal_query)
                self.log.info('Randomly formed reversed query is: %s', reversed_query)
            else:
                self.log.info('No partition keys found for table: %s! A reversed query cannot be executed!', self.ks_cf)
                return None
        return normal_query, self.randomly_add_timeout(reversed_query)

    def fetch_result_pages(self, result, read_pages):
        self.log.info('Will fetch up to %s result pages.."', read_pages)
        self.query_result_data = []
        handler = PagedResultHandler(future=result, scan_operation_thread=self)
        handler.finished_event.wait()
        if handler.error:
            self.log.warning("Got a Page Handler error: %s", handler.error)
            raise handler.error

    def execute_query(self, session, cmd: str):
        self.log.info('Will run command "%s"', cmd)
        session.default_fetch_size = self.page_size
        session.default_consistency_level = ConsistencyLevel.ONE
        return session.execute_async(cmd)

    def run_scan_operation(self, scan_operation_event, cmd: str = None):  # pylint: disable=too-many-locals
        queries = self.randomly_form_cql_statement()
        if not queries:
            return
        normal_query, reversed_query = queries
        ScanOperationThread.run_scan_operation(self, scan_operation_event=scan_operation_event, cmd=reversed_query)
        if self.full_partition_scan_params.get('validate_data'):
            # TODO: remove and uncomment when a new hydra docker with deepdiff is available
            self.log.debug('Temporarily not executing the normal query of: %s', normal_query)
            # reversed_query_result = self.query_result_data
            # ScanOperationThread.run_scan_operation(self, scan_operation_event=scan_operation_event, cmd=normal_query)
            # diff = DeepDiff(t1=reversed_query_result, t2=self.query_result_data, ignore_order=True,
            #                 ignore_numeric_type_changes=True)
            # if diff:
            #     InfoEvent(
            #         message=f'Found the following differences between a reversed and normal queries: {diff}').publish()

    def run(self):
        self.run_for_a_duration(scan_operation_event=FullPartitionScanReversedOrderEvent)


class PagedResultHandler:

    def __init__(self, future, scan_operation_thread: FullPartitionScanThread):
        self.error = None
        self.finished_event = threading.Event()
        self.future = future
        self.max_read_pages = scan_operation_thread.read_pages
        self.current_read_pages = 0
        self.log = logging.getLogger(self.__class__.__name__)
        self.scan_operation_thread = scan_operation_thread
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error)

    def handle_page(self, rows):
        self.scan_operation_thread.query_result_data += rows
        if self.future.has_more_pages and self.current_read_pages <= self.max_read_pages:
            self.log.info('Will fetch the next page: %s', self.current_read_pages)
            self.future.start_fetching_next_page()
            if self.max_read_pages > 0:
                self.current_read_pages += 1
        else:
            self.finished_event.set()

    def handle_error(self, exc):
        self.error = exc
        self.finished_event.set()
