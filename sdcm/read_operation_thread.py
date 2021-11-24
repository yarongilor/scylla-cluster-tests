import logging
import random
import threading
import time
from abc import abstractmethod
from typing import Optional

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement  # pylint: disable=no-name-in-module

from sdcm.cluster import BaseNode, BaseScyllaCluster, BaseCluster, get_partition_keys
from sdcm.sct_events import Severity
from sdcm.sct_events.database import FullScanEvent, ReversedQueryEvent

ERROR_SUBSTRINGS = ("timed out", "unpack requires", "timeout")


# pylint: disable=too-many-instance-attributes
class ReadOperationThread:
    bypass_cache = ' bypass cache'
    basic_query = 'select * from {}'

    # pylint: disable=too-many-arguments
    def __init__(self, db_cluster: [BaseScyllaCluster, BaseCluster], ks_cf: str, duration: int, interval: int,
                 termination_event: threading.Event, page_size: int = 100000):
        self.ks_cf = ks_cf
        self.db_cluster = db_cluster
        self.page_size = page_size
        self.duration = duration
        self.interval = interval
        self.query_options = None
        self.db_node = None
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

    def run_read_operation(self, read_operation_event):  # pylint: disable=too-many-locals
        self.db_node = db_node = random.choice(self.db_cluster.nodes)
        if self.ks_cf == 'random':
            self.ks_cf = self.get_ks_cs(db_node)
        read_pages = random.choice([100, 1000, 0])
        with read_operation_event(node=db_node.name, ks_cf=self.ks_cf, message="") as operation_event:
            cmd = self.randomly_form_cql_statement()
            if not cmd:
                return
            with self.create_session(db_node) as session:

                if self.termination_event.is_set():
                    return

                try:
                    self.log.info('Will run command "%s"', cmd)
                    result = session.execute(SimpleStatement(
                        cmd,
                        fetch_size=self.page_size,
                        consistency_level=ConsistencyLevel.ONE))
                    pages = 0
                    while result.has_more_pages and pages <= read_pages:
                        result.fetch_next_page()
                        if read_pages > 0:
                            pages += 1
                    operation_event.message = f"{read_operation_event.__name__} operation ended successfully"
                except Exception as exc:  # pylint: disable=broad-except
                    msg = str(exc)
                    msg = f"{msg} while running Nemesis: {db_node.running_nemesis}" if db_node.running_nemesis else msg
                    operation_event.message = msg

                    if db_node.running_nemesis or any(s in msg.lower() for s in ERROR_SUBSTRINGS):
                        operation_event.severity = Severity.WARNING
                    else:
                        operation_event.severity = Severity.ERROR

    def run_for_a_duration(self, read_operation_event):
        end_time = time.time() + self.duration
        while time.time() < end_time and not self.termination_event.is_set():
            self.run_read_operation(read_operation_event=read_operation_event)
            time.sleep(self.interval)

    @abstractmethod
    def run(self):
        ...

    def start(self):
        self._thread.start()

    def join(self, timeout=None):
        return self._thread.join(timeout)


class FullScanThread(ReadOperationThread):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.query_options = [self.basic_query, self.basic_query + self.bypass_cache]

    def randomly_form_cql_statement(self) -> Optional[str]:
        cmd = random.choice(self.query_options).format(self.ks_cf)
        return self.randomly_add_timeout(cmd)

    def run(self):
        self.run_for_a_duration(read_operation_event=FullScanEvent)


class ReversedQueryThread(ReadOperationThread):
    reversed_query_suffix = ' where pk = {} order by ck desc'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        reversed_query = self.basic_query + self.reversed_query_suffix
        self.query_options = [reversed_query, reversed_query + self.bypass_cache]

    def randomly_form_cql_statement(self) -> Optional[str]:
        if pks := get_partition_keys(ks_cf=self.ks_cf, db_node=self.db_node, limit=10000):
            partition_key = random.choice(pks)
            cmd = random.choice(self.query_options).format(self.ks_cf, partition_key)
        else:
            self.log.info('No partition keys found for table: %s! A reversed query cannot be executed!', self.ks_cf)
            return None
        return self.randomly_add_timeout(cmd)

    def run(self):
        self.run_for_a_duration(read_operation_event=ReversedQueryEvent)
