# coding: utf-8
# -*- coding: utf-8 -*-
import os

from enum import Enum
from textwrap import dedent

from invoke.exceptions import UnexpectedExit, Failure

import sys
reload(sys)
sys.setdefaultencoding('utf8')

import requests
import logging
import json
import time
import datetime

from sdcm import wait
from sdcm.utils import Distro

logger = logging.getLogger(__name__)

STATUS_DONE = 'done'
STATUS_ERROR = 'error'
MANAGER_IDENTITY_FILE_DIR = '/root/.ssh'
MANAGER_IDENTITY_FILE_NAME = 'scylla-manager.pem'
MANAGER_IDENTITY_FILE = os.path.join(MANAGER_IDENTITY_FILE_DIR,MANAGER_IDENTITY_FILE_NAME)
SSL_CONF_DIR = '/tmp/ssl_conf'
SSL_USER_CERT_FILE = SSL_CONF_DIR + '/db.crt'
SSL_USER_KEY_FILE = SSL_CONF_DIR + '/db.key'


class ScyllaManagerError(Exception):
    """
    A custom exception for Manager related errors
    """
    pass


class HostSsl(Enum):
    ON = "ON"
    OFF = "OFF"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            return getattr(cls, output_str)
        except AttributeError:
            raise ScyllaManagerError("Could not recognize returned task status: {}".format(output_str))

class HostStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            return getattr(cls, output_str)
        except AttributeError:
            raise ScyllaManagerError("Could not recognize returned task status: {}".format(output_str))


class HostRestStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            return getattr(cls, output_str)
        except AttributeError:
            raise ScyllaManagerError("Could not recognize returned task status: {}".format(output_str))


class TaskStatus(Enum):
    NEW = "NEW"
    RUNNING = "RUNNING"
    DONE = "DONE"
    UNKNOWN = "UNKNOWN"
    ERROR = "ERROR"
    STOPPED = "STOPPED"
    STARTING = "STARTING"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            return getattr(cls, output_str)
        except AttributeError:
            raise ScyllaManagerError("Could not recognize returned task status: {}".format(output_str))


class ScyllaManagerBase(object):

    def __init__(self, id, manager_node):
        self.id = id
        self.manager_node = manager_node
        self.sctool = SCTool(manager_node=manager_node)

    def get_property(self, parsed_table, column_name):
        return self.sctool.get_table_value(parsed_table=parsed_table, column_name=column_name, identifier=self.id)


class ManagerTask(ScyllaManagerBase):

    def __init__(self, task_id, cluster_id, manager_node):
        ScyllaManagerBase.__init__(self, id=task_id, manager_node=manager_node)
        self.cluster_id = cluster_id

    def stop(self):
        cmd = "task stop {} -c {}".format(self.id, self.cluster_id)
        res = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return self.wait_and_get_final_status(timeout=30, step=3)

    def start(self, cmd=None):
        cmd = cmd or "task start {} -c {}".format(self.id, self.cluster_id)
        res = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        list_all_task_status = [s for s in TaskStatus.__dict__ if not s.startswith("__")]
        list_expected_task_status = [status for status in list_all_task_status if status != TaskStatus.STOPPED]
        return self.wait_for_status(list_status=list_expected_task_status, timeout=30, step=3)

    def _add_kwargs_to_cmd(self, cmd, **kwargs):
        for k, v in kwargs.items():
            cmd += ' --{}={}'.format(k, v)
        return cmd

    @property
    def history(self):
        """
        Gets the task's history table
        """
        # ╭──────────────────────────────────────┬────────────────────────┬────────────────────────┬──────────┬───────╮
        # │ id                                   │ start time             │ end time               │ duration │ status│                                                                                                                                                                  │
        # ├──────────────────────────────────────┼────────────────────────┼────────────────────────┼──────────┼───────┤
        # │ e4f70414-ebe7-11e8-82c4-12c0dad619c2 │ 19 Nov 18 10:43:04 UTC │ 19 Nov 18 10:43:04 UTC │ 0s       │ NEW   │
        # │ 7f564891-ebe6-11e8-82c3-12c0dad619c2 │ 19 Nov 18 10:33:04 UTC │ 19 Nov 18 10:33:04 UTC │ 0s       │ NEW   │
        # │ 19b58cb3-ebe5-11e8-82c2-12c0dad619c2 │ 19 Nov 18 10:23:04 UTC │ 19 Nov 18 10:23:04 UTC │ 0s       │ NEW   │
        # │ b414cde5-ebe3-11e8-82c1-12c0dad619c2 │ 19 Nov 18 10:13:04 UTC │ 19 Nov 18 10:13:04 UTC │ 0s       │ NEW   │
        # │ 4e741c3d-ebe2-11e8-82c0-12c0dad619c2 │ 19 Nov 18 10:03:04 UTC │ 19 Nov 18 10:03:04 UTC │ 0s       │ NEW   │
        # ╰──────────────────────────────────────┴────────────────────────┴────────────────────────┴──────────┴───────╯
        cmd = "task history {} -c {}".format(self.id, self.cluster_id)
        res = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return res  # or can be specified like: self.get_property(parsed_table=res, column_name='status')

    @property
    def next_run(self):
        """
        Gets the task's next run value
        """
        # ╭──────────────────────────────────────────────────┬───────────────────────────────┬──────┬────────────┬────────│
        # │ task                                             │ next run                      │ ret. │ properties │ status │
        # ├──────────────────────────────────────────────────┼───────────────────────────────┼──────┼────────────┼────────│
        # │ healthcheck/7fb6f1a7-aafc-4950-90eb-dc64729e8ecb │ 18 Nov 18 20:32:08 UTC (+15s) │ 0    │            │ NEW    │
        # │ repair/22b68423-4332-443d-b8b4-713005ea6049      │ 19 Nov 18 00:00:00 UTC (+7d)  │ 3    │            │ NEW    │
        # ╰──────────────────────────────────────────────────┴───────────────────────────────┴──────┴────────────┴────────╯
        cmd = "task list -c {}".format(self.cluster_id)
        res = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return self.get_property(parsed_table=res, column_name='next run')

    @property
    def status(self):
        """
        Gets the task's status
        """
        cmd = "task list -c {}".format(self.cluster_id)
        res = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        str_status = self.get_property(parsed_table=res, column_name='status')
        return TaskStatus.from_str(str_status)

        # expecting output of:
        # ╭─────────────────────────────────────────────┬───────────────────────────────┬──────┬────────────┬────────╮
        # │ task                                        │ next run                      │ ret. │ properties │ status │
        # ├─────────────────────────────────────────────┼───────────────────────────────┼──────┼────────────┼────────┤
        # │ repair/2a4125d6-5d5a-45b9-9d8d-dec038b3732d │ 05 Nov 18 00:00 UTC (+7 days) │ 3    │            │ DONE   │
        # │ repair/dd98f6ae-bcf4-4c98-8949-573d533bb789 │                               │ 3    │            │ DONE   │
        # ╰─────────────────────────────────────────────┴───────────────────────────────┴──────┴────────────┴────────╯

    @property
    def progress(self):
        """
        Gets the repair task's progress
        """
        if self.status in [TaskStatus.NEW, TaskStatus.STARTING]:
            return " 0%"
        cmd = "task progress {} -c {}".format(self.id, self.cluster_id)
        res = self.sctool.run(cmd=cmd)
        # expecting output of:
        #  Status:           RUNNING
        #  Start time:       26 Mar 19 19:40:21 UTC
        #  Duration: 6s
        #  Progress: 0.12%
        #  Datacenters:
        #    - us-eastscylla_node_east
        #  ╭────────────────────┬───────╮
        #  │ system_auth        │ 0.47% │
        #  │ system_distributed │ 0.00% │
        #  │ system_traces      │ 0.00% │
        #  │ keyspace1          │ 0.00% │
        #  ╰────────────────────┴───────╯
        # [['Status: RUNNING'], ['Start time: 26 Mar 19 19:40:21 UTC'], ['Duration: 6s'], ['Progress: 0.12%'], ... ]
        progress = "N/A"
        for task_property in res:
            if task_property[0].startswith("Progress"):
                progress = task_property[0].split(':')[1]
                break
        return progress

    def is_status_in_list(self, list_status, check_task_progress=False):
        """
        Check if the status of a given task is in list
        :param list_status:
        :return:
        """
        status = self.status
        if check_task_progress and status not in [TaskStatus.NEW,
                                                  TaskStatus.STARTING]:  # check progress for all statuses except 'NEW' / 'STARTING'
            ###
            # The reasons for the below (un-used) assignment are:
            # * check that progress command works on varios task statuses (that was how manager bug #856 found).
            # * print the progress to log in cases needed for failures/performance analysis.
            ###
            progress = self.progress
        return self.status in list_status

    def wait_for_status(self, list_status, check_task_progress=True, timeout=3600, step=120):
        text = "Waiting until task: {} reaches status of: {}".format(self.id, list_status)
        is_status_reached = wait.wait_for(func=self.is_status_in_list, step=step,
                                          text=text, list_status=list_status, check_task_progress=check_task_progress,
                                          timeout=timeout)
        return is_status_reached

    def wait_and_get_final_status(self, timeout=3600, step=120):
        """
        1) Wait for task to reach a 'final' status. meaning one of: done/error/stopped
        2) return the final status.
        :return:
        """
        list_final_status = [TaskStatus.ERROR, TaskStatus.STOPPED, TaskStatus.DONE]
        logger.debug("Waiting for task: {} getting to a final status ({})..".format(self.id, [str(s) for s in
                                                                                              list_final_status]))
        res = self.wait_for_status(list_status=list_final_status, timeout=timeout, step=step)
        if not res:
            raise ScyllaManagerError("Unexpected result on waiting for task {} status".format(self.id))
        return self.status


class RepairTask(ManagerTask):
    def __init__(self, task_id, cluster_id, manager_node):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, manager_node=manager_node)

    def start(self, use_continue=False, **kwargs):
        str_continue = '--continue=true' if use_continue else '--continue=false'
        cmd = "task start {} -c {} {}".format(self.id, self.cluster_id, str_continue)
        ManagerTask.start(self, cmd=cmd)

    def continue_repair(self):
        self.start(use_continue=True)


class HealthcheckTask(ManagerTask):
    def __init__(self, task_id, cluster_id, manager_node):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, manager_node=manager_node)


class ManagerCluster(ScyllaManagerBase):

    def __init__(self, manager_node, cluster_id, client_encrypt=False):
        if not manager_node:
            raise ScyllaManagerError("Cannot create a Manager Cluster where no 'manager tool' parameter is given")
        ScyllaManagerBase.__init__(self, id=cluster_id, manager_node=manager_node)
        self.client_encrypt = client_encrypt

    def create_repair_task(self):
        cmd = "repair -c {}".format(self.id)
        res = self.sctool.run(cmd=cmd, parse_table_res=False)
        if not res:
            raise ScyllaManagerError("Unknown failure for sctool {} command".format(cmd))

        if "no matching units found" in res.stderr:
            raise ScyllaManagerError("Manager cannot run repair where no keyspace exists.")

        # expected result output is to have a format of: "repair/2a4125d6-5d5a-45b9-9d8d-dec038b3732d"
        if 'repair' not in res.stdout:
            logger.error("Encountered an error on '{}' command response".format(cmd))
            raise ScyllaManagerError(res.stderr)

        task_id = res.stdout.split('\n')[0]
        logger.debug("Created task id is: {}".format(task_id))

        return RepairTask(task_id=task_id, cluster_id=self.id,
                          manager_node=self.manager_node)  # return the manager's object with new repair-task-id

    def delete(self):
        """
        $ sctool cluster delete
        """

        cmd = "cluster delete -c {}".format(self.id)
        res = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    def update(self, name=None, host=None, ssh_identity_file=None, ssh_user=None, client_encrypt=None):
        """
        $ sctool cluster update --help
        Modify a cluster

        Usage:
          sctool cluster update [flags]

        Flags:
          -h, --help                     help for update
              --host string              hostname or IP of one of the cluster nodes
          -n, --name alias               alias you can give to your cluster
              --ssh-identity-file path   path to identity file containing SSH private key
              --ssh-user name            SSH user name used to connect to the cluster nodes
        """
        cmd = "cluster update -c {}".format(self.id)
        if name:
            cmd += " --name={}".format(name)
        if host:
            cmd += " --host={}".format(host)
        if ssh_identity_file:
            cmd += " --ssh-identity-file={}".format(ssh_identity_file)
        if ssh_user:
            cmd += " --ssh-user={}".format(ssh_user)
        if client_encrypt:
            cmd += " --ssl-user-cert-file {} --ssl-user-key-file {}".format(SSL_USER_CERT_FILE, SSL_USER_KEY_FILE)
        res = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    @property
    def _cluster_list(self):
        """
        Gets the Manager's Cluster list
        """
        cmd = "cluster list"
        return self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    @property
    def name(self):
        """
        Gets the Cluster name as represented in Manager
        """
        # expecting output of:
        # ╭──────────────────────────────────────┬──────┬─────────────┬────────────────╮
        # │ cluster id                           │ name │ host        │ ssh user       │
        # ├──────────────────────────────────────┼──────┼─────────────┼────────────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ sce2 │ 10.142.0.25 │ scylla-manager │
        # ╰──────────────────────────────────────┴──────┴─────────────┴────────────────╯
        return self.get_property(parsed_table=self._cluster_list, column_name='name')

    @property
    def ssh_user(self):
        """
        Gets the Cluster ssh_user as represented in Manager
        """
        # expecting output of:
        # ╭──────────────────────────────────────┬──────┬─────────────┬────────────────╮
        # │ cluster id                           │ name │ host        │ ssh user       │
        # ├──────────────────────────────────────┼──────┼─────────────┼────────────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ sce2 │ 10.142.0.25 │ scylla-manager │
        # ╰──────────────────────────────────────┴──────┴─────────────┴────────────────╯
        return self.get_property(parsed_table=self._cluster_list, column_name='ssh user')

    def _get_task_list(self):
        cmd = "task list -c {}".format(self.id)
        return self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    @property
    def repair_task_list(self):
        """
        Gets the Cluster's  Task list
        """
        # ╭─────────────────────────────────────────────┬───────────────────────────────┬──────┬────────────┬────────╮
        # │ task                                        │ next run                      │ ret. │ properties │ status │
        # ├─────────────────────────────────────────────┼───────────────────────────────┼──────┼────────────┼────────┤
        # │ repair/2a4125d6-5d5a-45b9-9d8d-dec038b3732d │ 26 Nov 18 00:00 UTC (+7 days) │ 3    │            │ DONE   │
        # │ repair/dd98f6ae-bcf4-4c98-8949-573d533bb789 │                               │ 3    │            │ DONE   │
        # ╰─────────────────────────────────────────────┴───────────────────────────────┴──────┴────────────┴────────╯
        repair_task_list = []
        table_res = self._get_task_list()
        if len(table_res) > 1:  # if there are any tasks in list - add them as RepairTask generated objects.
            repair_task_rows_list = [row for row in table_res[1:] if row[0].startswith("repair/")]
            for row in repair_task_rows_list:
                repair_task_list.append(RepairTask(task_id=row[0], cluster_id=self.id, manager_node=self.manager_node))
        return repair_task_list

    def get_healthcheck_task(self):
        healthcheck_id = self.sctool.get_table_value(parsed_table=self._get_task_list(), column_name="task",
                                                     identifier="healthcheck/", is_search_substring=True)
        return HealthcheckTask(task_id=healthcheck_id, cluster_id=self.id,
                               manager_node=self.manager_node)  # return the manager's health-check-task object with the found id

    def get_hosts_health(self):
        """
        Gets the Manager's Cluster Nodes status
        """
        # $ sctool status -c bla
        # 19:43:56 [107.23.100.82] [stdout] Datacenter: us-eastscylla_node_east
        # 19:43:56 [107.23.100.82] [stdout] ╭──────────┬─────┬──────────┬────────────────╮
        # 19:43:56 [107.23.100.82] [stdout] │ CQL      │ SSL │ REST     │ Host           │
        # 19:43:56 [107.23.100.82] [stdout] ├──────────┼─────┼──────────┼────────────────┤
        # 19:43:56 [107.23.100.82] [stdout] │ UP (0ms) │ OFF │ UP (0ms) │ 34.205.64.58   │
        # 19:43:56 [107.23.100.82] [stdout] │ UP (0ms) │ OFF │ UP (0ms) │ 54.159.184.253 │
        # 19:43:56 [107.23.100.82] [stdout] ╰──────────┴─────┴──────────┴────────────────╯
        # 19:43:56 [107.23.100.82] [stdout] Datacenter: us-west-2scylla_node_west
        # 19:43:56 [107.23.100.82] [stdout] ╭────────────┬─────┬───────────┬──────────────╮
        # 19:43:56 [107.23.100.82] [stdout] │ CQL        │ SSL │ REST      │ Host         │
        # 19:43:56 [107.23.100.82] [stdout] ├────────────┼─────┼───────────┼──────────────┤
        # 19:43:56 [107.23.100.82] [stdout] │ UP (151ms) │ OFF │ UP (80ms) │ 34.219.6.187 │
        # 19:43:56 [107.23.100.82] [stdout] ╰────────────┴─────┴───────────┴──────────────╯
        cmd = "status -c {}".format(self.id)
        dict_status_tables = self.sctool.run(cmd=cmd, is_verify_errorless_result=True, is_multiple_tables=True)

        dict_hosts_health = {}
        for dc_name, hosts_table in dict_status_tables.items():
            if len(hosts_table) < 2:
                logger.debug("Cluster: {} - {} has no hosts health report".format(self.id, dc_name))
            else:
                list_titles_row = hosts_table[0]
                host_col_idx = list_titles_row.index("Host")
                cql_status_col_idx = list_titles_row.index("CQL")
                ssl_col_idx = list_titles_row.index("SSL")
                rest_col_idx = list_titles_row.index("REST")

                for line in hosts_table[1:]:
                    host = line[host_col_idx]
                    list_cql = line[cql_status_col_idx].split()
                    status = list_cql[0]
                    rtt = list_cql[1].strip("()") if len(list_cql) == 2 else "N/A"

                    list_rest = line[rest_col_idx].split()
                    rest_status = list_rest[0]
                    rest_rtt = list_rest[1].strip("()") if len(list_rest) == 2 else "N/A"

                    ssl = line[ssl_col_idx]
                    dict_hosts_health[host] = self._HostHealth(status=HostStatus.from_str(status), rtt=rtt, rest_status=HostRestStatus.from_str(rest_status), rest_rtt=rest_rtt, ssl=HostSsl.from_str(ssl))
            logger.debug("Cluster {} Hosts Health is:".format(self.id))
            for ip, health in dict_hosts_health.items():
                logger.debug("{}: {},{},{}".format(ip, health.status, health.rtt, health.rest_status, health.rest_rtt, health.ssl))
        return dict_hosts_health

    class _HostHealth():
        def __init__(self, status, rtt, ssl, rest_status, rest_rtt):
            self.status = status
            self.rtt = rtt
            self.rest_status = rest_status
            self.rest_rtt = rest_rtt
            self.ssl = ssl


class MgrUtils(object):

    @staticmethod
    def verify_errorless_result(cmd, res):
        if not res or res.stderr:
            logger.error("Encountered an error on '{}' command response: {}".format(cmd, str(res)))
            raise ScyllaManagerError("Encountered an error on '{}' command response".format(cmd))


def get_scylla_manager_tool(manager_node):
    if manager_node.is_rhel_like():
        return ScyllaManagerToolRedhatLike(manager_node=manager_node)
    else:
        return ScyllaManagerToolNonRedhat(manager_node=manager_node)


class ScyllaManagerTool(ScyllaManagerBase):
    """
    Provides communication with scylla-manager, operating sctool commands and ssh-scripts.
    """

    def __init__(self, manager_node):
        ScyllaManagerBase.__init__(self, id="MANAGER", manager_node=manager_node)
        # self._manager_node = manager_node
        sleep = 30
        logger.debug('Sleep {} seconds, waiting for manager service ready to respond'.format(sleep))
        time.sleep(sleep)
        logger.debug("Initiating Scylla-Manager, version: {}".format(self.version))
        dict_distro_user = {Distro.CENTOS7: 'centos', Distro.DEBIAN8: 'admin', Distro.UBUNTU16: 'ubuntu'}
        try:
            self.user = dict_distro_user[manager_node.distro]
        except KeyError as e:
            raise ScyllaManagerError("Non-supported Distro found on Monitoring Node: {} - {}".format(manager_node.distro, e))

    @property
    def version(self):
        cmd = "version"
        return self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    @property
    def cluster_list(self):
        """
        Gets the Manager's Cluster list
        """
        cmd = "cluster list"
        return self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    def get_cluster(self, cluster_name):
        """
        Returns Manager Cluster object by a given name if exist, else returns none.
        """
        # ╭──────────────────────────────────────┬──────────┬─────────────┬────────────────╮
        # │ cluster id                           │ name     │ host        │ ssh user       │
        # ├──────────────────────────────────────┼──────────┼─────────────┼────────────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ Dev_Test │ 10.142.0.25 │ scylla-manager │
        # │ bf6571ef-21d9-4cf1-9f67-9d05bc07b32e │ Prod     │ 10.142.0.26 │ scylla-manager │
        # ╰──────────────────────────────────────┴──────────┴─────────────┴────────────────╯
        try:
            cluster_id = self.sctool.get_table_value(parsed_table=self.cluster_list, column_name="cluster id",
                                                     identifier=cluster_name)
        except ScyllaManagerError as e:
            logger.warning("Cluster name not found in Scylla-Manager: {}".format(e))
            return None

        return ManagerCluster(manager_node=self.manager_node, cluster_id=cluster_id)

    def scylla_mgr_ssh_setup(self, node_ip, user='centos', identity_file='/tmp/scylla-test',
                             create_user=None):
        """
        scyllamgr_ssh_setup [--ssh-user <username>] [--ssh-identity-file <path to private key>] [--ssh-config-file <path to SSH config file>] [--create-user <username>] [--single-node] [--debug] SCYLLA_NODE_IP
           -u --ssh-user <username>        username used to connect to Scylla nodes, must be a sudo enabled user
           -i --ssh-identity-file <file>        path to SSH identity file (private key) for user
           -c --ssh-config-file <file>        path to alternate SSH configuration file, see man ssh_config
              --create-user <username>        username that will be created on Scylla nodes, default scylla-manager
              --single-node            setup the given node only, skip discovery of all the cluster nodes
              --debug                display debug info

        sudo scyllamgr_ssh_setup -u centos -i /tmp/scylla-qa-ec2 192.168.100.11
        """
        cmd = 'sudo scyllamgr_ssh_setup'
        if create_user:
            cmd += " --create-user {}".format(create_user)
        # create-user
        cmd += ' -u {} -i {} {}'.format(
            user, identity_file, node_ip)

        logger.debug("SSH setup command is: {}".format(cmd))
        res = self.manager_node.remoter.run(cmd)
        MgrUtils.verify_errorless_result(cmd=cmd, res=res)

    def _get_cluster_hosts_ip(self, db_cluster):
        return [node_data[1] for node_data in self._get_cluster_hosts_with_ips(db_cluster=db_cluster)]

    def _get_cluster_hosts_with_ips(self, db_cluster):
        ip_addr_attr = 'public_ip_address'
        return [[n, getattr(n, ip_addr_attr)] for n in db_cluster.nodes]

    def add_cluster(self, name, host=None, db_cluster=None, client_encrypt=None, user=None, create_user=None):
        """
        :param name: cluster name
        :param host: cluster node IP
        :param db_cluster: scylla cluster
        :param client_encrypt: is TSL client encryption enable/disable
        :return: ManagerCluster

        Add a cluster to manager

        Usage:
          sctool cluster add [flags]

        Flags:
          -h, --help                      help for add
              --host string               hostname or IP of one of the cluster nodes
          -n, --name alias                alias you can give to your cluster
              --ssh-identity-file path    path to identity file containing SSH private key
              --ssh-user name             SSH user name used to connect to the cluster nodes
              --ssl-user-cert-file path   path to client certificate when using client/server encryption with require_client_auth enabled
              --ssl-user-key-file path    path to key associated with ssl-user-cert-file

        Global Flags:
              --api-url URL    URL of Scylla Manager server (default "https://127.0.0.1:56443/api/v1")
          -c, --cluster name   target cluster name or ID

        Scylla Docs:
          https://docs.scylladb.com/operating-scylla/manager/1.4/add-a-cluster/
          https://docs.scylladb.com/operating-scylla/manager/1.4/sctool/#cluster-add


        """
        if not any([host, db_cluster]):
            raise ScyllaManagerError("Neither host or db_cluster parameter were given to Manager add_cluster")
        host = host or self._get_cluster_hosts_ip(db_cluster=db_cluster)[0]
        user = user or self.user
        logger.debug("Configuring ssh setup for cluster using {} node before adding the cluster: {}".format(host, name))
        self.scylla_mgr_ssh_setup(node_ip=host, user=user, create_user=create_user)
        ssh_user = create_user or 'scylla-manager'
        manager_identity_file = MANAGER_IDENTITY_FILE
        cmd = 'cluster add --host={} --ssh-identity-file={} --ssh-user={} --name={}'.format(host, manager_identity_file,
                                                                                            ssh_user, name)
        # Adding client-encryption parameters if required
        if client_encrypt != False:
            if not db_cluster:
                logger.warning("db_cluster is not given. Scylla-Manager connection to cluster may fail since not using client-encryption parameters.")
            else:  # check if scylla-node has client-encrypt
                db_node, _ip = self._get_cluster_hosts_with_ips(db_cluster=db_cluster)[0]
                if client_encrypt or db_node.is_client_encrypt:
                    cmd += " --ssl-user-cert-file {} --ssl-user-key-file {}".format(SSL_USER_CERT_FILE,
                                                                                    SSL_USER_KEY_FILE)
        res = self.sctool.run(cmd, parse_table_res=False)
        if not res or 'Cluster added' not in res.stderr:
            raise ScyllaManagerError("Encountered an error on 'sctool cluster add' command response: {}".format(res))
        cluster_id = res.stdout.split('\n')[0]  # return ManagerCluster instance with the manager's new cluster-id
        return ManagerCluster(manager_node=self.manager_node, cluster_id=cluster_id, client_encrypt=client_encrypt)

    def upgrade(self, scylla_mgmt_upgrade_to_repo):
        manager_from_version = self.version
        logger.debug('Running Manager upgrade from: {} to version in repo: {}'.format(manager_from_version, scylla_mgmt_upgrade_to_repo))
        self.manager_node.upgrade_mgmt(scylla_mgmt_repo=scylla_mgmt_upgrade_to_repo)
        new_manager_version = self.version
        logger.debug('The Manager version after upgrade is: {}'.format(new_manager_version))
        return new_manager_version

    def rollback_upgrade(self, manager_node):
        raise NotImplementedError


class ScyllaManagerToolRedhatLike(ScyllaManagerTool):

    def __init__(self, manager_node):
        ScyllaManagerTool.__init__(self, manager_node=manager_node)
        self.manager_repo_path = '/etc/yum.repos.d/scylla-manager.repo'


    def rollback_upgrade(self, scylla_mgmt_repo):

        manager_from_version = self.version
        remove_post_upgrade_repo = dedent("""
                        sudo systemctl stop scylla-manager
                        cqlsh -e 'DROP KEYSPACE scylla_manager'
                        sudo rm -rf {}
                        sudo yum clean all
                        sudo rm -rf /var/cache/yum
                    """.format(self.manager_repo_path))
        self.manager_node.remoter.run('sudo bash -cxe "%s"' % remove_post_upgrade_repo)

        # Downgrade to pre-upgrade scylla-manager repository
        self.manager_node.download_scylla_manager_repo(scylla_mgmt_repo)

        downgrade_to_pre_upgrade_repo = dedent("""
                                sudo yum downgrade scylla-manager* -y
                                sleep 2
                                sudo systemctl daemon-reload
                                sudo systemctl restart scylla-manager
                                sleep 3
                            """)
        self.manager_node.remoter.run('sudo bash -cxe "%s"' % downgrade_to_pre_upgrade_repo)

        # Rollback the Scylla Manager database???


class ScyllaManagerToolNonRedhat(ScyllaManagerTool):
    def __init__(self, manager_node):
        ScyllaManagerTool.__init__(self, manager_node=manager_node)
        self.manager_repo_path = '/etc/apt/sources.list.d/scylla-manager.list'

    def rollback_upgrade(self, scylla_mgmt_repo):
        manager_from_version = self.version[0]
        remove_post_upgrade_repo = dedent("""
                        cqlsh -e 'DROP KEYSPACE scylla_manager'
                        sudo systemctl stop scylla-manager
                        sudo systemctl stop scylla-server.service
                        sudo apt-get remove scylla-manager -y
                        sudo apt-get remove scylla-manager-server -y
                        sudo apt-get remove scylla-manager-client -y
                        sudo rm -rf {}
                        sudo apt-get clean
                    """.format(self.manager_repo_path)) # +" /var/lib/scylla-manager/*"))
        self.manager_node.remoter.run('sudo bash -cxe "%s"' % remove_post_upgrade_repo)
        self.manager_node.remoter.run('sudo apt-get update', ignore_status=True)

        # Downgrade to pre-upgrade scylla-manager repository
        self.manager_node.download_scylla_manager_repo(scylla_mgmt_repo)
        res = self.manager_node.remoter.run('sudo apt-get update', ignore_status=True)
        res = self.manager_node.remoter.run('apt-cache  show scylla-manager-client | grep Version:')
        rollback_to_version = res.stdout.split()[1]
        logger.debug("Rolling back manager version from: {} to: {}".format(manager_from_version, rollback_to_version))
        # self.manager_node.install_mgmt(scylla_mgmt_repo=scylla_mgmt_repo)
        downgrade_to_pre_upgrade_repo = dedent("""

                                sudo apt-get install scylla-manager -y
                                sudo systemctl unmask scylla-manager.service
                                sudo systemctl enable scylla-manager
                                echo yes| sudo scyllamgr_setup
                                sudo systemctl restart scylla-manager.service
                                sleep 25
                            """)
        self.manager_node.remoter.run('sudo bash -cxe "%s"' % downgrade_to_pre_upgrade_repo)

        # Rollback the Scylla Manager database???


class SCTool(object):

    def __init__(self, manager_node):
        self.manager_node = manager_node

    def _remoter_run(self, cmd):
        return self.manager_node.remoter.run(cmd)

    def run(self, cmd, is_verify_errorless_result=False, parse_table_res=True, is_multiple_tables=False):
        logger.debug("Issuing: 'sctool {}'".format(cmd))
        try:
            res = self._remoter_run(cmd='sudo sctool {}'.format(cmd))
        except (UnexpectedExit, Failure) as e:
            raise ScyllaManagerError("Encountered an error on sctool command: {}: {}".format(cmd, e))

        if is_verify_errorless_result:
            MgrUtils.verify_errorless_result(cmd=cmd, res=res)
        if parse_table_res:
            res = self.parse_result_table(res=res)
            if is_multiple_tables:
                dict_res_tables = self.parse_result_multiple_tables(res=res)
                return dict_res_tables
        return res

    def parse_result_table(self, res):
        parsed_table = []
        lines = res.stdout.split('\n')
        filtered_lines = [line for line in lines if
                          not (line.startswith('╭') or line.startswith('├') or line.startswith(
                              '╰'))]  # filter out the dashes lines
        filtered_lines = [line for line in filtered_lines if line]
        for line in filtered_lines:
            list_line = [s if s else 'EMPTY' for s in line.split("│")]  # filter out spaces and "|" column seperators
            list_line_no_spaces = [s.split() for s in list_line if s != 'EMPTY']
            list_line_with_multiple_words_join = []
            for words in list_line_no_spaces:
                list_line_with_multiple_words_join.append(" ".join(words))
            if list_line_with_multiple_words_join:
                parsed_table.append(list_line_with_multiple_words_join)
        return parsed_table

    def parse_result_multiple_tables(self, res):
        """

        # Datacenter: us-eastscylla_node_east
        # ╭──────────┬────────────────╮
        # │ CQL      │ Host           │
        # ├──────────┼────────────────┤
        # │ UP (1ms) │ 18.233.164.181 │
        # ╰──────────┴────────────────╯
        # Datacenter: us-west-2scylla_node_west
        # ╭────────────┬───────────────╮
        # │ CQL        │ Host          │
        # ├────────────┼───────────────┤
        # │ UP (180ms) │ 54.245.183.30 │
        # ╰────────────┴───────────────╯
        # the above output example was translated to a single table that includes both 2 DC's values:
        # [['Datacenter: us-eastscylla_node_east'],
        #  ['CQL', 'Host'],
        #  ['UP (1ms)', '18.233.164.181'],
        #  ['Datacenter: us-west-2scylla_node_west'],
        #  ['CQL', 'Host'],
        #  ['UP (180ms)', '54.245.183.30']]
        :param res:
        :return:
        """
        if not any(len(line) == 1 for line in res):  # "1" means a table title like DC-name is found.
            return {"single_table": res}

        dict_res_tables = {}
        cur_table = None
        for line in res:
            if len(line) == 1:  # "1" means it is the table title like DC name.
                cur_table = line[0]
                dict_res_tables[cur_table] = []
            else:
                dict_res_tables[cur_table].append(line)
        return dict_res_tables

    def get_table_value(self, parsed_table, identifier, column_name=None, is_search_substring=False):
        """

        :param parsed_table:
        :param column_name:
        :param identifier:
        :return:
        """

        # example expected parsed_table input is:
        # [['Host', 'Status', 'RTT'],
        #  ['18.234.77.216', 'UP', '0.92761'],
        #  ['54.203.234.42', 'DOWN', '0']]
        # usage flow example: mgr_cluster1.host -> mgr_cluster1.get_property -> get_table_value

        if not parsed_table or not self._is_found_in_table(parsed_table=parsed_table, identifier=identifier,
                                                           is_search_substring=is_search_substring):
            raise ScyllaManagerError(
                "Encountered an error retrieving sctool table value: {} not found in: {}".format(identifier,
                                                                                                 str(parsed_table)))
        column_titles = [title.upper() for title in
                         parsed_table[0]]  # get all table column titles capital (for comparison)
        if column_name and column_name.upper() not in column_titles:
            raise ScyllaManagerError("Column name: {} not found in table: {}".format(column_name, parsed_table))
        column_name_index = column_titles.index(
            column_name.upper()) if column_name else 1  # "1" is used in a case like "task progress" where no column names exist.
        ret_val = 'N/A'
        for row in parsed_table:
            if is_search_substring:
                if any(identifier in cur_str for cur_str in row):
                    ret_val = row[column_name_index]
                    break
            elif identifier in row:
                ret_val = row[column_name_index]
                break
        logger.debug("{} {} value is:{}".format(identifier, column_name, ret_val))
        return ret_val

    def _is_found_in_table(self, parsed_table, identifier, is_search_substring=False):
        full_rows_list = []
        for row in parsed_table:
            full_rows_list += row
        if is_search_substring:
            return any(identifier in cur_str for cur_str in full_rows_list)

        return identifier in full_rows_list


class ScyllaMgmt(object):
    """
    Provides communication with scylla-manager via REST API
    """

    def __init__(self, server, port=9090):
        self._url = 'http://{}:{}/api/v1/'.format(server, port)

    def get(self, path, params={}):
        resp = requests.get(url=self._url + path, params=params)
        if resp.status_code not in [200, 201, 202]:
            err_msg = 'GET request to scylla-manager failed! error: {}'.format(resp.content)
            logger.error(err_msg)
            raise Exception(err_msg)
        try:
            return json.loads(resp.content)
        except Exception as ex:
            logger.error('Failed load data from json %s, error: %s', resp.content, ex)
        return resp.content

    def post(self, path, data):
        resp = requests.post(url=self._url + path, data=json.dumps(data))
        if resp.status_code not in [200, 201]:
            err_msg = 'POST request to scylla-manager failed! error: {}'.format(resp.content)
            logger.error(err_msg)
            raise Exception(err_msg)
        return resp

    def put(self, path, data={}):
        resp = requests.put(url=self._url + path, data=json.dumps(data))
        if resp.status_code not in [200, 201]:
            err_msg = 'PUT request to scylla-manager failed! error: {}'.format(resp.content)
            logger.error(err_msg)
            raise Exception(err_msg)
        return resp

    def delete(self, path):
        resp = requests.delete(url=self._url + path)
        if resp.status_code not in [200, 204]:
            err_msg = 'DELETE request to scylla-manager failed! error: {}'.format(resp.content)
            logger.error(err_msg)
            raise Exception(err_msg)

    def get_cluster(self, cluster_name):
        """
        Get cluster by name
        :param cluster_name: cluster name
        :return: cluster id if found, otherwise None
        """
        resp = self.get('clusters', params={'name': cluster_name})

        if not resp:
            logger.debug('Cluster %s not found in scylla-manager', cluster_name)
            return None
        return resp[0]['id']

    def add_cluster(self, cluster_name, hosts, shard_count=16):
        """
        Add cluster to management
        :param cluster_name: cluster name
        :param hosts: list of cluster node IP-s
        :param shard_count: number of shards in nodes
        :return: cluster id
        """
        cluster_obj = {'name': cluster_name, 'hosts': hosts, 'shard_count': shard_count}
        resp = self.post('clusters', cluster_obj)
        return resp.headers['Location'].split('/')[-1]

    def delete_cluster(self, cluster_id):
        """
        Remove cluster from management
        :param cluster_id: cluster id/name
        :return: nothing
        """
        self.delete('cluster/{}'.format(cluster_id))

    def get_schedule_task(self, cluster_id):
        """
        Find auto scheduling repair task created automatically on cluster creation
        :param cluster_id: cluster id
        :return: task dict
        """
        resp = []
        while not resp:
            resp = self.get(path='cluster/{}/tasks'.format(cluster_id), params={'type': 'repair_auto_schedule'})
        return resp[0]

    def disable_task_schedule(self, cluster_id, task):
        start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=30)
        task['schedule'] = {'start_date': start_time.isoformat() + 'Z',
                            'interval_days': 0,
                            'num_retries': 0}
        task['enabled'] = True

        self.put(path='cluster/{}/task/repair_auto_schedule/{}'.format(cluster_id, task['id']), data=task)

    def start_repair_task(self, cluster_id, task_id, task_type='repair'):
        self.put(path='cluster/{}/task/{}/{}/start'.format(cluster_id, task_type, task_id))

    def get_repair_tasks(self, cluster_id):
        """
        Get cluster repair tasks(repair units per cluster keyspace)
        :param cluster_id: cluster id
        :return: repair tasks dict with unit id as key, task id as value
        """
        resp = []
        while not resp:
            resp = self.get(path='cluster/{}/tasks'.format(cluster_id), params={'type': 'repair'})
        tasks = {}
        for task in resp:
            unit_id = task['properties']['unit_id']
            if unit_id not in tasks and 'status' not in task:
                tasks[unit_id] = task['id']
        return tasks

    def get_task_progress(self, cluster_id, repair_unit):
        try:
            return self.get(path='cluster/{}/repair/unit/{}/progress'.format(cluster_id, repair_unit))
        except Exception as ex:
            logger.exception('Failed to get repair progress: %s', ex)
        return None

    def run_repair(self, cluster_id, timeout=0):
        """
        Run repair for cluster
        :param cluster_id: cluster id
        :param timeout: timeout in seconds to wait for repair done
        :return: repair status(True/False)
        """
        sched_task = self.get_schedule_task(cluster_id)

        if sched_task['schedule']['interval_days'] > 0:
            self.disable_task_schedule(cluster_id, sched_task)

        self.start_repair_task(cluster_id, sched_task['id'], 'repair_auto_schedule')

        tasks = self.get_repair_tasks(cluster_id)

        status = True
        # start repair tasks one by one:
        # currently scylla-manager cannot execute tasks simultaneously, only one can run
        logger.info('Start repair tasks per cluster %s keyspace', cluster_id)
        unit_to = timeout / len(tasks)
        start_time = time.time()
        for unit, task in tasks.iteritems():
            self.start_repair_task(cluster_id, task, 'repair')
            task_status = self.wait_for_repair_done(cluster_id, unit, unit_to)
            if task_status['status'] != STATUS_DONE or task_status['error']:
                logger.error('Repair unit %s failed, status: %s, error count: %s', unit,
                             task_status['status'], task_status['error'])
                status = False

        logger.debug('Repair finished with status: %s, time elapsed: %s', status, time.time() - start_time)
        return status

    def wait_for_repair_done(self, cluster_id, unit, timeout=0):
        """
        Wait for repair unit task finished
        :param cluster_id: cluster id
        :param unit: repair unit id
        :param timeout: timeout in seconds to wait for repair unit done
        :return: task status dict
        """
        done = False
        status = {'status': 'unknown', 'percent': 0, 'error': 0}
        interval = 10
        wait_time = 0

        logger.info('Wait for repair unit done: %s', unit)
        while not done and wait_time <= timeout:
            time.sleep(interval)
            resp = self.get_task_progress(cluster_id, unit)
            if not resp:
                break
            status['status'] = resp['status']
            status['percent'] = resp['percent_complete']
            if resp['error']:
                status['error'] = resp['error']
            if status['status'] in [STATUS_DONE, STATUS_ERROR]:
                done = True
            logger.debug('Repair status: %s', status)
            if timeout:
                wait_time += interval
                if wait_time > timeout:
                    logger.error('Waiting for repair unit %s: timeout expired: %s', unit, timeout)
        return status
