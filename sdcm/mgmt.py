# coding: utf-8
from avocado.utils import process
import requests
import logging
import json
import time
import datetime

from sdcm import wait

logger = logging.getLogger(__name__)

STATUS_DONE = 'done'
STATUS_ERROR = 'error'
MANAGER_IDENTITY_FILE = '/tmp/scylla_manager_pem'

from enum import Enum

class ScyllaManagerError(Exception):
    """
    A custom exception for Manager related errors
    """
    pass

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


class ScyllaLogicObj(object):

    def __init__(self, id, mgr_tool):
        self.id = id
        self.mgr_tool = mgr_tool

    def get_property(self, parsed_table, column_name):
        return self.mgr_tool.get_table_value(parsed_table=parsed_table, column_name=column_name, identifier=self.id)

    def _get_output_table_value(self, table_result, column_num, filter_param=None):

        """
        extract a column value out of an sctool output table
        :param table_result:
        :param column_num:
        :param filter_param:
        :return:
        """
        filter_param_updated = filter_param or self.id # default filtering (if not given) is by the scylla-object-id.
        if not table_result or filter_param_updated not in table_result.stdout:
            raise ScyllaManagerError("Encountered an error on sctool command response: {} not found in output: {}".format(filter_param_updated, str(table_result)))

        # example expected output is:
        # ╭──────────────────────────────────────┬──────┬─────────────┬────────────────╮
        # │ cluster id                           │ name │ host        │ ssh user       │
        # ├──────────────────────────────────────┼──────┼─────────────┼────────────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ sce2 │ 10.142.0.25 │ scylla-manager │
        # ╰──────────────────────────────────────┴──────┴─────────────┴────────────────╯
        lines = table_result.stdout.split('\n')
        title = filter_param or [s.split() for s in lines[1].split("│")][column_num][0] # get the column title (or given filter name)
        ret_val = 'N/A'
        for line in lines:
            if filter_param_updated in line:
                list_line = [s if s else 'EMPTY' for s in line.split("│")]
                list_line_no_spaces = [s.split()[0] for s in list_line]
                ret_val = list_line_no_spaces[column_num]
                break
        logger.debug("{}: {} {} is: {}".format(type(self), self.id, title, ret_val))
        return ret_val



class ManagerTask(ScyllaLogicObj):

    def __init__(self, task_id, mgr_cluster):
        ScyllaLogicObj.__init__(self, id=task_id, mgr_tool=mgr_cluster.mgr_tool)
        self.mgr_cluster = mgr_cluster

    def stop(self):
        cmd = "task stop {} -c {}".format(self.id, self.mgr_cluster.id)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)
        return self.wait_and_get_final_status(timeout=30, step=3)

    def start(self, cmd=None):
        cmd = cmd or "task start {} -c {}".format(self.id, self.mgr_cluster.id)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)
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
        cmd = "task history {} -c {}".format(self.id, self.mgr_cluster.id)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)
        return res # or can be specified like: self.get_property(parsed_table=res, column_name='status')

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
        cmd = "task list -c {}".format(self.mgr_cluster.id)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)
        return self.get_property(parsed_table=res, column_name='next run')

    @property
    def status(self):
        """
        Gets the repair task's status
        """
        cmd = "task list -c {}".format(self.mgr_cluster.id)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)
        return self.get_property(parsed_table=res, column_name='status')

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
        if self.status == TaskStatus.NEW:
            return "0%"
        cmd = "task progress {} -c {}".format(self.id, self.mgr_cluster.id)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd)
        # expecting output of:
        # ╭─────────────┬─────────────────────╮
        # │ Status      │ DONE                │
        # │ Start time  │ 28 Oct 18 15:02 UTC │
        # │ End time    │ 28 Oct 18 16:14 UTC │
        # │ Duration    │ 1h12m23s            │
        # │ Progress    │ 100%                │
        # │ Datacenters │ [datacenter1]       │
        # ├─────────────┼─────────────────────┤
        return self.mgr_tool.get_table_value(parsed_table=res, identifier="Progress")

    def is_status_in_list(self, list_status, check_task_progress=False):
        """
        Check if the status of a given task is in list
        :param list_status:
        :return:
        """
        status = self.status
        if check_task_progress and status != TaskStatus.NEW: # check progress for all statuses except 'NEW'
            progress = self.progress
        return self.status in list_status

    def wait_for_status(self, list_status, check_task_progress=True, timeout=3600, step=120):
        text = "Waiting until task: {} reaches status of: {}".format(self.id, list_status)
        is_status_reached = wait.wait_for(func=self.is_status_in_list, step=step,
                                          text=text, list_status=list_status, check_task_progress=check_task_progress, timeout=timeout)
        return is_status_reached

    def wait_and_get_final_status(self, timeout=3600, step=120):
        """
        1) Wait for task to reach a 'final' status. meaning one of: done/error/stopped
        2) return the final status.
        :return:
        """
        list_final_status = [TaskStatus.ERROR, TaskStatus.STOPPED, TaskStatus.DONE]
        logger.debug("Waiting for task: {} getting to a final status ({})..".format(self.id, [str(s) for s in list_final_status]))
        res = self.wait_for_status(list_status=list_final_status, timeout=timeout, step=step)
        if not res:
            raise ScyllaManagerError("Unexpected result on waiting for task {} status".format(self.id))
        return self.status


class RepairTask(ManagerTask):
    def __init__(self, task_id, mgr_cluster):
        ManagerTask.__init__(self, task_id=task_id, mgr_cluster=mgr_cluster)

    def start(self, use_continue=False, **kwargs):
        str_continue = '--continue=true' if use_continue else '--continue=false'
        cmd = "task start {} -c {} {}".format(self.id, self.mgr_cluster.id, str_continue)
        ManagerTask.start(self, cmd=cmd)

    def continue_repair(self):
        self.start(use_continue=True)

class HealthcheckTask(ManagerTask):
    def __init__(self, task_id, mgr_cluster):
        ManagerTask.__init__(self, task_id=task_id, mgr_cluster=mgr_cluster)

class ManagerCluster(ScyllaLogicObj):

    def __init__(self, mgr_tool, cluster_id):
        if not mgr_tool:
            raise ScyllaManagerError("Cannot create a Manager Cluster where no 'manager tool' parameter is given")
        ScyllaLogicObj.__init__(self, id=cluster_id, mgr_tool=mgr_tool)

    def create_repair_task(self):
        cmd = "repair -c {}".format(self.id)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd)
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

        return RepairTask(task_id=task_id, mgr_cluster=self) # return the manager's object with new repair-task-id

    def delete(self):
        """
        $ sctool cluster delete
        """

        cmd = "cluster delete -c {}".format(self.id)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)

    def update(self, name=None, host=None, ssh_identity_file=None, ssh_user=None):
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
            cmd+=" --name={}".format(name)
        if host:
            cmd+=" --host={}".format(host)
        if ssh_identity_file:
            cmd+=" --ssh-identity-file={}".format(ssh_identity_file)
        if ssh_user:
            cmd+=" --ssh-user={}".format(ssh_user)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)

    @property
    def host(self):
        """
        Gets the Cluster host as represented in Manager
        """
        # expecting output of:
        # ╭──────────────────────────────────────┬──────┬─────────────┬────────────────╮
        # │ cluster id                           │ name │ host        │ ssh user       │
        # ├──────────────────────────────────────┼──────┼─────────────┼────────────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ sce2 │ 10.142.0.25 │ scylla-manager │
        # ╰──────────────────────────────────────┴──────┴─────────────┴────────────────╯

        return self.get_property(parsed_table=self.mgr_tool.cluster_list, column_name='host')

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
        return self.get_property(parsed_table=self.mgr_tool.cluster_list, column_name='name')

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
        return self.get_property(parsed_table=self.mgr_tool.cluster_list, column_name='ssh user')

    def get_healthcheck_task(self):
        cmd = "task list -c {}".format(self.id)
        parsed_table = self.mgr_tool.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)
        healthcheck_id = self.mgr_tool.get_table_value(parsed_table=parsed_table, column_name="task", identifier="healthcheck/", is_search_substring=True)
        return HealthcheckTask(task_id=healthcheck_id, mgr_cluster=self)

    def get_hosts_health(self):
        """
        Gets the Manager's Cluster Nodes status
        """
        # $ sctool status -c cluster1
        # ╭───────────────┬────────┬──────────╮
        # │ Host          │ Status │ RTT (ms) │
        # ├───────────────┼────────┼──────────┤
        # │ 18.234.77.216 │ UP     │ 0.92761  │
        # │ 54.203.234.42 │ DOWN   │ 0        │
        # ╰───────────────┴────────┴──────────╯
        # or where all nodes up:
        # ╭────────────────┬────────┬──────────╮
        # │ Host           │ Status │ RTT (ms) │
        # ├────────────────┼────────┼──────────┤
        # │ 34.220.201.186 │ UP     │ 152.76   │
        # │ 54.164.46.117  │ UP     │ 0.82     │
        # ╰────────────────┴────────┴──────────╯
        cmd = "status -c {}".format(self.id)
        parsed_table = self.mgr_tool.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)

        dict_hosts_health = {}
        if len(parsed_table) < 2:
            logger.debug("Cluster: {} has no hosts health report".format(self.id))
        else:
            for line in parsed_table[1:]:
                host = line[0]
                status = line[1]
                rtt = line[2]
                dict_hosts_health[host] = HostHealth(status=HostStatus.from_str(status), rtt=rtt)
        logger.debug("Cluster {} Hosts Health is:".format(self.id))
        for ip, health in dict_hosts_health.items():
            logger.debug("{}: {},{}".format(ip, health.status, health.rtt))
        return dict_hosts_health

class HostHealth():
    def __init__(self,status,rtt):
        self.status = status
        self.rtt = rtt

def verify_errorless_result(cmd, res):
    if not res or res.stderr:
        logger.error("Encountered an error on '{}' command response: {}".format(cmd, str(res)))
        raise ScyllaManagerError("Encountered an error on '{}' command response".format(cmd))

class ScyllaManagerTool(object):
    """
    Provides communication with scylla-manager, operating sctool commands and ssh-scripts.
    """

    def __init__(self, manager_node):
        self.manager_node = manager_node
        sleep = 30
        logger.debug('Sleep {} seconds, waiting for manager service ready to respond'.format(sleep))
        time.sleep(sleep)

    @property
    def version(self):
        cmd = "version"
        res = self.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)
        logger.info("Manager version is: {}".format(res))
        return res

    def _mgr_remoter_run(self, cmd):
        return self.manager_node.remoter.run(cmd)

    def run_sctool_cmd(self, cmd, is_verify_errorless_result = False, parse_table_res = True):
        logger.debug("Issuing: 'sctool {}'".format(cmd))
        try:
            res = self._mgr_remoter_run(cmd='sudo sctool {}'.format(cmd))
        except process.CmdError as e:
            raise ScyllaManagerError("Encountered an error on sctool command: {}: {}".format(cmd, e))

        if is_verify_errorless_result:
            verify_errorless_result(cmd=cmd, res=res)
        if parse_table_res:
            res = self.parse_sctool_result_table(res=res)
        return res

    def parse_sctool_result_table(self, res):
        parsed_table = []
        lines = res.stdout.split('\n')
        filtered_lines = [line for line in lines if not (line.startswith('╭') or line.startswith('├') or line.startswith(
            '╰'))]  # filter out the dashes lines
        filtered_lines = [line for line in filtered_lines if line]
        for line in filtered_lines:
            list_line = [s if s else 'EMPTY' for s in line.split("│")] # filter out spaces and "|" column seperators
            list_line_no_spaces = [s.split() for s in list_line if s != 'EMPTY']
            list_line_with_multiple_words_join = []
            for words in list_line_no_spaces:
                list_line_with_multiple_words_join.append(" ".join(words))
            if list_line_with_multiple_words_join:
                parsed_table.append(list_line_with_multiple_words_join)
        return parsed_table

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

        if not parsed_table or not is_found_in_table(parsed_table=parsed_table, identifier=identifier, is_search_substring=is_search_substring):
            raise ScyllaManagerError("Encountered an error retrieving sctool table value: {} not found in: {}".format(identifier, str(parsed_table)))
        column_titles = [title.upper() for title in parsed_table[0]] # get all table column titles capital (for comparison)
        if column_name and column_name.upper() not in column_titles:
            raise ScyllaManagerError("Column name: {} not found in table: {}".format(column_name, parsed_table))
        column_name_index = column_titles.index(column_name.upper()) if column_name else 1  # "1" is used in a case like "task progress" where no column names exist.
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

    @property
    def cluster_list(self):
        """
        Gets the Manager's Cluster list
        """
        cmd = "cluster list"
        return self.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)

    def get_cluster(self, cluster_name):
        cmd = 'cluster list'
        res = self.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)

        # expecting output of:
        # ╭──────────────────────────────────────┬──────┬─────────────┬────────────────╮
        # │ cluster id                           │ name │ host        │ ssh user       │
        # ├──────────────────────────────────────┼──────┼─────────────┼────────────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ sce  │ 10.142.0.25 │ scylla-manager │
        # ╰──────────────────────────────────────┴──────┴─────────────┴────────────────╯
        if not res.stdout or cluster_name not in res.stdout:
            logger.debug('Cluster {} not found in scylla-manager'.format(cluster_name))
            return None

        cluster_id = "N/A"
        lines = res.stdout.split('\n')
        for line in lines:
            if cluster_name in line:
                cluster_id = line.split()[1]
                break
        logger.debug("Cluster: {} ID is: {}".format(cluster_name, cluster_id))
        return ManagerCluster(mgr_tool=self,cluster_id=cluster_id)


    def scylla_mgr_ssh_setup(self, node_ip, user='centos', identity_file='/tmp/scylla-test', manager_user='scylla-manager', manager_identity_file=MANAGER_IDENTITY_FILE):
        """
        scyllamgr_ssh_setup -u <username> -i <path to private key> --m <manager username> -o <path to manager private key> [HOST...]
          -u --user				SSH user name used to connect to hosts
          -i --identity-file			path to identity file containing SSH private key
          -m --manager-user			user name that will be created and configured on hosts, default scylla-manager
          -o --manager-identity-file		path to identity file containing SSH private key for MANAGER_USERNAME, if there is no such file it will be created
          -d --discover				use first host to discover and setup all hosts in a cluster


        :param node_ip:
        :param identity_file:
        :param manager_user:
        :return:

        sudo scyllamgr_ssh_setup --user centos --identity-file /tmp/scylla-qa-ec2 --manager-user scylla-manager --manager-identity-file /tmp/scylla_manager_pem --discover 54.158.51.22"
        """
        cmd = 'sudo scyllamgr_ssh_setup --user {} --identity-file {} --manager-user {} --manager-identity-file {} --discover {}'.format(user, identity_file, manager_user, manager_identity_file, node_ip)
        logger.debug("SSH setup command is: {}".format(cmd))
        res = self._mgr_remoter_run(cmd=cmd)
        verify_errorless_result(cmd=cmd, res=res)




    def add_cluster(self, name, host):
        """
        Add cluster to management
        :param name: cluster name
        :param host: cluster node IP-s
        :return: cluster id

        --host string              hostname or IP of one of the cluster nodes
        -n, --name alias               alias you can give to your cluster
        --ssh-identity-file path   path to identity file containing SSH private key
        --ssh-user name            SSH user name used to connect to the cluster nodes

        """
        logger.debug("Configuring ssh setup for cluster using {} node before adding the cluster: {}".format(host, name))
        self.scylla_mgr_ssh_setup(node_ip=host)
        identity_file_centos = '/tmp/scylla-test'
        ssh_user='scylla-manager'
        manager_identity_file=MANAGER_IDENTITY_FILE
        sctool_cmd = 'cluster add --host={} --ssh-identity-file={} --ssh-user={} --name={}'.format(host, manager_identity_file, ssh_user, name)
        logger.debug("Cluster add command is: {}".format(sctool_cmd))
        res = self.run_sctool_cmd(sctool_cmd, parse_table_res=False)
        if not res or 'Cluster added' not in res.stderr:
            raise ScyllaManagerError("Encountered an error on 'sctool cluster add' command response: {}".format(res))
        cluster_id = res.stdout.split('\n')[0] # return ManagerCluster instance with the manager's new cluster-id
        return ManagerCluster(mgr_tool=self, cluster_id=cluster_id)

def is_found_in_table(parsed_table, identifier, is_search_substring = False):
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
