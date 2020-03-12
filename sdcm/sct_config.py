"""
Handling Scylla-cluster-test configuration loading
"""

# pylint: disable=too-many-lines
from __future__ import print_function

import os
import ast
import logging
import getpass
from distutils.util import strtobool  # pylint: disable=import-error,no-name-in-module

import anyconfig

from sdcm.utils.common import get_s3_scylla_repos_mapping, get_scylla_ami_versions, get_branched_ami
from sdcm.utils.version_utils import get_branch_version

logging.getLogger("anyconfig").setLevel(logging.ERROR)


LOGGER = logging.getLogger(__name__)


# pylint: disable=too-few-public-methods
class UnsetMarker(object):
    """
    Marker object for function empty default, used SCTConfiguration.get to mimic dict.get behavior
    """
    pass


def str_or_list(value):
    """
    Convert an environment variable into a python list

    :param value: raw string variable
    :return: list of strings
    """
    if isinstance(value, basestring):
        try:
            return ast.literal_eval(value)
        except Exception:  # pylint: disable=broad-except
            pass
        return [str(value)]

    elif isinstance(value, list):
        return value

    raise ValueError("{} isn't string or list".format(value))


def int_or_list(value):
    try:
        value = int(value)
        return value
    except Exception:  # pylint: disable=broad-except
        pass

    if isinstance(value, basestring):
        try:
            values = value.split()
            [int(v) for v in values]  # pylint: disable=expression-not-assigned
            return value
        except Exception:  # pylint: disable=broad-except
            pass
        try:
            return ast.literal_eval(value)
        except Exception:  # pylint: disable=broad-except
            pass

    raise ValueError("{} isn't int or list".format(value))


def boolean(value):
    if isinstance(value, bool):
        return value
    elif isinstance(value, basestring):
        return bool(strtobool(value))
    else:
        raise ValueError("{} isn't a boolean".format(type(value)))


def sct_abs_path(relative_filename):
    sct_root = os.path.dirname(os.path.dirname(__file__))
    return os.path.join(sct_root, relative_filename)


class SCTConfiguration(dict):
    """
    Class the hold the SCT configuration
    """

    available_backends = ['aws', 'gce', 'docker', 'libvirt', 'baremetal', 'openstack', 'aws-siren']

    config_options = [
        dict(name="config_files", env="SCT_CONFIG_FILES", type=str_or_list,
             help="a list of config files that would be used"),

        dict(name="cluster_backend", env="SCT_CLUSTER_BACKEND", type=str,
             help="backend that will be used, aws/gce/docker/libvirt/openstack"),

        dict(name="test_duration", env="SCT_TEST_DURATION", type=int,
             help="""
                  Test duration (min). Parameter used to keep instances produced by tests that are
                  supposed to run longer than 24 hours from being killed
             """),

        dict(name="n_db_nodes", env="SCT_N_DB_NODES", type=int_or_list,
             help="Number list of database nodes in multiple data centers."),

        dict(name="n_test_oracle_db_nodes", env="SCT_N_TEST_ORACLE_DB_NODES", type=int_or_list,
             help="Number list of oracle test nodes in multiple data centers."),

        dict(name="n_loaders", env="SCT_N_LOADERS", type=int_or_list,
             help="Number list of loader nodes in multiple data centers"),

        dict(name="n_monitor_nodes", env="SCT_N_MONITORS_NODES", type=int_or_list,
             help="Number list of monitor nodes in multiple data centers"),

        dict(name="intra_node_comm_public", env="SCT_INTRA_NODE_COMM_PUBLIC", type=boolean,
             help="If True, all communication between nodes are via public addresses"),

        dict(name="endpoint_snitch", env="SCT_ENDPOINT_SNITCH", type=str,
             help="""
                The snitch class scylla would use

                'GossipingPropertyFileSnitch' - default
                'Ec2MultiRegionSnitch' - default on aws backend
                'GoogleCloudSnitch'
             """),

        dict(name="user_credentials_path", env="SCT_USER_CREDENTIALS_PATH", type=str,
             help="""Path to your user credentials. qa key are downloaded automatically from S3 bucket"""),

        dict(name="cloud_credentials_path", env="SCT_CLOUD_CREDENTIALS_PATH", type=str,
             help="""Path to your user credentials. qa key are downloaded automatically from S3 bucket"""),

        dict(name="ip_ssh_connections", env="SCT_IP_SSH_CONNECTIONS", type=str,
             help="""
                Type of IP used to connect to machine instances.
                This depends on whether you are running your tests from a machine inside
                your cloud provider, where it makes sense to use 'private', or outside (use 'public')

                Default: Use public IPs to connect to instances (public)
                Use private IPs to connect to instances (private)
                Use IPv6 IPs to connect to instances (ipv6)
             """,
             choices=("public", "private", "ipv6"),
             ),

        dict(name="scylla_repo", env="SCT_SCYLLA_REPO", type=str,
             help="Url to the repo of scylla version to install scylla"),

        dict(name="scylla_version", env="SCT_SCYLLA_VERSION",
             type=str,
             help="""Version of scylla to install, ex. '2.3.1'
                     Automatically lookup AMIs and repo links for formal versions.
                     WARNING: can't be used together with 'scylla_repo' or 'ami_id_db_scylla'"""),

        dict(name="scylla_linux_distro", env="SCT_SCYLLA_LINUX_DISTRO", type=str,
             help="""The distro name and family name to use [centos/ubuntu-xenial/debien-jessie]"""),

        dict(name="scylla_linux_distro_loader", env="SCT_SCYLLA_LINUX_DISTRO_LOADER", type=str,
             help="""The distro name and family name to use [centos/ubuntu-xenial/debien-jessie]"""),

        dict(name="scylla_repo_m", env="SCT_SCYLLA_REPO_M", type=str,
             help="Url to the repo of scylla version to install scylla from for managment tests"),

        dict(name="scylla_repo_loader", env="SCT_SCYLLA_REPO_LOADER", type=str,
             help="Url to the repo of scylla version to install c-s for loader"),

        dict(name="scylla_mgmt_repo", env="SCT_SCYLLA_MGMT_REPO",
             type=str,
             help="Url to the repo of scylla manager version to install for management tests"),

        dict(name="use_mgmt", env="SCT_USE_MGMT", type=boolean,
             help="When define true, will install scylla management"),

        dict(name="mgmt_port", env="SCT_MGMT_PORT", type=int,
             help="The port of scylla management"),

        dict(name="update_db_packages", env="SCT_UPDATE_DB_PACKAGES", type=str,
             help="""A local directory of rpms to install a custom version on top of
                     the scylla installed (or from repo or from ami)"""),

        dict(name="monitor_branch", env="SCT_MONITOR_BRANCH", type=str,
             help="The port of scylla management"),

        dict(name="db_type", env="SCT_DB_TYPE", type=str,
             help="Db type to install into db nodes, scylla/cassandra"),

        dict(name="user_prefix", env="SCT_USER_PREFIX", type=str,

             help="the prefix of the name of the cloud instances, defaults to username"),

        dict(name="ami_id_db_scylla_desc", env="SCT_AMI_ID_DB_SCYLLA_DESC", type=str,
             help="version name to report stats to Elasticsearch and tagged on cloud instances"),

        dict(name="store_results_in_elasticsearch", env="SCT_STORE_RESULTS_IN_ELASTICSEARCH", type=boolean,
             help="save the results in elasticsearch"),

        dict(name="sct_public_ip", env="SCT_SCT_PUBLIC_IP", type=str,
             help="""
                Override the default hostname address of the sct test runner,
                for the monitoring of the Nemesis.
                can only work out of the box in AWS
             """),
        dict(name="sct_ngrok_name", env="SCT_NGROK_NAME", type=str,
             help="""
            Override the default hostname address of the sct test runner,
            using ngrok server, see readme for more instructions
         """),

        dict(name="backtrace_decoding", env="SCT_BACKTRACE_DECODING", type=boolean,
             help="""If True, all backtraces found in db nodes would be decoded automatically"""),


        dict(name="reuse_cluster", env="SCT_REUSE_CLUSTER", type=str,
             help="""
            If reuse_cluster is set it should hold test_id of the cluster that will be reused.
            `reuse_cluster: 7dc6db84-eb01-4b61-a946-b5c72e0f6d71`
         """),

        dict(name="test_id", env="SCT_TEST_ID", type=str,
             help="""Set the test_id of the run manually. Use only from the env before running Hydra"""),

        dict(name="seeds_selector", env="SCT_SEEDS_SELECTOR", type=str,
             choices=['reflector', 'random', 'first'],
             help="""How to select the seeds. Expected values: reflector/random/first"""),

        dict(name="seeds_num", env="SCT_SEEDS_NUM", type=int,
             help="""Number of seeds to select"""),

        dict(name="send_email", env="SCT_SEND_EMAIL", type=boolean,
             help="""If true would send email out of the performance regression test"""),

        dict(name="email_recipients", env="SCT_EMAIL_RECIPIENTS", type=str_or_list,
             help="""list of email of send the performance regression test to"""),

        # should be removed once stress commands would be refactored
        dict(name="bench_run", env="SCT_BENCH_RUN", type=boolean,
             help="""If true would kill the scylla-bench thread in the test teardown"""),

        # should be removed once stress commands would be refactored
        dict(name="fullscan", env="SCT_FULLSCAN", type=boolean,
             help="""If true would kill the fullscan thread in the test teardown"""),

        # Scylla command line arguments options
        dict(name="experimental", env="SCT_EXPERIMENTAL", type=boolean,
             help="when enabled scylla will use it's experimental features"),

        dict(name="server_encrypt", env="SCT_SERVER_ENCRYPT", type=boolean,
             help="when enable scylla will use encryption on the server side"),

        dict(name="client_encrypt", env="SCT_CLIENT_ENCRYPT", type=boolean,
             help="when enable scylla will use encryption on the client side"),

        dict(name="hinted_handoff", env="SCT_HINTED_HANDOFF", type=str,
             help="when enable or disable scylla hinted handoff (enabled/disabled)"),

        dict(name="authenticator", env="SCT_AUTHENTICATOR", type=str,
             help="which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator",
             choices=("PasswordAuthenticator", "AllowAllAuthenticator"),
             ),

        dict(name="authenticator_user", env="SCT_AUTHENTICATOR_USER", type=str,
             help="the username if PasswordAuthenticator is used"),

        dict(name="authenticator_password", env="SCT_AUTHENTICATOR_PASSWORD", type=str,
             help="the password if PasswordAuthenticator is used"),

        dict(name="authorizer", env="SCT_AUTHORIZER", type=str,
             help="which authorizer scylla will use AllowAllAuthorizer/CassandraAuthorizer"),

        dict(name="system_auth_rf", env="SCT_SYSTEM_AUTH_RF", type=str,
             help="Replication factor will be set to system_auth"),

        dict(name="alternator_port", env="SCT_ALTERNATOR_PORT", type=int,
             help="Port to configure for alternator in scylla.yaml"),
        dict(name="dynamodb_primarykey_type", env="SCT_DYNAMODB_PRIMARYKEY_TYPE", type=str,
             help="Type of dynamodb table to create with range key or not, can be HASH or HASH_AND_RANGE"),

        dict(name="append_scylla_args", env="SCT_APPEND_SCYLLA_ARGS", type=str,
             help="More arguments to append to scylla command line"),

        dict(name="append_scylla_args_oracle", env="SCT_APPEND_SCYLLA_ARGS_ORACLE", type=str,
             help="More arguments to append to oracle command line"),

        dict(name="append_scylla_yaml", env="SCT_APPEND_SCYLLA_YAML", type=str,
             help="More configuration to append to /etc/scylla/scylla.yaml"),

        # Nemesis config options

        dict(name="nemesis_class_name", env="SCT_NEMESIS_CLASS_NAME", type=str,
             help="""
                    Nemesis class to use (possible types in sdcm.nemesis).
                    Next syntax supporting:
                    - nemesis_class_name: "NemesisName"  Run one nemesis in single thread
                    - nemesis_class_name: "<NemesisName>:<num>" Run <NemesisName> in <num>
                      parallel threads on different nodes. Ex.: "ChaosMonkey:2"
                    - nemesis_class_name: "<NemesisName1>:<num1> <NemesisName2>:<num2>" Run
                      <NemesisName1> in <num1> parallel threads and <NemesisName2> in <num2>
                      parallel threads. Ex.: "DisruptiveMonkey:1 NonDisruptiveMonkey:2"
            """),

        dict(name="nemesis_interval", env="SCT_NEMESIS_INTERVAL", type=int,
             help="""Nemesis sleep interval to use if None provided specifically in the test"""),

        dict(name="nemesis_during_prepare", env="SCT_NEMESIS_DURING_PREPARE", type=boolean,
             help="""Run nemesis during prepare stage of the test"""),

        dict(name="cluster_target_size", env="SCT_CLUSTER_TARGET_SIZE", type=int,
             help="""Used for scale test: max size of the cluster"""),

        dict(name="space_node_threshold", env="SCT_SPACE_NODE_THRESHOLD", type=int,
             help="""
                 Space node threshold before starting nemesis (bytes)
                 The default value is 6GB (6x1024^3 bytes)
                 This value is supposed to reproduce
                 https://github.com/scylladb/scylla/issues/1140
             """),

        dict(name="nemesis_filter_seeds", env="SCT_NEMESIS_FILTER_SEEDS", type=boolean,
             help="""If true runs the nemesis only on non seed nodes"""),

        # Stress Commands

        dict(name="stress_cmd", env="SCT_STRESS_CMD", type=str_or_list,
             help="""cassandra-stress commands.
                    You can specify everything but the -node parameter, which is going to
                    be provided by the test suite infrastructure.
                    multiple commands can passed as a list"""),

        dict(name="gemini_version", env="SCT_GEMINI_VERSION", type=str,
             help="""Version of download of the binaries of gemini tool"""),

        dict(name="gemini_schema_url", env="SCT_GEMINI_SCHEMA_URL", type=str,
             help="""Url of the schema/configuration the gemini tool would use """),

        dict(name="gemini_cmd", env="SCT_GEMINI_CMD", type=str,
             help="""gemini command to run (for now used only in GeminiTest)"""),

        dict(name="gemini_seed", env="SCT_GEMINI_SEED", type=int,
             help="Seed number for gemini command"),
        # AWS config options

        dict(name="instance_type_loader", env="SCT_INSTANCE_TYPE_LOADER", type=str,
             help="AWS image type of the loader node"),

        dict(name="instance_type_monitor", env="SCT_INSTANCE_TYPE_MONITOR", type=str,
             help="AWS image type of the monitor node"),

        dict(name="instance_type_db", env="SCT_INSTANCE_TYPE_DB", type=str,
             help="AWS image type of the db node"),

        dict(name="instance_type_db_oracle", env="SCT_INSTANCE_TYPE_DB_ORACLE", type=str,
             help="AWS image type of the oracle node"),

        dict(name="region_name", env="SCT_REGION_NAME", type=str_or_list,
             help="AWS regions to use"),

        dict(name="security_group_ids", env="SCT_SECURITY_GROUP_IDS", type=str_or_list,
             help="AWS security groups ids to use"),

        dict(name="subnet_id", env="SCT_SUBNET_ID", type=str_or_list,
             help="AWS subnet ids to use"),

        dict(name="ami_id_db_scylla", env="SCT_AMI_ID_DB_SCYLLA", type=str,
             help="AMS AMI id to use for scylla db node"),

        dict(name="ami_id_loader", env="SCT_AMI_ID_LOADER", type=str,
             help="AMS AMI id to use for loader node"),

        dict(name="ami_id_monitor", env="SCT_AMI_ID_MONITOR", type=str,
             help="AMS AMI id to use for monitor node"),

        dict(name="ami_id_db_cassandra", env="SCT_AMI_ID_DB_CASSANDRA", type=str,
             help="AMS AMI id to use for cassandra node"),

        dict(name="ami_id_db_oracle", env="SCT_AMI_ID_DB_ORACLE", type=str,
             help="AMS AMI id to use for oracle node"),

        dict(name="aws_root_disk_size_db", env="SCT_AWS_ROOT_DISK_SIZE_DB", type=int,
             help=""),

        dict(name="aws_root_disk_name_db", env="SCT_AWS_ROOT_DISK_NAME_DB", type=str,
             help=""),

        dict(name="aws_root_disk_size_monitor", env="SCT_AWS_ROOT_DISK_SIZE_MONITOR", type=int,
             help=""),

        dict(name="aws_root_disk_name_monitor", env="SCT_AWS_ROOT_DISK_NAME_MONITOR", type=str,
             help=""),

        dict(name="aws_root_disk_size_loader", env="SCT_AWS_ROOT_DISK_SIZE_LOADER", type=int,
             help=""),

        dict(name="aws_root_disk_name_loader", env="SCT_AWS_ROOT_DISK_NAME_LOADER", type=str,
             help=""),

        dict(name="ami_db_scylla_user", env="SCT_AMI_DB_SCYLLA_USER", type=str,
             help=""),

        dict(name="ami_monitor_user", env="SCT_AMI_MONITOR_USER", type=str,
             help=""),

        dict(name="ami_loader_user", env="SCT_AMI_LOADER_USER", type=str,
             help=""),

        dict(name="ami_db_cassandra_user", env="SCT_AMI_DB_CASSANDRA_USER", type=str,
             help=""),

        dict(name="instance_provision", env="SCT_INSTANCE_PROVISION", type=str,
             help=" aws instance_provision: on_demand|spot_fleet|spot_low_price|spot_duration"),

        dict(name="spot_max_price", env="SCT_SPOT_MAX_PRICE", type=float,
             help="The max percentage of the on demand price we set for spot/fleet instances"),

        dict(name="aws_extra_network_interface", env="SCT_AWS_EXTRA_NETWORK_INTERFACE", type=boolean,
             help="if true, create extra network interface on each node"),

        dict(name="tag_ami_with_result", env="SCT_TAG_AMI_WITH_RESULT", type=boolean,
             help="If True, would tag the ami with the test final result"),

        # GCE config options

        dict(name="gce_datacenter", env="SCT_GCE_DATACENTER", type=str,
             help=""),

        dict(name="gce_network", env="SCT_GCE_DATACENTER", type=str,
             help=""),

        dict(name="gce_image", env="SCT_GCE_IMAGE", type=str,
             help=""),

        dict(name="gce_image_db", env="SCT_GCE_IMAGE_DB", type=str,
             help=""),

        dict(name="gce_image_monitor", env="SCT_GCE_IMAGE_MONITOR", type=str,
             help=""),

        dict(name="gce_image_username", env="SCT_GCE_IMAGE_USERNAME", type=str,
             help=""),

        dict(name="gce_instance_type_loader", env="SCT_GCE_INSTANCE_TYPE_LOADER", type=str,
             help=""),

        dict(name="gce_root_disk_type_loader", env="SCT_GCE_ROOT_DISK_TYPE_LOADER", type=str,
             help=""),

        dict(name="gce_n_local_ssd_disk_loader", env="SCT_GCE_N_LOCAL_SSD_DISK_LOADER", type=int,
             help=""),

        dict(name="gce_instance_type_monitor", env="SCT_GCE_INSTANCE_TYPE_MONITOR", type=str,
             help=""),

        dict(name="gce_root_disk_type_monitor", env="SCT_GCE_ROOT_DISK_TYPE_MONITOR", type=str,
             help=""),

        dict(name="gce_root_disk_size_monitor", env="SCT_GCE_ROOT_DISK_SIZE_MONITOR", type=int,
             help=""),

        dict(name="gce_n_local_ssd_disk_monitor", env="SCT_GCE_N_LOCAL_SSD_DISK_MONITOR", type=int,
             help=""),

        dict(name="gce_instance_type_db", env="SCT_GCE_INSTANCE_TYPE_DB", type=str,
             help=""),

        dict(name="gce_root_disk_type_db", env="SCT_GCE_ROOT_DISK_TYPE_DB", type=str,
             help=""),

        dict(name="gce_root_disk_size_db", env="SCT_GCE_ROOT_DISK_SIZE_DB", type=int,
             help=""),

        dict(name="gce_n_local_ssd_disk_db", env="SCT_GCE_N_LOCAL_SSD_DISK_DB", type=int,
             help=""),


        # docker config options

        dict(name="docker_image", env="SCT_DOCKER_IMAGE", type=str, help=""),

        # libvirt config options

        dict(name="libvirt_uri", env="SCT_LIBVIRT_URI", type=str,
             help=""),

        dict(name="libvirt_bridge", env="SCT_LIBVIRT_BRIDGE", type=str,
             help=""),

        dict(name="libvirt_loader_image", env="SCT_LIBVIRT_LOADER_IMAGE", type=str,
             help=""),

        dict(name="libvirt_loader_image_user", env="SCT_LIBVIRT_LOADER_IMAGE_USER", type=str,
             help=""),

        dict(name="libvirt_loader_image_password", env="SCT_LIBVIRT_LOADER_IMAGE_PASSWORD", type=str,
             help=""),

        dict(name="libvirt_loader_os_type", env="SCT_LIBVIRT_LOADER_OS_TYPE", type=str,
             help=""),

        dict(name="libvirt_loader_os_variant", env="SCT_LIBVIRT_LOADER_OS_VARIANT", type=str,
             help=""),

        dict(name="libvirt_loader_memory", env="SCT_LIBVIRT_LOADER_MEMORY", type=int,
             help=""),

        dict(name="libvirt_db_image", env="SCT_LIBVIRT_DB_IMAGE", type=str,
             help=""),

        dict(name="libvirt_db_image_user", env="SCT_LIBVIRT_DB_IMAGE_USER", type=str,
             help=""),

        dict(name="libvirt_db_image_password", env="SCT_LIBVIRT_DB_IMAGE_PASSWORD", type=str,
             help=""),

        dict(name="libvirt_db_os_type", env="SCT_LIBVIRT_DB_OS_TYPE", type=str,
             help=""),

        dict(name="libvirt_db_os_variant", env="SCT_LIBVIRT_DB_OS_VARIANT", type=str,
             help=""),

        dict(name="libvirt_db_memory", env="SCT_LIBVIRT_DB_MEMORY", type=int,
             help=""),

        dict(name="libvirt_monitor_image", env="SCT_LIBVIRT_MONITOR_IMAGE", type=str,
             help=""),

        dict(name="libvirt_monitor_image_user", env="SCT_LIBVIRT_MONITOR_IMAGE_USER", type=str,
             help=""),

        dict(name="libvirt_monitor_image_password", env="SCT_LIBVIRT_MONITOR_IMAGE_PASSWORD", type=str,
             help=""),

        dict(name="libvirt_monitor_os_type", env="SCT_LIBVIRT_MONITOR_OS_TYPE", type=str,
             help=""),

        dict(name="libvirt_monitor_os_variant", env="SCT_LIBVIRT_MONITOR_OS_VARIANT", type=str,
             help=""),

        dict(name="libvirt_monitor_memory", env="SCT_LIBVIRT_MONITOR_MEMORY", type=int,
             help=""),

        # baremetal config options

        dict(name="db_nodes_private_ip", env="SCT_DB_NODES_PRIVATE_IP", type=str_or_list,
             help=""),

        dict(name="db_nodes_public_ip", env="SCT_DB_NODES_PUBLIC_IP", type=str_or_list,
             help=""),

        dict(name="loaders_private_ip", env="SCT_LOADERS_PRIVATE_IP", type=str_or_list,
             help=""),

        dict(name="loaders_public_ip", env="SCT_LOADERS_PUBLIC_IP", type=str_or_list,
             help=""),

        dict(name="monitor_nodes_private_ip", env="SCT_MONITOR_NODES_PRIVATE_IP", type=str_or_list,

             help=""),

        dict(name="monitor_nodes_public_ip", env="SCT_MONITOR_NODES_PUBLIC_IP", type=str_or_list,

             help=""),

        # openstack config options

        dict(name="openstack_user", env="SCT_OPENSTACK_USER", type=str,
             help=""),

        dict(name="openstack_password", env="SCT_OPENSTACK_PASSWORD", type=str,
             help=""),

        dict(name="openstack_tenant", env="SCT_OPENSTACK_TENANT", type=str,
             help=""),

        dict(name="openstack_auth_version", env="SCT_OPENSTACK_AUTH_VERSION", type=str,
             help=""),

        dict(name="openstack_auth_url", env="SCT_OPENSTACK_AUTH_URL", type=str,
             help=""),

        dict(name="openstack_service_type", env="SCT_OPENSTACK_SERVICE_TYPE", type=str,
             help=""),

        dict(name="openstack_service_name", env="SCT_OPENSTACK_SERVICE_NAME", type=str,
             help=""),

        dict(name="openstack_service_region", env="SCT_OPENSTACK_SERVICE_REGION", type=str,
             help=""),

        dict(name="openstack_instance_type_loader", env="SCT_OPENSTACK_INSTANCE_TYPE_LOADER", type=str,
             help=""),

        dict(name="openstack_instance_type_db", env="SCT_OPENSTACK_INSTANCE_TYPE_DB", type=str,
             help=""),

        dict(name="openstack_instance_type_monitor", env="SCT_OPENSTACK_INSTANCE_TYPE_MONITOR", type=str,
             help=""),

        dict(name="openstack_image", env="SCT_OPENSTACK_IMAGE", type=str,
             help=""),

        dict(name="openstack_image_username", env="SCT_OPENSTACK_IMAGE_USERNAME", type=str,
             help=""),

        dict(name="openstack_network", env="SCT_OPENSTACK_NETWORK", type=str,
             help=""),


        # test specific config parameters

        # GrowClusterTest
        dict(name="cassandra_stress_population_size", env="SCT_CASSANDRA_STRESS_POPULATION_SIZE", type=int,
             help=""),
        dict(name="cassandra_stress_threads", env="SCT_CASSANDRA_STRESS_THREADS", type=int,
             help=""),
        dict(name="add_node_cnt", env="SCT_ADD_NODE_CNT", type=int,
             help=""),

        # LongevityTest
        dict(name="stress_multiplier", env="SCT_STRESS_MULTIPLIER", type=int,
             help=""),
        dict(name="run_fullscan", env="SCT_RUN_FULLSCAN", type=str,
             help=""),
        dict(name="keyspace_num", env="SCT_KEYSPACE_NUM", type=int,
             help=""),
        dict(name="round_robin", env="SCT_ROUND_ROBIN", type=boolean,
             help=""),
        dict(name="batch_size", env="SCT_BATCH_SIZE", type=int,
             help=""),
        dict(name="pre_create_schema", env="SCT_PRE_CREATE_SCHEMA", type=boolean,
             help=""),

        dict(name="compaction_strategy", env="SCT_COMPACTION_STRATEGY", type=str,
             help="Choose a specific compaction strategy to pre-create schema with."),

        dict(name="cluster_health_check", env="SCT_CLUSTER_HEALTH_CHECK", type=boolean,
             help="When true, start cluster health checker for all nodes"),

        dict(name="validate_partitions", env="SCT_VALIDATE_PARTITIONS", type=boolean,
             help="when true, log of the partitions before and after the nemesis run is compacted"),
        dict(name="table_name", env="SCT_TABLE_NAME", type=str,
             help="table name to check for the validate_partitions check"),
        dict(name="primary_key_column", env="SCT_PRIMARY_KEY_COLUMN", type=str,
             help="primary key of the table to check for the validate_partitions check"),

        dict(name="stress_read_cmd", env="SCT_STRESS_READ_CMD", type=str_or_list,
             help="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure.
                multiple commands can passed as a list"""),

        dict(name="prepare_verify_cmd", env="SCT_PREPARE_VERIFY_CMD", type=str_or_list,
             help="""cassandra-stress commands.
            You can specify everything but the -node parameter, which is going to
            be provided by the test suite infrastructure.
            multiple commands can passed as a list"""),

        dict(name="user_profile_table_count", env="SCT_USER_PROFILE_TABLE_COUNT", type=int,
             help="""number of tables to create for template user c-s"""),

        # MgmtCliTest
        dict(name="scylla_mgmt_upgrade_to_repo", env="SCT_SCYLLA_MGMT_UPGRADE_TO_REPO", type=str,
             help="Url to the repo of scylla manager version to upgrade to for management tests"),

        # PerformanceRegressionTest
        dict(name="stress_cmd_w", env="SCT_STRESS_CMD_W", type=str_or_list,
             help="""cassandra-stress commands.
                    You can specify everything but the -node parameter, which is going to
                    be provided by the test suite infrastructure.
                    multiple commands can passed as a list"""),

        dict(name="stress_cmd_r", env="SCT_STRESS_CMD_R", type=str_or_list,
             help="""cassandra-stress commands.
                    You can specify everything but the -node parameter, which is going to
                    be provided by the test suite infrastructure.
                    multiple commands can passed as a list"""),

        dict(name="stress_cmd_m", env="SCT_STRESS_CMD_M", type=str_or_list,
             help="""cassandra-stress commands.
                    You can specify everything but the -node parameter, which is going to
                    be provided by the test suite infrastructure.
                    multiple commands can passed as a list"""),

        dict(name="prepare_write_cmd", env="SCT_PREPARE_WRITE_CMD", type=str_or_list,
             help="""cassandra-stress commands.
                    You can specify everything but the -node parameter, which is going to
                    be provided by the test suite infrastructure.
                    multiple commands can passed as a list"""),

        dict(name="stress_cmd_no_mv", env="SCT_STRESS_CMD_NO_MV", type=str_or_list,
             help="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure.
                multiple commands can passed as a list"""),

        dict(name="stress_cmd_no_mv_profile", env="SCT_STRESS_CMD_NO_MV_PROFILE", type=str,
             help=""),

        # PerformanceRegressionUserProfilesTest
        dict(name="cs_user_profiles", env="SCT_CS_USER_PROFILES", type=str_or_list,
             help=""),
        dict(name="cs_duration", env="SCT_CS_DURATION", type=str,
             help=""),

        dict(name="stress_cmd_mv", env="SCT_STRESS_CMD_MV", type=str_or_list,
             help="""cassandra-stress commands.
            You can specify everything but the -node parameter, which is going to
            be provided by the test suite infrastructure.
            multiple commands can passed as a list"""),

        dict(name="prepare_stress_cmd", env="SCT_PREPARE_STRESS_CMD", type=str_or_list,
             help="""cassandra-stress commands.
            You can specify everything but the -node parameter, which is going to
            be provided by the test suite infrastructure.
            multiple commands can passed as a list"""),

        # RefreshTest
        dict(name="skip_download", env="SCT_SKIP_DOWNLOAD", type=boolean,
             help=""),
        dict(name="sstable_file", env="SCT_SSTABLE_FILE", type=str,
             help=""),
        dict(name="sstable_url", env="SCT_SSTABLE_URL", type=str,
             help=""),
        dict(name="sstable_md5", env="SCT_SSTABLE_MD5", type=str,
             help=""),
        dict(name="flush_times", env="SCT_FLUSH_TIMES", type=int,
             help=""),
        dict(name="flush_period", env="SCT_FLUSH_PERIOD", type=int,
             help=""),

        # UpgradeTest
        dict(name="new_scylla_repo", env="SCT_NEW_SCYLLA_REPO", type=str,
             help=""),

        dict(name="new_version", env="SCT_NEW_VERSION", type=str,
             help="Assign new upgrade version, use it to upgrade to specific minor release. eg: 3.0.1"),
        dict(name="target_upgrade_version", env="SCT_TAGRET_UPGRADE_VERSION", type=str,
             help="Assign target upgrade version, use for decide if the truncate entries test should be run. "
                  "This test should be performed in case the target upgrade version >= 3.1"),
        dict(name="upgrade_node_packages", env="SCT_UPGRADE_NODE_PACKAGES", type=int,
             help=""),

        dict(name="test_sst3", env="SCT_TEST_SST3", type=boolean,
             help=""),

        dict(name="test_upgrade_from_installed_3_1_0", env="SCT_TEST_UPGRADE_FROM_INSTALLED_3_1_0", type=boolean,
             help="Enable an option for installed 3.1.0 for work around a scylla issue if it's true"),

        dict(name="authorization_in_upgrade", env="SCT_AUTHORIZATION_IN_UPGRADE", type=str,
             help="Which Authorization to enable after upgrade"),

        dict(name="remove_authorization_in_rollback", env="SCT_REMOVE_AUTHORIZATION_IN_ROLLBACK", type=boolean,
             help="Disable Authorization after rollback to old Scylla"),

        dict(name="new_introduced_pkgs", env="SCT_NEW_INTRODUCED_PKGS", type=str,
             help=""),

        dict(name="recover_system_tables", env="SCT_RECOVER_SYSTEM_TABLES", type=boolean,
             help=""),

        dict(name="stress_cmd_1", env="SCT_STRESS_CMD_1", type=str_or_list,
             help="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure.
                multiple commands can passed as a list"""),

        dict(name="stress_cmd_complex_prepare", env="SCT_STRESS_CMD_COMPLEX_PREPARE", type=str_or_list,
             help="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure.
                multiple commands can passed as a list"""),

        dict(name="prepare_write_stress", env="SCT_PREPARE_WRITE_STRESS", type=str_or_list,

             help="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure.
                multiple commands can passed as a list"""),

        dict(name="stress_cmd_read_10m", env="SCT_STRESS_CMD_READ_10M", type=str_or_list,

             help="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure.
                multiple commands can passed as a list"""),

        dict(name="stress_cmd_read_cl_one", env="SCT_STRESS_CMD_READ_CL_ONE", type=str_or_list,
             help="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure."""),

        dict(name="stress_cmd_read_80m", env="SCT_STRESS_CMD_READ_80M", type=str_or_list,

             help="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure.
                multiple commands can passed as a list"""),

        dict(name="stress_cmd_complex_verify_read", env="SCT_STRESS_CMD_COMPLEX_VERIFY_READ", type=str_or_list,
             help="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure.
                multiple commands can passed as a list"""),

        dict(name="stress_cmd_complex_verify_more", env="SCT_STRESS_CMD_COMPLEX_VERIFY_MORE", type=str_or_list,
             help="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure.
                multiple commands can passed as a list"""),

        dict(name="write_stress_during_entire_test", env="SCT_WRITE_STRESS_DURING_ENTIRE_TEST", type=str_or_list,
             help="""cassandra-stress commands.
                    You can specify everything but the -node parameter, which is going to
                    be provided by the test suite infrastructure.
                    multiple commands can passed as a list"""),

        dict(name="verify_data_after_entire_test", env="SCT_VERIFY_DATA_AFTER_ENTIRE_TEST", type=str_or_list,
             help="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure."""),

        dict(name="stress_cmd_read_cl_quorum", env="SCT_STRESS_CMD_READ_CL_QUORUM", type=str_or_list,
             help="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure.
                multiple commands can passed as a list"""),

        dict(name="verify_stress_after_cluster_upgrade", env="SCT_VERIFY_STRESS_AFTER_CLUSTER_UPGRADE", type=str_or_list,
             help="""cassandra-stress commands.
            You can specify everything but the -node parameter, which is going to
            be provided by the test suite infrastructure.
            multiple commands can passed as a list"""),

        dict(name="stress_cmd_complex_verify_delete", env="SCT_STRESS_CMD_COMPLEX_VERIFY_DELETE",
             type=str_or_list,
             help="""cassandra-stress commands.
                    You can specify everything but the -node parameter, which is going to
                    be provided by the test suite infrastructure.
                    multiple commands can passed as a list"""),

        dict(name="scylla_encryption_options", env="SCT_SCYLLA_ENCRYPTION_OPTIONS", type=str_or_list,
             help="options will be used for enable encryption at-rest for tables"),

        dict(name="logs_transport", env="SCT_LOGS_TRANSPORT", type=str,
             help="How to transport logs: rsyslog or ssh", choices=("rsyslog", "ssh")),

        dict(name="collect_logs", env="SCT_COLLECT_LOGS", type=boolean,
             help="Collect logs from instances and sct runner"),

        dict(name="execute_post_behavior", env="SCT_EXECUTE_POST_BEHAVIOR", type=boolean,
             help="Run post behavior actions in sct teardown step"),

        dict(name="post_behavior_db_nodes", env="SCT_POST_BEHAVIOR_DB_NODES", type=str,
             help="""
                Failure/post test behavior, i.e. what to do with the db cloud instances at the end of the test.

                'destroy' - Destroy instances and credentials (default)
                'keep' - Keep instances running and leave credentials alone
                'keep-on-failure' - Keep instances if testrun failed
             """,
             choices=("keep", "keep-on-failure", "destroy")),

        dict(name="post_behavior_loader_nodes", env="SCT_POST_BEHAVIOR_LOADER_NODES", type=str,
             help="""
                Failure/post test behavior, i.e. what to do with the loader cloud instances at the end of the test.

                'destroy' - Destroy instances and credentials (default)
                'keep' - Keep instances running and leave credentials alone
                'keep-on-failure' - Keep instances if testrun failed
             """,
             choices=("keep", "keep-on-failure", "destroy")),

        dict(name="post_behavior_monitor_nodes", env="SCT_POST_BEHAVIOR_MONITOR_NODES", type=str,
             help="""
                Failure/post test behavior, i.e. what to do with the monitor cloud instances at the end of the test.

                'destroy' - Destroy instances and credentials (default)
                'keep' - Keep instances running and leave credentials alone
                'keep-on-failure' - Keep instances if testrun failed
             """,
             choices=("keep", "keep-on-failure", "destroy")),
        dict(name="workaround_kernel_bug_for_iotune", env="SCT_WORKAROUND_KERNEL_BUG_FOR_IOTUNE", type=bool,
             help="Workaround a known kernel bug which causes iotune to fail in scylla_io_setup, only effect GCE backend")
    ]

    required_params = ['cluster_backend', 'test_duration', 'n_db_nodes', 'n_loaders', 'user_credentials_path']

    # those can be added to a json scheme to validate / or write the validation code for it to be a bit clearer output
    backend_required_params = {
        'aws':  ['user_prefix', "instance_type_loader", "instance_type_monitor", "instance_type_db",
                 "region_name", "security_group_ids", "subnet_id", "ami_id_db_scylla", "ami_id_loader",
                 "ami_id_monitor", "aws_root_disk_size_monitor", "aws_root_disk_name_monitor",  "aws_root_disk_size_loader", "ami_db_scylla_user",
                 "ami_monitor_user"],

        'gce': ['user_prefix', 'gce_network', 'gce_image', 'gce_image_username', 'gce_instance_type_db', 'gce_root_disk_type_db',
                'gce_root_disk_size_db', 'gce_n_local_ssd_disk_db', 'gce_instance_type_loader',
                'gce_root_disk_type_loader', 'gce_n_local_ssd_disk_loader', 'gce_instance_type_monitor',
                'gce_root_disk_type_monitor', 'gce_root_disk_size_monitor', 'gce_n_local_ssd_disk_monitor',
                'gce_datacenter', 'scylla_repo'],

        'docker': ['docker_image', 'user_credentials_path', 'scylla_repo'],

        'libvirt':  ['libvirt_uri', 'libvirt_bridge', 'scylla_repo'],

        'baremetal': ['db_nodes_private_ip', 'db_nodes_public_ip', 'user_credentials_path'],

        'openstack': ['openstack_user', 'openstack_password', 'openstack_tenant', 'openstack_auth_version',
                      'openstack_auth_url', 'openstack_service_type', 'openstack_service_name', 'openstack_service_region',
                      'openstack_instance_type_loader', 'openstack_instance_type_db', 'openstack_instance_type_monitor',
                      'openstack_image', 'openstack_image_username', 'openstack_network'],
        'aws-siren': ["user_prefix", "instance_type_loader", "region_name", "security_group_ids", "subnet_id",
                      "cloud_credentials_path", "authenticator_user", "authenticator_password", "db_nodes_public_ip",
                      "db_nodes_private_ip", "nemesis_filter_seeds"]
    }

    defaults_config_files = {
        "aws": [sct_abs_path('defaults/aws_config.yaml')],
        "gce": [sct_abs_path('defaults/gce_config.yaml')],
        "docker": [sct_abs_path('defaults/docker_config.yaml')],
        "libvirt": [sct_abs_path('defaults/libvirt_config.yaml')],
        "baremetal": [sct_abs_path('defaults/baremetal_config.yaml')],
        "openstack": [sct_abs_path('defaults/openstack_config.yaml')],
        "aws-siren": [sct_abs_path('defaults/aws_config.yaml')]
    }

    multi_region_params = [
        'region_name', 'n_db_nodes', 'security_group_ids', 'subnet_id', 'ami_id_db_scylla', 'ami_id_loader', 'ami_id_monitor'
    ]

    def __init__(self):
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        super(SCTConfiguration, self).__init__()

        env = self._load_environment_variables()
        config_files = env.get('config_files', [])
        config_files = [sct_abs_path(f) for f in config_files]

        # prepend to the config list the defaults the config files
        backend = env.get('cluster_backend')
        backend_config_files = [sct_abs_path('defaults/test_default.yaml')]
        if backend:
            backend_config_files += self.defaults_config_files[str(backend)]

        # 1) load the default backend config files
        files = anyconfig.load(list(backend_config_files))
        anyconfig.merge(self, files)

        regions_data = self.get('regions_data', {})
        if regions_data:
            del self['regions_data']

        # 2) load the config files
        files = anyconfig.load(list(config_files))
        anyconfig.merge(self, files)

        # 2.2) load the region data
        region_names = self.get('region_name', '').split()
        region_names = env.get('region_name', region_names)

        cluster_backend = self.get('cluster_backend')
        cluster_backend = env.get('cluster_backend', cluster_backend)

        if 'aws' in cluster_backend:
            for region in region_names:
                for key, value in regions_data[region].items():
                    if key not in self.keys():
                        self[key] = value
                    elif len(self[key].split()) < len(region_names):
                        self[key] += " {}".format(value)

        # 3) overwrite with environment variables
        anyconfig.merge(self, env)

        # 4) assume multi dc by n_db_nodes set size
        if 'aws' in cluster_backend:
            num_of_regions = len(self.get('region_name', '').split())
            num_of_db_nodes_sets = len(str(self.get('n_db_nodes', '')).split(' '))
            if num_of_db_nodes_sets > num_of_regions:
                for region in regions_data.keys()[:num_of_db_nodes_sets]:
                    for key, value in regions_data[region].items():
                        if key not in self.keys():
                            self[key] = value
                        else:
                            self[key] += " {}".format(value)

        # 5) handle scylla_version if exists
        scylla_version = self.get('scylla_version', None)
        scylla_linux_distro = self.get('scylla_linux_distro', '')
        dist_type = scylla_linux_distro.split('-')[0]
        dist_version = scylla_linux_distro.split('-')[-1]

        if scylla_version:
            # Look for the version, and return it's info ami + repo
            # According to backend, populate 'scylla_repo' or 'ami_id_db_scylla'
            if 'ami_id_db_scylla' not in self and self.get('cluster_backend') == 'aws':
                ami_list = []
                for region in self.get('region_name').split():
                    if ':' in scylla_version:
                        amis = get_branched_ami(scylla_version, region_name=region)
                        ami_list.append(amis[0].id)
                        continue

                    amis = get_scylla_ami_versions(region)
                    for ami in amis:
                        if scylla_version in ami['Name']:
                            ami_list.append(ami['ImageId'])
                            break
                    else:
                        raise ValueError("AMI for scylla version {} wasn't found".format(scylla_version))
                self['ami_id_db_scylla'] = " ".join(ami_list)
            elif 'scylla_repo' not in self:
                repo_map = get_s3_scylla_repos_mapping(dist_type, dist_version)

                for key in repo_map.keys():
                    if scylla_version.startswith(key):
                        self['scylla_repo'] = repo_map[key]
                        break
                else:
                    raise ValueError("repo for scylla version {} wasn't found".format(scylla_version))
            else:
                raise ValueError("'scylla_version' can't used together with  'ami_id_db_scylla' or with 'scylla_repo'")

            if 'scylla_repo_loader' not in self and ':' not in scylla_version:
                scylla_linux_distro_loader = self.get('scylla_linux_distro_loader', '')
                dist_type_loader = scylla_linux_distro_loader.split('-')[0]
                dist_version_loader = scylla_linux_distro_loader.split('-')[-1]

                repo_map = get_s3_scylla_repos_mapping(dist_type_loader, dist_version_loader)

                for key in repo_map.keys():
                    if scylla_version.startswith(key):
                        self['scylla_repo_loader'] = repo_map[key]
                        break
                else:
                    raise ValueError("repo for scylla version {} wasn't found in []".format(scylla_version))

        # 6) support lookup of repos for upgrade test
        new_scylla_version = self.get('new_version', None)
        if new_scylla_version:
            if 'ami_id_db_scylla' not in self and cluster_backend == 'aws':
                raise ValueError("'new_version' isn't supported for AWS AMIs")

            elif 'new_scylla_repo' not in self:
                repo_map = get_s3_scylla_repos_mapping(dist_type, dist_version)

                for key in repo_map.keys():
                    if scylla_version.startswith(key):
                        self['new_scylla_repo'] = repo_map[key]
                        break
                else:
                    raise ValueError("repo for scylla version {} wasn't found".format(new_scylla_version))

        # 7) append username or ami_id_db_scylla_desc to the user_prefix
        version_tag = self.get('ami_id_db_scylla_desc', default=None)
        user_prefix = self.get('user_prefix', default=None)
        if user_prefix:
            if not version_tag:
                version_tag = getpass.getuser()

            self['user_prefix'] = "{}-{}".format(user_prefix, version_tag)[:35]

        # 8) update target_upgrade_version automatically
        new_scylla_repo = self.get('new_scylla_repo', None)
        if new_scylla_repo and 'target_upgrade_version' not in self:
            self['target_upgrade_version'] = get_branch_version(new_scylla_repo)
        LOGGER.info(self.dump_config())

        # 9) instance_provision MIXED is not supported
        if self.get('instance_provision') == 'mixed':
            LOGGER.warning('Selected instance_provision type "MIXED" is not supported!')

    @classmethod
    def get_config_option(cls, name):
        return [o for o in cls.config_options if o['name'] == name][0]

    def get_default_value(self, key, include_backend=False):

        default_config_files = [sct_abs_path('defaults/test_default.yaml')]
        backend = self['cluster_backend']
        if backend and include_backend:
            default_config_files += self.defaults_config_files[str(backend)]

        return anyconfig.load(list(default_config_files)).get(key, None)

    def _load_environment_variables(self):
        environment_vars = {}
        for opt in self.config_options:
            if opt['env'] in os.environ:
                try:
                    environment_vars[opt['name']] = opt['type'](os.environ[opt['env']])
                except Exception as ex:
                    raise ValueError("failed to parse {} from environment variable:\n\t\t{}".format(opt['env'], ex))
        return environment_vars

    def get(self, key, default=UnsetMarker):
        """
        get a specific configuration by key
        :param key: the key to get
        :param default: default value if the key doesn't exist
        :return: int / str / list
        """

        if default is not UnsetMarker:
            ret_val = super(SCTConfiguration, self).get(key, default)
        else:
            ret_val = super(SCTConfiguration, self).get(key)

        if key in self.multi_region_params and isinstance(ret_val, list):
            ret_val = ' '.join(ret_val)

        return ret_val

    def _validate_value(self, opt):
        try:
            opt['type'](self.get(opt['name']))
        except Exception as ex:
            raise ValueError("failed to validate {}:\n\t\t{}".format(opt['name'], ex))
        choices = opt.get('choices')
        if choices:
            cur_val = self.get(opt['name'])
            assert cur_val in choices, "failed to validate '{}': {} not in {}".format(opt['name'], cur_val, choices)

    def verify_configuration(self):
        """
        Check that all required values are set, and validated each value to be of correct type or value
        also check required options per backend

        :return: None
        :raises ValueError: on failures in validations
        :raise Exception: on unsupported backends
        """

        # pylint: disable=too-many-locals,too-many-branches

        # check if there are SCT_* environment variable which aren't documented
        config_keys = set([opt['env'] for opt in self.config_options])
        env_keys = set([o for o in os.environ.keys() if o.startswith('SCT_') and o != 'SCT_NEW_CONFIG'])
        unknown_env_keys = (env_keys.difference(config_keys))
        if unknown_env_keys:
            output = ["{}={}".format(key, os.environ.get(key)) for key in unknown_env_keys]
            raise ValueError("Unsupported environment variables were used:\n\t - {}".format("\n\t - ".join(output)))

        # check for unsupported configuration
        config_names = set([o['name'] for o in self.config_options])
        unsupported_option = set(self.keys()).difference(config_names)

        if unsupported_option:
            res = "Unsupported config option/s found:\n"
            for option in unsupported_option:
                res += "\t * '{}: {}'\n".format(option, self[option])
            raise ValueError(res)

        # validate passed configuration
        for opt in self.config_options:
            if opt['name'] in self:
                self._validate_value(opt)

        # validated per backend
        def _check_backend_defaults(backend, required_params):
            opts = [o for o in self.config_options if o['name'] in required_params]
            for _opt in opts:
                assert _opt['name'] in self, "{} missing from config for {}".format(_opt['name'], backend)

        backend = self.get('cluster_backend')
        if backend in self.available_backends:
            _check_backend_defaults(backend, self.backend_required_params[backend])
        else:
            raise ValueError("Unsupported backend [{}]".format(backend))

        # verify multi-region aws params
        if backend in ['aws', 'aws-siren']:
            region_count = {}
            for opt in self.multi_region_params:
                val = self.get(opt)
                if isinstance(val, basestring):
                    region_count[opt] = len(self.get(opt).split())
                elif isinstance(val, list):
                    region_count[opt] = len(val)
                else:
                    region_count[opt] = 1
            if not all(region_count['region_name'] == x for x in region_count.values()):
                raise ValueError("not all multi region values are equal: \n\t{}".format(region_count))

        if 'aws_extra_network_interface' in self and len(self.get('region_name').split()) >= 2:
            raise ValueError("aws_extra_network_interface isn't supported for multi region use cases")

        # validate seeds number
        seeds_num = self.get('seeds_num')
        assert seeds_num > 0, "Seed number should be at least one"

        num_of_db_nodes = sum([int(i) for i in str(self.get('n_db_nodes')).split(' ')])
        assert seeds_num <= num_of_db_nodes, 'Seeds number ({}) should be not more then nodes number ({})'. \
            format(seeds_num, num_of_db_nodes)

    def dump_config(self):
        """
        Dump current configuration to string

        :return: str
        """
        return anyconfig.dumps(self, ac_parser="yaml")

    def dump_help_config_markdown(self):
        """
        Dump all configuration options with their defaults and help to string in markdown format

        :return: str
        """
        header = """
            # scylla-cluster-tests configuration options
            | Parameter | Description  | Default | Override environment<br>variable
            | :-------  | :----------  | :------ | :-------------------------------
        """

        def strip_help_text(text):
            """
            strip all lines, and also remove empty lines from start or end
            """
            output = [l.strip() for l in text.splitlines()]
            return '\n'.join(output[1 if not output[0] else 0:-1 if not output[-1] else None])

        ret = strip_help_text(header) + '\n'

        for opt in self.config_options:
            if opt['help']:
                help_text = '<br>'.join(strip_help_text(opt['help']).splitlines())
            else:
                help_text = ''

            default = self.get_default_value(opt['name'])
            default_text = default if default else 'N/A'
            ret += """| **<a name="{name}">{name}</a>**  | {help_text} | {default_text} | {env}\n""".format(
                help_text=help_text, default_text=default_text, **opt)

        return ret

    def dump_help_config_yaml(self):
        """
        Dump all configuration options with their defaults and help to string in yaml format

        :return: str
        """
        ret = ""
        for opt in self.config_options:
            if opt['help']:
                help_text = '\n'.join(["# {}".format(l.strip()) for l in opt['help'].splitlines() if l.strip()]) + '\n'
            else:
                help_text = ''
            default = self.get_default_value(opt['name'])
            default = default if default else 'N/A'
            ret += "{help_text}{name}: {default}\n\n".format(help_text=help_text, default=default, **opt)

        return ret
