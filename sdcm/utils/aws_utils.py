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
# Copyright (c) 2021 ScyllaDB

import time
import logging
from functools import cached_property
from typing import List, Dict, Literal

import boto3
from botocore.exceptions import ClientError

from sdcm.utils.decorators import retrying
from sdcm.utils.aws_region import AwsRegion
from sdcm.wait import wait_for
from sdcm.test_config import TestConfig

LOGGER = logging.getLogger(__name__)
ARM_ARCH_PREFIXES = ('im4', 'is4', 'a1.', 'inf', 'm6', 'c6', 'r6', 'm7', 'c7', 'r7')
AwsArchType = Literal['x86_64', 'arm64']


class EksClusterCleanupMixin:
    short_cluster_name: str
    region_name: str

    @cached_property
    def eks_client(self):
        return boto3.client('eks', region_name=self.region_name)

    @cached_property
    def ec2_client(self):
        return boto3.client('ec2', region_name=self.region_name)

    @property
    def owned_object_tag_name(self):
        return f'kubernetes.io/cluster/{self.short_cluster_name}'

    @cached_property
    def cluster_owned_objects_filter(self):
        return [{"Name": f"tag:{self.owned_object_tag_name}", 'Values': ['owned']}]

    @property
    def attached_security_group_ids(self) -> List[str]:
        return [group_desc['GroupId'] for group_desc in
                self.ec2_client.describe_security_groups(Filters=self.cluster_owned_objects_filter)['SecurityGroups']]

    @property
    def attached_nodegroup_names(self) -> List[str]:
        return self._get_attached_nodegroup_names()

    @property
    def failed_to_delete_nodegroup_names(self) -> List[str]:
        return self._get_attached_nodegroup_names(status='DELETE_FAILED')

    @property
    def deleting_nodegroup_names(self) -> List[str]:
        return self._get_attached_nodegroup_names(status='DELETING')

    def _get_attached_nodegroup_names(self, status: str = None) -> List[str]:
        if status is None:
            return self.eks_client.list_nodegroups(clusterName=self.short_cluster_name)['nodegroups']
        output = []
        for nodegroup_name in self.attached_nodegroup_names:
            if status == self.eks_client.describe_nodegroup(
                    clusterName=self.short_cluster_name, nodegroupName=nodegroup_name)['nodegroup']['status']:
                output.append(nodegroup_name)
        return output

    @property
    def cluster_exists(self) -> bool:
        if self.short_cluster_name in self.eks_client.list_clusters()['clusters']:
            return True
        return False

    def destroy(self):
        for _ in range(2):
            self.destroy_nodegroups()
            if self.failed_to_delete_nodegroup_names:
                self.destroy_nodegroups(status='DELETE_FAILED')
            self.destroy_cluster()
            # Destroying of the security groups will affect load balancers and node groups that is why
            # in order to do not distract load balancers cleaning process we should have
            # destroy_attached_security_groups performed after destroy_attached_load_balancers
            # but before retrying of the destroy_nodegroups
            self.destroy_attached_security_groups()
            if not self.cluster_exists:
                return

    def check_if_all_network_interfaces_detached(self, sg_id):
        for interface_description in self.ec2_client.describe_network_interfaces(
                Filters=[{'Name': 'group-id', 'Values': [sg_id]}])['NetworkInterfaces']:
            if attachment := interface_description.get('Attachment'):
                if attachment.get('AttachmentId'):
                    return False
        return True

    def delete_network_interfaces_of_sg(self, sg_id: str):
        network_interfaces = self.ec2_client.describe_network_interfaces(
            Filters=[{'Name': 'group-id', 'Values': [sg_id]}])['NetworkInterfaces']

        for interface_description in network_interfaces:
            network_interface_id = interface_description['NetworkInterfaceId']
            if attachment := interface_description.get('Attachment'):
                if attachment_id := attachment.get('AttachmentId'):
                    try:
                        self.ec2_client.detach_network_interface(AttachmentId=attachment_id, Force=True)
                    except Exception as exc:  # pylint: disable=broad-except
                        LOGGER.debug("Failed to detach network interface (%s) attachment %s:\n%s",
                                     network_interface_id, attachment_id, exc)

        wait_for(self.check_if_all_network_interfaces_detached, sg_id=sg_id, timeout=120, throw_exc=False)

        for interface_description in network_interfaces:
            network_interface_id = interface_description['NetworkInterfaceId']
            try:
                self.ec2_client.delete_network_interface(NetworkInterfaceId=network_interface_id)
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.debug("Failed to delete network interface %s :\n%s", network_interface_id, exc)

    def destroy_attached_security_groups(self):
        # EKS infra does not cleanup security group perfectly and some of them can be left alive
        # even when cluster is gone
        try:
            sg_list = self.attached_security_group_ids
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.debug("Failed to get list of security groups:\n%s", exc)
            return

        for security_group_id in sg_list:
            # EKS Nodegroup deletion can fail due to the network interfaces stuck in attached state
            # while instance is gone.
            # In this case you need to forcefully detach interfaces and delete them to make nodegroup deletion possible.
            try:
                self.delete_network_interfaces_of_sg(security_group_id)
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.debug("destroy_attached_security_groups: %s", exc)

            try:
                self.ec2_client.delete_security_group(GroupId=security_group_id)
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.debug("Failed to delete security groups %s, due to the following error:\n%s",
                             security_group_id, exc)

    def destroy_nodegroups(self, status=None):

        def _destroy_attached_nodegroups():
            for node_group_name in self._get_attached_nodegroup_names(status=status):
                try:
                    self.eks_client.delete_nodegroup(clusterName=self.short_cluster_name, nodegroupName=node_group_name)
                except Exception as exc:  # pylint: disable=broad-except
                    LOGGER.debug("Failed to delete nodegroup %s/%s, due to the following error:\n%s",
                                 self.short_cluster_name, node_group_name, exc)
            time.sleep(10)
            return wait_for(lambda: not self._get_attached_nodegroup_names(status='DELETING'),
                            text='Waiting till target nodegroups are deleted',
                            step=10,
                            timeout=300,
                            throw_exc=False)

        wait_for(_destroy_attached_nodegroups, timeout=400, throw_exc=False)

    def destroy_cluster(self):
        try:
            self.eks_client.delete_cluster(name=self.short_cluster_name)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.debug("Failed to delete cluster %s, due to the following error:\n%s",
                         self.short_cluster_name, exc)


def init_monitoring_info_from_params(monitor_info: dict, params: dict, regions: List):
    if monitor_info['n_nodes'] is None:
        monitor_info['n_nodes'] = params.get('n_monitor_nodes')
    if monitor_info['type'] is None:
        monitor_info['type'] = params.get('instance_type_monitor')
    if monitor_info['disk_size'] is None:
        monitor_info['disk_size'] = params.get('root_disk_size_monitor')
    if monitor_info['device_mappings'] is None:
        if monitor_info['disk_size']:
            monitor_info['device_mappings'] = [{
                "DeviceName": ec2_ami_get_root_device_name(image_id=params.get('ami_id_monitor').split()[0],
                                                           region=regions[0]),
                "Ebs": {
                    "VolumeSize": monitor_info['disk_size'],
                    "VolumeType": "gp2"
                }
            }]
        else:
            monitor_info['device_mappings'] = []
    return monitor_info


def init_db_info_from_params(db_info: dict, params: dict, regions: List, root_device: str = None):
    if db_info['n_nodes'] is None:
        n_db_nodes = params.get('n_db_nodes')
        if isinstance(n_db_nodes, int):  # legacy type
            db_info['n_nodes'] = [n_db_nodes]
        elif isinstance(n_db_nodes, str):  # latest type to support multiple datacenters
            db_info['n_nodes'] = [int(n) for n in n_db_nodes.split()]
        else:
            raise RuntimeError(f"Unsupported parameter type: {type(n_db_nodes)}")
    if db_info['type'] is None:
        db_info['type'] = params.get('instance_type_db')
    if db_info['disk_size'] is None:
        db_info['disk_size'] = params.get('root_disk_size_db')
    if db_info['device_mappings'] is None and (root_device or params.get('ami_id_db_scylla')):
        if db_info['disk_size']:
            root_device = root_device if root_device else ec2_ami_get_root_device_name(
                image_id=params.get('ami_id_db_scylla').split()[0],
                region=regions[0])
            db_info['device_mappings'] = [{
                "DeviceName": root_device,
                "Ebs": {
                    "VolumeSize": db_info['disk_size'],
                    "VolumeType": "gp2"
                }
            }]
        else:
            db_info['device_mappings'] = []

        additional_ebs_volumes_num = params.get("data_volume_disk_num")
        if additional_ebs_volumes_num > 0:
            ebs_info = {"DeleteOnTermination": True,
                        "VolumeType": params.get("data_volume_disk_type"),
                        "VolumeSize": params.get('data_volume_disk_size')}

            if ebs_info['VolumeType'] in ['io1', 'io2', 'gp3']:
                ebs_info["Iops"] = params.get('data_volume_disk_iops')

            for disk_char in "fghijklmnop"[:additional_ebs_volumes_num]:
                ebs_volume = {
                    "DeviceName": f"/dev/xvd{disk_char}",
                    "Ebs": ebs_info
                }

                db_info['device_mappings'].append(ebs_volume)

        LOGGER.debug(db_info['device_mappings'])
    return db_info


def get_common_params(params: dict, regions: List, credentials: List, services: List, auto_availability_zone: bool = False) -> dict:
    ec2_security_group_ids, ec2_subnet_ids = get_ec2_network_configuration(
        regions=regions,
        availability_zones=params.get('availability_zone').split(','),
        params=params,
        auto_availability_zone=auto_availability_zone)
    LOGGER.info("[yg] get_ec2_network_configuration auto_az output: %s, %s ", ec2_security_group_ids, ec2_subnet_ids)
    return dict(ec2_security_group_ids=ec2_security_group_ids,
                ec2_subnet_id=ec2_subnet_ids,
                services=services,
                credentials=credentials,
                user_prefix=params.get('user_prefix'),
                params=params,
                )


def get_ec2_network_configuration(regions: list[str], availability_zones: list[str], params: dict,
                                  auto_availability_zone: bool = False):
    ec2_security_group_ids = []
    ec2_subnet_ids = []
    for region in regions:
        aws_region = AwsRegion(region_name=region)
        if auto_availability_zone:
            availability_zones = aws_region.availability_zones
            LOGGER.info("[yg] availability_zones: %s", availability_zones)
        else:
            availability_zones = [region + availability_zone for availability_zone in availability_zones]
        for availability_zone in availability_zones:
            sct_subnet = aws_region.sct_subnet(region_az=availability_zone)
            assert sct_subnet, f"No SCT subnet configured for {region}! Run 'hydra prepare-aws-region'"
            ec2_subnet_ids.append(sct_subnet.subnet_id)

            security_groups = []
            sct_sg = aws_region.sct_security_group
            assert sct_sg, f"No SCT security group configured for {region}! Run 'hydra prepare-aws-region'"
            security_groups.append(sct_sg.group_id)

            if params.get('intra_node_comm_public') or params.get('ip_ssh_connections') == 'public':
                test_config = TestConfig()
                test_id = test_config.test_id()

                test_sg = aws_region.provide_sct_test_security_group(test_id)
                security_groups.append(test_sg.group_id)

            ec2_security_group_ids.append(security_groups)

    return ec2_security_group_ids, ec2_subnet_ids


def get_ec2_services(regions):
    services = []
    for region in regions:
        session = boto3.session.Session(region_name=region)
        service = session.resource('ec2')
        services.append(service)
    return services


def tags_as_ec2_tags(tags: Dict[str, str]) -> List[Dict[str, str]]:
    return [{"Key": key, "Value": value} for key, value in tags.items()]


class PublicIpNotReady(Exception):
    pass


@retrying(n=90, sleep_time=10, allowed_exceptions=(PublicIpNotReady,),
          message="Waiting for instance to get public ip")
def ec2_instance_wait_public_ip(instance):
    instance.reload()
    if instance.public_ip_address is None:
        raise PublicIpNotReady(instance)
    LOGGER.debug("[%s] Got public ip: %s", instance, instance.public_ip_address)


def ec2_ami_get_root_device_name(image_id, region):
    ec2 = boto3.resource('ec2', region)
    image = ec2.Image(image_id)
    try:
        if image.root_device_name:
            return image.root_device_name
    except (TypeError, ClientError) as exc:
        raise AssertionError(f"Image '{image_id}' details not found in '{region}'") from exc
    return None


def get_arch_from_instance_type(instance_type: str) -> AwsArchType:
    if any((prefix in instance_type for prefix in ARM_ARCH_PREFIXES)):
        return 'arm64'
    return 'x86_64'
