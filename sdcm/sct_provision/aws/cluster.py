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

import abc
from functools import cached_property
from typing import List, Dict, Optional

from pydantic import BaseModel  # pylint: disable=no-name-in-module

from sdcm import cluster
from sdcm.provision.aws.instance_parameters import AWSInstanceParams
from sdcm.provision.aws.provisioner import AWSInstanceProvisioner
from sdcm.provision.aws.constants import MAX_SPOT_DURATION_TIME
from sdcm.provision.common.provision_plan import ProvisionPlan
from sdcm.provision.common.provision_plan_builder import ProvisionPlanBuilder
from sdcm.provision.common.provisioner import TagsType
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_provision.aws.instance_parameters_builder import ScyllaInstanceParamsBuilder, \
    LoaderInstanceParamsBuilder, MonitorInstanceParamsBuilder, OracleScyllaInstanceParamsBuilder
from sdcm.sct_provision.aws.user_data import ScyllaUserDataBuilder, AWSInstanceUserDataBuilder
from sdcm.sct_provision.common.utils import INSTANCE_PROVISION_SPOT, INSTANCE_PROVISION_SPOT_FLEET
from sdcm.test_config import TestConfig


class ClusterNode(BaseModel):
    parent_cluster: 'ClusterBase' = None
    region_id: int
    node_num: int
    node_name_prefix: str

    @property
    def name(self):
        return self.node_name_prefix + '-' + str(self.node_num)

    @property
    def tags(self) -> Dict[str, str]:
        return self.parent_cluster.tags | {'NodeIndex': str(self.node_num)}


class ClusterBase(BaseModel):
    params: SCTConfiguration
    test_id: str
    common_tags: TagsType
    _NODE_TYPE = None
    _NODE_PREFIX = None
    _INSTANCE_TYPE_PARAM_NAME = None
    _NODE_NUM_PARAM_NAME = None
    _INSTANCE_PARAMS_BUILDER = None
    _USER_PARAM = None

    @property
    def _provisioner(self):
        return AWSInstanceProvisioner()

    @property
    def nodes(self):
        nodes = []
        node_num = 0
        for region_id in range(len(self._regions_with_nodes)):
            for _ in range(self._node_nums[region_id]):
                node_num += 1
                nodes.append(
                    ClusterNode(
                        parent_cluster=self,
                        node_num=node_num,
                        region_id=region_id,
                        node_name_prefix=self._node_prefix,
                    )
                )
        return nodes

    @property
    def _test_config(self):
        return TestConfig()

    @property
    def _cluster_postfix(self):
        return self._NODE_PREFIX + '-cluster'

    @property
    def _node_postfix(self):
        return self._NODE_PREFIX + '-node'

    @property
    def _user_prefix(self):
        return self.params.get('user_prefix')

    @property
    def cluster_name(self):
        return '%s-%s' % (cluster.prepend_user_prefix(self._user_prefix, self._cluster_postfix), self._short_id)

    @property
    def _node_prefix(self):
        return '%s-%s' % (cluster.prepend_user_prefix(self._user_prefix, self._node_postfix), self._short_id)

    @property
    def _short_id(self):
        return str(self.test_id)[:8]

    @property
    def tags(self):
        return self.common_tags | {"NodeType": str(self._NODE_TYPE), "UserName": self.params.get(self._USER_PARAM)}

    def _node_tags(self, region_id: int) -> List[TagsType]:
        return [node.tags for node in self.nodes if node.region_id == region_id]

    def _node_names(self, region_id: int) -> List[str]:
        return [node.name for node in self.nodes if node.region_id == region_id]

    @property
    def _instance_provision(self):
        instance_provision = self.params.get('instance_provision')
        return INSTANCE_PROVISION_SPOT if instance_provision == INSTANCE_PROVISION_SPOT_FLEET else instance_provision

    @property
    @abc.abstractmethod
    def _user_data(self) -> str:
        pass

    @cached_property
    def _regions(self) -> List[str]:
        return self.params.region_names

    @cached_property
    def _regions_with_nodes(self) -> List[str]:
        output = []
        for region_id, region_name in enumerate(self.params.region_names):
            if len(self._node_nums) <= region_id:
                continue
            if self._node_nums[region_id] > 0:
                output.append(region_name)
        return output

    def _region(self, region_id: int) -> str:
        return self.params.region_names[region_id]

    @cached_property
    def _azs(self) -> str:
        return self.params.get('availability_zone').split(',')

    @cached_property
    def _node_nums(self) -> List[int]:
        node_nums = self.params.get(self._NODE_NUM_PARAM_NAME)
        if isinstance(node_nums, list):
            return [int(num) for num in node_nums]
        if isinstance(node_nums, int):
            return [node_nums]
        if isinstance(node_nums, str):
            return [int(num) for num in node_nums.split()]
        raise ValueError('Unexpected value of %s parameter' % (self._NODE_NUM_PARAM_NAME,))

    @property
    def _instance_type(self) -> str:
        return self.params.get(self._INSTANCE_TYPE_PARAM_NAME)

    @property
    def _test_duration(self) -> int:
        return self.params.get('test_duration')

    @property
    def _spot_duration(self) -> Optional[int]:
        duration = self._test_duration // 60 * 60 + 60
        if duration >= MAX_SPOT_DURATION_TIME:
            return None
        return duration

    def _az(self, region_id: int) -> str:
        if len(self._azs) == 1:
            return self._azs[0]
        return self._azs[region_id]

    def _spot_low_price(self, region_id: int) -> float:
        from sdcm.utils.pricing import AWSPricing  # pylint: disable=import-outside-toplevel

        aws_pricing = AWSPricing()
        on_demand_price = float(aws_pricing.get_on_demand_instance_price(
            region_name=self._region(region_id),
            instance_type=self._instance_type,
        ))
        return on_demand_price * self.params.get('spot_max_price')

    def provision_plan(self, region_id: int) -> ProvisionPlan:
        if not self.params.get('auto_availability_zone'):
            return ProvisionPlanBuilder(
                initial_provision_type=self._instance_provision,
                duration=self._test_duration,
                fallback_provision_on_demand=self.params.get('instance_provision_fallback_on_demand'),
                region_name=self._region(region_id),
                availability_zone=self._az(region_id),
                spot_low_price=self._spot_low_price(region_id),
                provisioner=AWSInstanceProvisioner(),
            ).provision_plan
        return ProvisionPlanBuilder(
            initial_provision_type=self._instance_provision,
            duration=self._test_duration,
            fallback_provision_on_demand=self.params.get('instance_provision_fallback_on_demand'),
            region_name=self._region(region_id),
            spot_low_price=self._spot_low_price(region_id),
            provisioner=AWSInstanceProvisioner(),
        ).provision_plan

    def _instance_parameters(self, region_id: int) -> AWSInstanceParams:
        params_builder = self._INSTANCE_PARAMS_BUILDER(  # pylint: disable=not-callable
            params=self.params,
            region_id=region_id,
            user_data_raw=self._user_data
        )
        return AWSInstanceParams(**params_builder.dict(exclude_none=True, exclude_unset=True, exclude_defaults=True))

    def provision(self):
        if self._node_nums == [0]:
            return []
        total_instances_provisioned = []
        for region_id in range(len(self._regions_with_nodes)):
            instance_parameters = self._instance_parameters(region_id=region_id)
            node_tags = self._node_tags(region_id=region_id)
            node_names = self._node_names(region_id=region_id)
            node_count = self._node_nums[region_id]
            instances = self.provision_plan(region_id).provision_instances(
                instance_parameters=instance_parameters,
                node_tags=node_tags,
                node_names=node_names,
                node_count=node_count
            )
            if not instances:
                raise RuntimeError('End of provision plan reached, but no instances provisioned')
            total_instances_provisioned.extend(instances)
        return total_instances_provisioned


class DBCluster(ClusterBase):
    _NODE_TYPE = 'scylla-db'
    _NODE_PREFIX = 'db'
    _INSTANCE_TYPE_PARAM_NAME = 'instance_type_db'
    _NODE_NUM_PARAM_NAME = 'n_db_nodes'
    _INSTANCE_PARAMS_BUILDER = ScyllaInstanceParamsBuilder
    _USER_PARAM = 'ami_db_scylla_user'

    @property
    def _user_data(self) -> str:
        return ScyllaUserDataBuilder(
            params=self.params,
            cluster_name=self.cluster_name,
            user_data_format_version=self.params.get('user_data_format_version'),
            syslog_host_port=self._test_config.get_logging_service_host_port(),
        ).to_string()


class OracleDBCluster(ClusterBase):
    _NODE_TYPE = 'oracle-db'
    _NODE_PREFIX = 'oracle'
    _INSTANCE_TYPE_PARAM_NAME = 'instance_type_db'
    _NODE_NUM_PARAM_NAME = 'n_test_oracle_db_nodes'
    _INSTANCE_PARAMS_BUILDER = OracleScyllaInstanceParamsBuilder
    _USER_PARAM = 'ami_db_scylla_user'

    @property
    def _user_data(self) -> str:
        return ScyllaUserDataBuilder(
            params=self.params,
            cluster_name=self.cluster_name,
            user_data_format_version=self.params.get('oracle_user_data_format_version'),
            syslog_host_port=self._test_config.get_logging_service_host_port(),
        ).to_string()


class LoaderCluster(ClusterBase):
    _NODE_TYPE = 'loader'
    _NODE_PREFIX = 'loader'
    _INSTANCE_TYPE_PARAM_NAME = 'instance_type_loader'
    _NODE_NUM_PARAM_NAME = 'n_loaders'
    _INSTANCE_PARAMS_BUILDER = LoaderInstanceParamsBuilder
    _USER_PARAM = 'ami_loader_user'

    @property
    def _user_data(self) -> str:
        return AWSInstanceUserDataBuilder(
            params=self.params,
            syslog_host_port=self._test_config.get_logging_service_host_port(),
        ).to_string()


class MonitoringCluster(ClusterBase):
    _NODE_TYPE = 'monitor'
    _NODE_PREFIX = 'monitor'
    _INSTANCE_TYPE_PARAM_NAME = 'instance_type_monitor'
    _NODE_NUM_PARAM_NAME = 'n_monitor_nodes'
    _INSTANCE_PARAMS_BUILDER = MonitorInstanceParamsBuilder
    _USER_PARAM = 'ami_monitor_user'

    @property
    def _user_data(self) -> str:
        return AWSInstanceUserDataBuilder(
            params=self.params,
            syslog_host_port=self._test_config.get_logging_service_host_port(),
        ).to_string()


ClusterNode.update_forward_refs()
