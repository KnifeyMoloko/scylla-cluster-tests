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
# Copyright (c) 2022 ScyllaDB
import json
import logging
from typing import Dict, List
from sdcm.provision.provisioner import Provisioner, InstanceDefinition, VmInstance, PricingModel
from sdcm.utils.gce_utils import get_gce_service

LOGGER = logging.getLogger(__name__)


class GCEProvisioner(Provisioner):
    """Provides API for VM provisioning in GCE"""

    def __init__(self, test_id, region):
        super().__init__(test_id, region)
        self._gce_service = get_gce_service(region=self.region)

    @classmethod
    def discover_regions(cls, test_id) -> List["Provisioner"]:
        pass

    def get_or_create_instance(self,
                               definition: InstanceDefinition,
                               pricing_model: PricingModel = PricingModel.SPOT) -> VmInstance:
        """
        common_params = dict(gce_image_username=self.params.get('gce_image_username'),
                             gce_network=self.params.get('gce_network'),
                             credentials=self.credentials,
                             user_prefix=user_prefix,
                             params=self.params,
                             gce_datacenter=gce_datacenter,
                             )
        ScyllaGCECluster(gce_image=gce_image_db,
                                               gce_image_type=db_info['disk_type'],
                                               gce_image_size=db_info['disk_size'],
                                               gce_n_local_ssd=db_info['n_local_ssd'],
                                               gce_instance_type=db_info['type'],
                                               services=services,
                                               n_nodes=db_info['n_nodes'],
                                               add_disks=cluster_additional_disks,
                                               service_accounts=service_accounts,
                                               **common_params)

       create_node_params = dict(name=name,
                                  size=self._gce_instance_type,
                                  image=self._gce_image,
                                  ex_network=self._gce_network,
                                  ex_disks_gce_struct=gce_disk_struct,
                                  ex_metadata={**self.tags,
                                               "Name": name,
                                               "NodeIndex": node_index,
                                               "startup-script": startup_script,
                                               "user-data": json.dumps(dict(scylla_yaml=dict(cluster_name=self.name),
                                                                            start_scylla_on_first_boot=False,
                                                                            raid_level=raid_level)),
                                               "block-project-ssh-keys": "true",
                                               "ssh-keys": f"{username}:ssh-rsa {public_key}", },
                                  ex_service_accounts=self._service_accounts,
                                  ex_preemptible=spot)

        """
        # from defaults - change this to self.params later
        defaults = {
            "gce_network": 'qa-vpc',
            "gce_image_username": 'scylla-test',
            "user_prefix": "gce-qa-provisioner-test",
            "params": {},
            "gce_datacenter": 'us-east1',
        }

        # TODO: get gce disk strcut -> DiskStructProvisioner
        # TODO: get metadata -> GCEMetadataProvisioner
        #

        instance_params = {
            "name": definition.name,
            "size": definition.type,
            "image": definition.image_id,
            "ex_network": defaults.get("gce_network"),
            "ex_disks_gce_struct": gce_disk_struct,
            "ex_metadata": {
                **self.tags,
                "Name": name,
                "NodeIndex": node_index,
                "startup-script": startup_script,
                "user-data": json.dumps(
                    dict(
                        scylla_yaml=dict(
                            "cluster_name": self.name),
                "start_scylla_on_first_boot": False,
                "raid_level": raid_level)),
                "block-project-ssh-keys": "true",
                "ssh-keys": f"{username}:ssh-rsa {public_key}"},
        "ex_service_accounts": self._service_accounts,
        "ex_preemptible": pricing_model
        }

        # print(definition)
        # new_node = self._gce_service.create_node(
        #     name=definition.name,
        #     size=definition.type,
        #     image=definition.image_id,
        # )
        print(new_node)
        # get gce service for the region
        # get instance params using InstanceDefinition and other data
        # call create_node with the params

        pass

    def terminate_instance(self, name: str, wait: bool = False) -> None:
        pass

    def reboot_instance(self, name: str, wait: bool) -> None:
        pass

    def list_instances(self) -> List[VmInstance]:
        pass

    def cleanup(self, wait: bool = False) -> None:
        pass

    def add_instance_tags(self, name: str, tags: Dict[str, str]) -> None:
        pass


# class AzureProvisioner(Provisioner):  # pylint: disable=too-many-instance-attributes
#     """Provides api for VM provisioning in Azure cloud, tuned for Scylla QA. """
#
#     def __init__(self, test_id: str, region: str,  # pylint: disable=unused-argument
#                  azure_service: AzureService = AzureService(), **kwargs):
#         super().__init__(test_id, region)
#         self._azure_service: AzureService = azure_service
#         self._cache: Dict[str, VmInstance] = {}
#         LOGGER.info("getting resources for %s...", self._resource_group_name)
#         self._rg_provider = ResourceGroupProvider(self._resource_group_name, self._region, self._azure_service)
#         self._network_sec_group_provider = NetworkSecurityGroupProvider(self._resource_group_name, self._region,
#                                                                         self._azure_service)
#         self._vnet_provider = VirtualNetworkProvider(self._resource_group_name, self._region, self._azure_service)
#         self._subnet_provider = SubnetProvider(self._resource_group_name, self._azure_service)
#         self._ip_provider = IpAddressProvider(self._resource_group_name, self._region, self._azure_service)
#         self._nic_provider = NetworkInterfaceProvider(self._resource_group_name, self._region, self._azure_service)
#         self._vm_provider = VirtualMachineProvider(self._resource_group_name, self._region, self._azure_service)
#         for v_m in self._vm_provider.list():
#             self._cache[v_m.name] = self._vm_to_instance(v_m)
#
#     @classmethod
#     def discover_regions(cls, test_id: str = "") -> List["AzureProvisioner"]:
#         """Discovers provisioners for in each region for given test id.
#
#         If test_id is not provided, it discovers all related to SCT provisioners."""
#         all_resource_groups = [rg for rg in AzureService().resource.resource_groups.list()
#                                if rg.name.startswith("SCT-")]
#         if test_id:
#             provisioner_params = [(test_id, rg.location) for rg in all_resource_groups if test_id in rg.name]
#         else:
#             # extract test_id from rg names where rg.name format is: SCT-<test_id>-<region>
#             provisioner_params = [(test_id, rg.location) for rg in all_resource_groups
#                                   if (test_id := "-".join(rg.name.split("-")[1:-1]))]
#         return [cls(*params) for params in provisioner_params]
#
#     def get_or_create_instance(self, definition: InstanceDefinition,
#                                pricing_model: PricingModel = PricingModel.SPOT) -> VmInstance:
#         """Create virtual machine in provided region, specified by InstanceDefinition"""
#         if definition.name in self._cache:
#             return self._cache[definition.name]
#         self._rg_provider.get_or_create()
#         sec_group_id = self._network_sec_group_provider.get_or_create(security_rules=ScyllaOpenPorts).id
#         vnet_name = self._vnet_provider.get_or_create().name
#         subnet_id = self._subnet_provider.get_or_create(vnet_name, sec_group_id).id
#         ip_address_id = self._ip_provider.get_or_create(definition.name).id
#         nic_id = self._nic_provider.get_or_create(subnet_id, ip_address_id, name=definition.name).id
#         v_m = self._vm_provider.get_or_create(definition, nic_id, pricing_model)
#         instance = self._vm_to_instance(v_m)
#         self._cache[definition.name] = instance
#         return instance
#
#     def terminate_instance(self, name: str, wait: bool = True) -> None:
#         """Terminates virtual machine, cleaning attached ip address and network interface."""
#         instance = self._cache.get(name)
#         if not instance:
#             LOGGER.warning("Instance %s does not exist. Shouldn't have called it", name)
#             return
#         self._vm_provider.delete(name, wait=wait)
#         del self._cache[name]
#         self._nic_provider.delete(self._nic_provider.get(name))
#         self._ip_provider.delete(self._ip_provider.get(name))
#
#     def reboot_instance(self, name: str, wait=True) -> None:
#         self._vm_provider.reboot(name, wait)
#
#     def list_instances(self) -> List[VmInstance]:
#         """List virtual machines for given provisioner."""
#         return list(self._cache.values())
#
#     def cleanup(self, wait: bool = False) -> None:
#         """Triggers delete of all resources."""
#         tasks = []
#         self._rg_provider.delete(wait)
#         self._network_sec_group_provider.clear_cache()
#         self._vnet_provider.clear_cache()
#         self._subnet_provider.clear_cache()
#         self._ip_provider.clear_cache()
#         self._nic_provider.clear_cache()
#         self._vm_provider.clear_cache()
#         self._cache = {}
#         if wait is True:
#             LOGGER.info("Waiting for completion of all resources cleanup")
#             for task in tasks:
#                 task.wait()
#
#     def add_instance_tags(self, name: str, tags: Dict[str, str]) -> None:
#         """Adds tags to instance."""
#         LOGGER.info("Adding tags '%s' to intance '%s'...", tags, name)
#         instance = self._vm_to_instance(self._vm_provider.add_tags(name, tags))
#         self._cache[name] = instance
#         LOGGER.info("Added tags '%s' to intance '%s'", tags, name)
#
#     @property
#     def _resource_group_name(self):
#         return f"SCT-{self._test_id}-{self._region}"
#
#     def _vm_to_instance(self, v_m: VirtualMachine) -> VmInstance:
#         pub_address = self._ip_provider.get(v_m.name).ip_address
#         nic = self._nic_provider.get(v_m.name)
#         priv_address = nic.ip_configurations[0].private_ip_address
#         tags = v_m.tags.copy()
#         try:
#             admin = v_m.os_profile.admin_username
#         except AttributeError:
#             # specialized machines don't provide usernames
#             # todo lukasz: find a way to get admin name from image (is it possible??)
#             admin = ""
#         image = str(v_m.storage_profile.image_reference)
#         if v_m.priority is VirtualMachinePriorityTypes.REGULAR:
#             pricing_model = PricingModel.ON_DEMAND
#         else:
#             pricing_model = PricingModel.SPOT
#
#         return VmInstance(name=v_m.name, region=v_m.location, user_name=admin, public_ip_address=pub_address,
#                           private_ip_address=priv_address, tags=tags, pricing_model=pricing_model,
#                           image=image, _provisioner=self)
#
#
#
#
