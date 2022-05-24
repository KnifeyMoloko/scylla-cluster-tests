import logging
from abc import ABC
from dataclasses import dataclass, asdict, field, fields

from sdcm.provision.gce.data_disks import GCEDataDisk, PersistentStandardDisk, ScratchDisk
from sdcm.provision.provisioner import InstanceDefinition

LOGGER = logging.getLogger(__name__)


@dataclass
class DiskStructArgs:
    instance_definition: InstanceDefinition
    root_disk_type: str
    gce_services_project_name: str
    location_name: str
    data_disks: list[GCEDataDisk]

#  pylint: disable=invalid-name, too-many-instance-attributes


@dataclass
class GCEDiskStruct(ABC):
    _instance_definition: InstanceDefinition
    _gce_services_project_name: str
    _location_name: str
    _disk_type: str
    type: str
    deviceName: str = field(init=False)
    initializeParams: dict = field(init=False)
    autoDelete: bool

    def as_struct(self) -> dict:
        private_fields = [item.name for item in fields(self) if item.name.startswith("_")]
        struct = asdict(self)

        for private_field in private_fields:
            struct.pop(private_field)

        return struct

    @staticmethod
    def _get_disk_url(name: str, region_name: str, disk_type: str) -> str:
        return f"projects/{name}/zones/{region_name}/diskTypes/{disk_type}"


@dataclass
class GCERootDiskStruct(GCEDiskStruct):
    boot: bool

    def __post_init__(self):
        self.deviceName = f"{self._instance_definition.name}-root-{self._disk_type}"
        self.initializeParams = {
            "diskType": self._get_disk_url(name=self._gce_services_project_name,
                                           region_name=self._location_name,
                                           disk_type=self._disk_type),
            "diskSizeGb": self._instance_definition.root_disk_size,
            "sourceImage": self._instance_definition.image_id
        }


@dataclass
class GCELocalDiskStruct(GCEDiskStruct):
    _index: int
    interface: str

    def __post_init__(self):
        self.deviceName = f"{self._instance_definition.name}-data-local-ssd-{self._index}"
        self.initializeParams = {
            "diskType": self._get_disk_url(name=self._gce_services_project_name,
                                           region_name=self._location_name,
                                           disk_type=self._disk_type),
        }


@dataclass
class GCEPersistentDiskStruct(GCEDiskStruct):
    _disk_size: int
    _index: int

    def __post_init__(self):
        self.deviceName = f"{self._instance_definition.name}-data-{self._disk_type}-{self._index}"
        self.initializeParams = {
            "diskType": self._get_disk_url(name=self._gce_services_project_name,
                                           region_name=self._location_name,
                                           disk_type=self._disk_type),
            "diskSizeGb": self._disk_size,
        }


# pylint:disable=too-few-public-methods
class DiskStructProvider:
    @staticmethod
    def get_disks_struct(disk_struct_args: DiskStructArgs):
        LOGGER.info("Running DiskStructProvider.get_disks_struct with disk_struct_args:\n%s", disk_struct_args)

        disk_structs = [GCERootDiskStruct(
            _disk_type=disk_struct_args.root_disk_type,
            _gce_services_project_name=disk_struct_args.gce_services_project_name,
            _location_name=disk_struct_args.location_name,
            _instance_definition=disk_struct_args.instance_definition,
            type=PersistentStandardDisk().type,  # default
            autoDelete=PersistentStandardDisk().auto_delete,  # default
            boot=True
        ).as_struct()]

        local_disks = [disk for disk in disk_struct_args.data_disks if isinstance(disk, ScratchDisk)]
        persistent_disks = [disk for disk in disk_struct_args.data_disks if isinstance(disk, PersistentStandardDisk)]

        for index, disk in enumerate(local_disks):
            disk_structs.append(GCELocalDiskStruct(
                type=disk.type,
                autoDelete=disk.auto_delete,
                _instance_definition=disk_struct_args.instance_definition,
                _gce_services_project_name=disk_struct_args.gce_services_project_name,
                _location_name=disk_struct_args.location_name,
                _disk_type=disk.gce_struct_type,  # get from param defaults
                _index=index,
                interface=disk.interface
            ).as_struct())

        for index, disk in enumerate(persistent_disks):
            disk_structs.append(
                GCEPersistentDiskStruct(
                    _instance_definition=disk_struct_args.instance_definition,
                    _gce_services_project_name=disk_struct_args.gce_services_project_name,
                    _location_name=disk_struct_args.location_name,
                    _disk_type=disk.gce_struct_type,
                    type=disk.type,
                    autoDelete=disk.auto_delete,
                    _disk_size=disk.size,
                    _index=index
                ).as_struct())

        return disk_structs
