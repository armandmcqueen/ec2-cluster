import dataclasses
from typing import List
import enum


@dataclasses.dataclass
class CidrBlockAssociationSet:
    association_id: str
    cidr_block: str
    cidr_block_state: str

@dataclasses.dataclass
class VpcInfo:
    cidr_block: str
    dhcp_options_id: str
    state: str
    vpc_id: str
    owner_id: str
    instance_tenancy: str
    cidr_block_association_sets: List[CidrBlockAssociationSet]
    is_default: bool

    @staticmethod
    def from_vpc_description_json(vpc_description_json):
        cidr_block_associations = [
            CidrBlockAssociationSet(
                association_id=association["AssociationId"],
                cidr_block=association["CidrBlock"],
                cidr_block_state=association["CidrBlockState"]["State"]
            )
            for association
            in vpc_description_json["CidrBlockAssociationSet"]
        ]
        return VpcInfo(
            cidr_block=vpc_description_json["CidrBlock"],
            dhcp_options_id=vpc_description_json["DhcpOptionsId"],
            state=vpc_description_json["State"],
            vpc_id=vpc_description_json["VpcId"],
            owner_id=vpc_description_json["OwnerId"],
            instance_tenancy=vpc_description_json["InstanceTenancy"],
            is_default=vpc_description_json["IsDefault"],
            cidr_block_association_sets=cidr_block_associations
        )


@dataclasses.dataclass
class CidrBlockAssociationSet:
    association_id: str
    cidr_block: str
    cidr_block_state: str

@dataclasses.dataclass
class VpcInfo:
    cidr_block: str
    dhcp_options_id: str
    state: str
    vpc_id: str
    owner_id: str
    instance_tenancy: str
    cidr_block_association_sets: List[CidrBlockAssociationSet]
    is_default: bool

    @staticmethod
    def from_vpc_description_json(vpc_description_json):
        cidr_block_associations = [
            CidrBlockAssociationSet(
                association_id=association["AssociationId"],
                cidr_block=association["CidrBlock"],
                cidr_block_state=association["CidrBlockState"]["State"]
            )
            for association
            in vpc_description_json["CidrBlockAssociationSet"]
        ]
        return VpcInfo(
            cidr_block=vpc_description_json["CidrBlock"],
            dhcp_options_id=vpc_description_json["DhcpOptionsId"],
            state=vpc_description_json["State"],
            vpc_id=vpc_description_json["VpcId"],
            owner_id=vpc_description_json["OwnerId"],
            instance_tenancy=vpc_description_json["InstanceTenancy"],
            is_default=vpc_description_json["IsDefault"],
            cidr_block_association_sets=cidr_block_associations
        )



@dataclasses.dataclass
class SubnetInfo:
    availability_zone: str
    availability_zone_id: str
    available_ip_address_count: int
    cidr_block: str
    default_for_az: bool
    map_public_ip_on_launch: bool
    map_customer_owned_id_on_launch: bool
    state: str
    subnet_id: str
    vpc_id: str
    owner_id: str
    assign_ipv6_address_on_creation: bool
    subnet_arn: str
    # Skipped (unused and requires additional class)
    # ipv6_cidr_block_association_set: List

    @staticmethod
    def from_subnet_description_json(j):
        return SubnetInfo(
            availability_zone=j["AvailabilityZone"],
            availability_zone_id=j["AvailabilityZoneId"],
            available_ip_address_count=j["AvailableIpAddressCount"],
            cidr_block=j["CidrBlock"],
            default_for_az=j["DefaultForAz"],
            map_public_ip_on_launch=j["MapPublicIpOnLaunch"],
            map_customer_owned_id_on_launch=j["MapCustomerOwnedIpOnLaunch"],
            state=j["State"],
            subnet_id=j["SubnetId"],
            vpc_id=j["VpcId"],
            owner_id=j["OwnerId"],
            assign_ipv6_address_on_creation=j["AssignIpv6AddressOnCreation"],
            subnet_arn=j["SubnetArn"],
        )

class ImageType(enum.Enum):
    Ubuntu = "Ubuntu"
    AmazonLinux = "Amazon Linux"

class ImageArchitecture(enum.Enum):
    i386 = "i386"
    x86_64 = "x86_64"
    arm64 = "arm64"


@dataclasses.dataclass
class BlockDeviceMapping:
    device_name: str
    ebs_delete_on_termination: bool
    ebs_snapshot_id: str
    ebs_volume_size: int
    ebs_volume_type: str
    ebs_encrypted: bool

@dataclasses.dataclass
class ImageInfo:
    architecture: str
    creation_date: str
    image_id: str
    image_location: str
    image_type: str
    public: bool
    owner_id: str
    platform_details: str
    usage_operations: str
    state: str
    block_device_mappings: List[BlockDeviceMapping]
    description: str
    ena_support: bool
    hypervisor: str
    image_owner_alias: str
    name: str
    root_device_name: str
    root_device_type: str
    sriov_net_support: str
    virtualization_type: str

    @staticmethod
    def from_image_description_json(j):
        block_device_mappings = [
            BlockDeviceMapping(
                device_name=block_device_mapping_json["DeviceName"],
                ebs_delete_on_termination=block_device_mapping_json["Ebs"]["DeleteOnTermination"],
                ebs_snapshot_id=block_device_mapping_json["Ebs"]["SnapshotId"],
                ebs_volume_size=block_device_mapping_json["Ebs"]["VolumeSize"],
                ebs_volume_type=block_device_mapping_json["Ebs"]["VolumeType"],
                ebs_encrypted=block_device_mapping_json["Ebs"]["Encrypted"],
            )
            for block_device_mapping_json in j["BlockDeviceMappings"]
        ]
        return ImageInfo(
            architecture=j["Architecture"],
            creation_date=j["CreationDate"],  # "2021-06-04T03:55:37.000Z",
            image_id=j["ImageId"],
            image_location=j["ImageLocation"],
            image_type=j["ImageType"],
            public=j["Public"],
            owner_id=j["OwnerId"],
            platform_details=j["PlatformDetails"],
            usage_operations=j["UsageOperation"],
            state=j["State"],
            block_device_mappings=block_device_mappings,
            description=j["Description"],
            ena_support=j["EnaSupport"],
            hypervisor=j["Hypervisor"],
            image_owner_alias=j["ImageOwnerAlias"],
            name=j["Name"],
            root_device_name=j["RootDeviceName"],
            root_device_type=j["RootDeviceType"],
            sriov_net_support=j["SriovNetSupport"],
            virtualization_type=j["VirtualizationType"]
        )
