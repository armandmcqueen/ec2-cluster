import dataclasses
from typing import Optional, Dict, Union, List, Any

import boto3

from ec2_cluster.config.fields import FIELDS
import ec2_cluster.config.defaults as defaults


def humanize_float(num): return "{0:,.2f}".format(num)


class EC2ClusterError(Exception):
    pass


class ClusterConfigValidationError(EC2ClusterError):
    pass


@dataclasses.dataclass
class ClusterConfig:
    cluster_id: Optional[str] = None
    region: Optional[str] = None
    vpc: Optional[str] = None
    subnet: Optional[str] = None
    ami: Optional[str] = None
    ebs_type: Optional[str] = None
    ebs_device_name: Optional[str] = None
    ebs_size: Optional[int] = None
    ebs_iops: Optional[int] = None
    ebs_throughput: Optional[int] = None
    instance_type: Optional[str] = None
    num_instances: Optional[int] = None
    iam_role: Optional[str] = None
    keypair: Optional[str] = None
    security_groups: Optional[List[str]] = None
    timeout: Optional[int] = None
    tags: Optional[List[Dict]] = None
    username: Optional[str] = None
    placement_group: Optional[bool] = None

    @property
    def field_names(self):
        return list(self.__dict__.keys())

    def fill_in_defaults(self):
        # Static defaults defined in FIELDS
        for field_name in self.field_names:
            default_val = FIELDS[field_name].default
            if getattr(self, field_name) is None:
                setattr(self, field_name, default_val)

        # More complicated defaults that require querying AWS
        assert self.region is not None, "The region must be set in the config"
        sess = boto3.Session(region_name=self.region)
        if self.vpc is None:
            self.vpc = defaults.get_default_vpc(sess).vpc_id

        if self.subnet is None:
            self.subnet = defaults.get_default_subnets(sess, self.vpc)[0].subnet_id

        # AMI and username are linked. If one is missing, they must both be missing.
        if self.ami is None or self.username is None:
            assert self.ami is None and self.username is None, f"AMI and Username are tightly coupled. If one " \
                                                               f"is set, they both must be set. AMI: {self.ami} " \
                                                               f"Username: {self.username}"
            # TODO: Automatically handle ARM instances
            ami = defaults.get_default_ami(sess)
            self.ami = ami.image_id
            print(f"Using most recent Amazon Linux 2 AMI ({self.ami}). We recommend setting this value "
                  "manually, as automatically filling in this value requires querying a very slow AWS "
                  "API. The username for this AMI is 'ec2-user'")
            self.username = "ec2-user"
            self.ebs_device_name = ami.block_device_mappings[0].device_name

        if self.ebs_device_name is None:
            self.ebs_device_name = defaults.get_default_ebs_device_name(sess, self.ami)



    def validate(self):
        # Field-by-field validation
        for field_name in self.field_names:
            validation_fn = FIELDS[field_name].validation_fn
            # TODO: Switch to a 'validation_fn raises Exception with additional detail' model
            is_valid = validation_fn(field_name, getattr(self, field_name))
            if not is_valid:
                raise ClusterConfigValidationError(f"Failed validation on field {field_name}. Value "
                                                   f"received was: {getattr(self, field_name)}")

        # Validation involving multiple fields
        if self.ebs_type == "io1":
            assert self.ebs_iops is not None, "If the disk type is io1, the number of IOPs must be specified"  # TODO: Is this actually true?
        # TODO: There must be more things we should be validating here for EBS

