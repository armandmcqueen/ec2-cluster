from typing import List
import ec2_cluster.config.validation as validations


class EC2ClusterError(Exception):
    pass


class ConfigField:
    def __init__(
            self,
            typ,
            help_str="",
            default=None,
            validation_fn=None
    ):
        self.typ = typ
        self.help_str = help_str
        self.default = default
        self.validation_fn = validation_fn


# Validation_fn validates the field in the finalized config, not user input.
FIELDS = {
    "cluster_id": ConfigField(
        typ=str,
        validation_fn=validations.is_nonempty_string
    ),
    "region": ConfigField(
        typ=str,
        validation_fn=validations.is_nonempty_string  # TODO: Better validation
    ),
    "vpc": ConfigField(
        typ=str,
        validation_fn=validations.validate_vpc
    ),
    "subnet": ConfigField(
        typ=str,
        validation_fn=validations.validate_subnet
    ),
    "ami": ConfigField(
        typ=str,
        validation_fn=validations.validate_ami
    ),
    "ebs_type": ConfigField(
        typ=str,
        default="gp3",
        validation_fn=validations.is_nonempty_string  # TODO: More rigorous validation
    ),
    "ebs_size": ConfigField(
        typ=int,
        default=100,
        validation_fn=validations.is_positive_int
    ),
    "ebs_iops": ConfigField(
        typ=int,
        default=3000,  # TODO: Check if it can be missing for gp3
        validation_fn=validations.is_positive_int_or_none
    ),
    "ebs_throughput": ConfigField(
        typ=int,
        default=125,   # TODO: Check if it can be missing for gp3
        validation_fn=validations.is_positive_int_or_none
    ),
    "instance_type": ConfigField(
        typ=str,
        default="m5.large",
        validation_fn=validations.is_nonempty_string  # TODO: Could do better validation
    ),
    "num_instances": ConfigField(
        typ=int,
        default=1,
        validation_fn=validations.is_positive_int
    ),
    "iam_role": ConfigField(
        typ=str,
        default=None,
        validation_fn=validations.is_nonempty_string_or_none
    ),
    "keypair": ConfigField(
        typ=str,
        default=None,
        validation_fn=validations.is_nonempty_string
    ),
    "security_groups": ConfigField(
        typ=List,
        default=[],
        validation_fn=validations.validate_security_groups
    ),
    "timeout": ConfigField(
        typ=int,
        default=None,
        validation_fn=validations.is_positive_int_or_none
    ),
    "tags": ConfigField(
        typ=List,
        default=[],
        validation_fn=validations.validate_tags
    ),
    "username": ConfigField(
        typ=str,
        validation_fn=validations.is_nonempty_string
    ),
    "placement_group": ConfigField(
        typ=bool,
        default=False,
        validation_fn=validations.is_bool
    ),
}



