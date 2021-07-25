from typing import List
import textwrap
import ec2_cluster.config.validation as validations


class EC2ClusterError(Exception):
    pass


class ConfigField:
    def __init__(
            self,
            typ,
            info,
            default=None,
            validation_fn=None
    ):
        self.typ = typ
        self.info = info
        self.default = default
        self.validation_fn = validation_fn


# Validation_fn validates the field in the finalized config, not user input.
FIELDS = {
    "cluster_id": ConfigField(
        typ=str,
        info="The name of the cluster. This is used in tags to manage instances. This should be unique within a region.",
        validation_fn=validations.is_nonempty_string
    ),
    "region": ConfigField(
        typ=str,
        info="The AWS region to deploy the cluster into",
        validation_fn=validations.is_nonempty_string  # TODO: Better validation
    ),
    "vpc": ConfigField(
        typ=str,
        info="The id of the VPC to deploy into. Must match the region and be in the form 'vpc-XXXXXXXXX'",
        validation_fn=validations.validate_vpc
    ),
    "subnet": ConfigField(
        typ=str,
        info="The id of the subnet to deploy into. Must match the VPC and be in the form 'subnet-XXXXXXXXX'",
        validation_fn=validations.validate_subnet
    ),
    "ami": ConfigField(
        typ=str,
        info="The AMI id to use for the instance. Must be in the form 'ami-XXXXXXX'. Not setting "
             "this will use the most recent AmazonLinux2 AMI as the deafult. Retrieving this value "
             "from AWS is slow so we recommend setting this value explicitly.",
        validation_fn=validations.validate_ami
    ),
    "ebs_type": ConfigField(
        typ=str,
        info="The type of the EBS volume",
        default="gp3",
        validation_fn=validations.is_nonempty_string  # TODO: More rigorous validation
    ),
    "ebs_device_name": ConfigField(
        typ=str,
        info="The device name of the root volume.",
        validation_fn=validations.is_nonempty_string_or_none
    ),
    "ebs_size": ConfigField(
        typ=int,
        info="The size of the EBS volume in GB",
        default=100,
        validation_fn=validations.is_positive_int
    ),
    "ebs_iops": ConfigField(
        typ=int,
        info="The amount of provisioned IOPS for the EBS volume. Should only be set if the ebs_type allows for IOPS to be specified",
        default=3000,  # TODO: Check if it can be missing for gp3
        validation_fn=validations.is_positive_int_or_none
    ),
    "ebs_throughput": ConfigField(
        typ=int,
        info="The amount of provisoned throughput for the EBS volume. Should only be set if the ebs_type allows for throughput to be specified",
        default=125,   # TODO: Check if it can be missing for gp3
        validation_fn=validations.is_positive_int_or_none
    ),
    "instance_type": ConfigField(
        typ=str,
        info="The type of the EC2 instances. Uses the API name (e.g. 'm5.large)",
        default="m5.large",
        validation_fn=validations.is_nonempty_string  # TODO: Could do better validation
    ),
    "num_instances": ConfigField(
        typ=int,
        info="The number of instance to launch in the cluster",
        default=1,
        validation_fn=validations.is_positive_int
    ),
    "iam_role": ConfigField(
        typ=str,
        info="The name of the IAM role to attach to the instances. This is not the ARN",
        default=None,
        validation_fn=validations.is_nonempty_string_or_none
    ),
    "keypair": ConfigField(
        typ=str,
        info="The name of the keypair to launch the instances with",
        default=None,
        validation_fn=validations.is_nonempty_string
    ),
    "security_groups": ConfigField(
        typ=List,
        info=textwrap.dedent("""A list of security groups to attach in the form 'sg-XXXXXXX'. This is always 
                                one security group created to allow communication between nodes in the cluster"""),
        default=[],
        validation_fn=validations.validate_security_groups
    ),
    "timeout": ConfigField(
        typ=int,
        info=textwrap.dedent("""How long to spend retrying to spin up instances when AWS has insufficient 
                                capacity. If the timeout is exceeded, the creation is aborted and any 
                                launched instances are shut down"""),
        default=None,  # None indicates trying forever
        validation_fn=validations.is_positive_int_or_none
    ),
    "tags": ConfigField(
        typ=List,
        info=textwrap.dedent("""Additional tags to attach to the instances. Must be a 
                                list of dicts in format 
                                [{'Key': key1, 'Value': val1}, {'Key': key2, 'Value': val2}]"""),
        default=[],
        validation_fn=validations.validate_tags
    ),
    "username": ConfigField(
        typ=str,
        info="The username associated with the AMI. Used for running commands on the instance.",
        validation_fn=validations.is_nonempty_string_or_none
    ),
    "placement_group": ConfigField(
        typ=bool,
        info="Whether to launch the instances into a placement group",
        default=False,
        validation_fn=validations.is_bool
    ),
}



