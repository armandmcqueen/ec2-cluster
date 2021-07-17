import dataclasses
from typing import Optional, Dict, Union, List, Any
from pathlib import Path
import yaml


class EC2ClusterError(Exception):
    pass


class ClusterConfigValidationError(EC2ClusterError):
    pass


@dataclasses.dataclass
class ClusterConfig:
    cluster_template_name: Optional[str] = None
    cluster_id: Optional[str] = None
    region: Optional[str] = None
    vpc: Optional[str] = None
    subnet: Optional[str] = None
    ami: Optional[str] = None
    ebs_type: Optional[str] = None
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

    def validate(self):
        # TODO: Implement
        return


def tag_dict_is_valid(tag: Any):
    if not isinstance(tag, Dict):
        return False
    if 'Key' not in tag.keys():
        return False
    if tag['Key'] == 'Name':
        # This is reserved
        return False
    if 'Value' not in tag.keys():
        return False
    if len(list(tag.keys())) != 2:
        return False
    return True


#
FIELDS = {
    "cluster_template_name": dict(typ=str),
    "cluster_id": dict(typ=str),
    "vpc": dict(typ=str),
    "subnet": dict(typ=str),
    "ami": dict(typ=str),
    "ebs_type": dict(typ=str),
    "ebs_size": dict(typ=int),
    "ebs_iops": dict(typ=int),
    "ebs_throughput": dict(typ=int),
    "instance_type": dict(typ=str),
    "num_instances": dict(typ=int),
    "iam_role": dict(typ=str),
    "keypair": dict(typ=str),
    "security_groups": dict(typ=List, list_val_fn=lambda x: isinstance(x, str)),
    "timeout": dict(typ=int),
    "tags": dict(typ=List, list_val_fn=tag_dict_is_valid),
    "username": dict(typ=str),
    "placement_group": dict(typ=bool),
}


def validate_config_file_dict(config_file_dict):
    for field_name, field_val in config_file_dict.items():
        # TODO: Better error handling UX
        assert field_name in FIELDS
        assert isinstance(field_val, FIELDS[field_name]["typ"])
        if isinstance(field_val, List):
            val_fn = FIELDS[field_name]["list_val_fn"]
            for item in field_val:
                if not val_fn(item):
                    raise EC2ClusterError()





def load_config_from_file(
        config_file_path: Optional[Union[str, Path]] = None,
        cluster_template_name: Optional[str] = None,
        cluster_id: Optional[str] = None,
        region: Optional[str] = None,
        vpc: Optional[str] = None,
        subnet: Optional[str] = None,
        ami: Optional[str] = None,
        ebs_type: Optional[str] = None,
        ebs_size: Optional[int] = None,
        ebs_iops: Optional[int] = None,
        ebs_throughput: Optional[int] = None,
        instance_type: Optional[str] = None,
        num_instances: Optional[int] = None,
        iam_role: Optional[str] = None,
        keypair: Optional[str] = None,
        security_groups: Optional[str] = None,
        timeout: Optional[int] = None,
        tags: Optional[Dict] = None,
        username: Optional[str] = None,
        placement_group: Optional[bool] = None
):
    cfg = ClusterConfig()

    config_path = Path(config_file_path).absolute()
    if not config_path.exists():
        raise EC2ClusterError(f"No config file found at {config_path}")

    with config_path.open() as f:
        config_file_dict = yaml.safe_load(f)

    validate_config_file_dict(config_file_dict)

    for field_name, field_val in config_file_dict.items():
        setattr(cfg, field_name, field_val)

    # Overwrite any specified fields
    if cluster_template_name is not None:
        cfg.cluster_template_name = cluster_template_name
    if cluster_id is not None:
        cfg.cluster_id = cluster_id
    if region is not None:
        cfg.region = region
    if vpc is not None:
        cfg.vpc = vpc
    if subnet is not None:
        cfg.subnet = subnet
    if ami is not None:
        cfg.ami = ami
    if ebs_type is not None:
        cfg.ebs_type = ebs_type
    if ebs_size is not None:
        cfg.ebs_size = ebs_size
    if ebs_iops is not None:
        cfg.ebs_iops = ebs_iops
    if ebs_throughput is not None:
        cfg.ebs_throughput = ebs_throughput
    if instance_type is not None:
        cfg.instance_type = instance_type
    if num_instances is not None:
        cfg.num_instances = num_instances
    if iam_role is not None:
        cfg.iam_role = iam_role
    if keypair is not None:
        cfg.keypair = keypair
    if security_groups is not None:
        cfg.security_groups = security_groups
    if timeout is not None:
        cfg.timeout = timeout
    if tags is not None:
        cfg.tags = tags
    if username is not None:
        cfg.username = username
    if placement_group is not None:
        cfg.placement_group = placement_group

    return cfg



