import os
import yaml
import json

from ec2_cluster.infra import EC2NodeCluster


class AttrDict(dict):
    """
    Class for working with dicts using dot notation
    """
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

    def __str__(self):
        return json.dumps(self.__dict__, indent=4)



class ConfigCluster:
    """Class for defining an EC2NodeCluster in a config file

    Lightweight wrapper around EC2NodeCluster where you define the launch configuration at instantiation time instead
    of at runtime. Configuration values are loaded from a YAML file, with values optionally overwritten in `__init__`.

    Generally exposes the same API as EC2NodeCluster. The underlying EC2NodeCluster is always available via
    `self.cluster`.

    Args:
        config_yaml_abspath: Path to a yaml configuration file
        other_args: Dictionary containing additional configuration values which will overwrite values from the
                    config file
    """
    def __init__(self, config_yaml_abspath=None, other_args=None):
        if other_args is None:
            other_args = {}


        # Pull in list of params
        param_list_yaml_abspath = os.path.join(os.path.dirname(os.path.realpath(__file__)), "clusterdef_params.yaml")
        with open(param_list_yaml_abspath, 'r') as f:
            self.paramdef_list = yaml.load(f)["params"]


        # Use yaml arguments as base if yaml file path was input
        if config_yaml_abspath is None:
            config_dict = {}
        else:
            with open(config_yaml_abspath, 'r') as yml:
                config_dict = yaml.load(yml)


        # Add the other_args, overwriting the yaml arguments if the param is defined in both
        for param_name, param_val in other_args.items():
            config_dict[param_name] = param_val


        # Validate then convert to AttrDict for dot notation member access
        self.validate_config_dict(config_dict)
        self._config = AttrDict(config_dict)


        # Define cluster name from params and instantiate EC2NodeCluster
        template_name = self.config.cluster_template_name
        node_count = self.config.node_count
        cluster_id = self.config.cluster_id
        self.cluster_name = f'{template_name}-{node_count}node-cluster{cluster_id}'
        self.cluster = EC2NodeCluster(node_count=node_count,
                                      cluster_name=self.cluster_name,
                                      region=self.config.region)


    @property
    def config(self):
        """Get config AttrDict"""
        return self._config


    def validate_config_dict(self, config_dict):
        """Validate that a given configuration is valid

        Args:
            config_dict: Dictionary of config values. Descriptions for each config field can be found in
                        `clusterdef_params.yaml`
        """
        # Some params are optional. Handle them separately later
        maybe_nonexistant_params = ["ebs_iops", 'ebs_optimized_instance', 'additional_tags']

        for p in self.paramdef_list:
            param_name = p["param_name"]
            if param_name in maybe_nonexistant_params:
                continue

            assert param_name in config_dict.keys(), f'Mandatory argument {param_name} is missing'
            assert config_dict[param_name] is not None, f'Mandatory argument {param_name} is None'


        # ebs_iops special case. Must be set when ebs_type=="io1"
        if config_dict["ebs_type"] == "io1":
            assert "ebs_iops" in config_dict.keys(), f'When ebs_type==io1, ebs_iops must be defined. Currently missing'
            assert config_dict["ebs_iops"] is not None, f'When ebs_type==io1, ebs_iops must be defined. Currently None'

        # ebs_optimized_instances special case. Defaults to True
        if "ebs_optimized_instance" not in config_dict.keys() or config_dict["ebs_optimized_instance"] is None:
            config_dict["ebs_optimized_instance"] = True

        # additional_tags special case. Defaults to None as expected by EC2NodeCluster
        if "additional_tags" not in config_dict.keys() or config_dict["additional_tags"] is None:
            config_dict["additional_tags"] = None


    @property
    def instance_ids(self):
        """A list of InstanceIds for the nodes in the cluster.

        All nodes must be in RUNNING or PENDING stats. Always in the same order: [Master, Worker1, Worker2, etc...]
        """
        return self.cluster.instance_ids

    @property
    def private_ips(self):
        """The list of private IPs for the nodes in the cluster.

        All nodes must be in RUNNING or PENDING stats. Output is always in the same order: [Master, Worker1, Worker2, etc...]
        """
        return self.cluster.private_ips

    @property
    def public_ips(self):
        """A list of public IPs for the nodes in the cluster.

        All nodes must be in RUNNING or PENDING stats. Output is always in the same order: [Master, Worker1, Worker2, etc...]
        """
        return self.cluster.public_ips

    @property
    def cluster_sg_id(self):
        """Return the Id of the ClusterSecurityGroup

        When cluster is launched, a security group is created to allow the nodes to communicate with each other. This
        is deleted when the cluster is terminated.

        Raise exception if the ClusterSecurityGroup doesn't exist.
        """
        return self.cluster.cluster_sg_id


    def any_node_is_running_or_pending(self):
        """Return True if any node is in RUNNING or PENDING states"""
        return self.cluster.any_node_is_running_or_pending()


    def wait_for_all_nodes_to_be_running(self):
        """Blocks until all nodes are in the RUNNING state"""
        return self.cluster.wait_for_all_nodes_to_be_running()

    def wait_for_all_nodes_to_be_status_ok(self):
        """Blocks until all nodes have passed the EC2 health check.

        Once nodes are status OK, you can SSH to them. See EC2Node.wait_for_instance_to_be_status_ok() for details.
        """
        return self.cluster.wait_for_all_nodes_to_be_status_ok()

    def wait_for_all_nodes_to_be_terminated(self):
        """Blocks until all nodes are in the TERMINATED state"""
        return self.cluster.wait_for_all_nodes_to_be_terminated()

    def launch(self, verbose=False):
        """Launch the cluster nodes using the config set in `__init__`

        Will repeatedly try to launch instances until all nodes are launched or the timeout is reached.

        Args:
             verbose: True to print out detailed information about progress
        """

        self.cluster.launch(az=self.config.az,
                            vpc_id=self.config.vpc_id,
                            subnet_id=self.config.subnet_id,
                            ami_id=self.config.ami_id,
                            ebs_snapshot_id=self.config.ebs_snapshot_id,
                            volume_gbs=self.config.volume_gbs,
                            volume_type=self.config.volume_type,
                            key_name=self.config.key_name,
                            security_group_ids=self.config.sg_list,
                            iam_ec2_role_name=self.config.iam_ec2_role_name,
                            instance_type=self.config.instance_type,
                            use_placement_group=self.config.use_placement_group,
                            iops=self.config.iops,
                            ebs_optimized=self.config.ebs_optimized,
                            tags=self.config.additional_tags,
                            timeout_secs=self.config.cluster_create_timeout_secs,
                            verbose=verbose)


    def terminate(self, verbose=False):
        """Terminate all nodes in the cluster and clean up security group and placement group

        Args:
            verbose: True to print out detailed information about progress.
        """
        self.cluster.terminate(verbose=verbose)

    @property
    def ips(self):
        """Get all public and private IPs for nodes in the cluster

        All nodes must be in RUNNING or PENDING stats.

        Returns:
        ::
            {
                "master_public_ip": MasterPublicIp,
                "worker_public_ips": [Worker1PublicIp, Worker2PublicIp, etc...]
                "master_private_ip": MasterPrivateIp,
                "worker_private_ips": [Worker1PrivateIp, Worker2PrivateIp, etc...]
            }
        """
        if not self.any_node_is_running_or_pending():
            raise RuntimeError("Cluster does not exist. Cannot list ips of cluster that does not exist")

        return {
            "master_public_ip": self.cluster.public_ips[0],
            "worker_public_ips": self.cluster.public_ips[1:],
            "master_private_ip": self.cluster.private_ips[0],
            "worker_private_ips": self.cluster.private_ips[1:]
        }


