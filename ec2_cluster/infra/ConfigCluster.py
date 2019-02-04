import os
import yaml
import json

from ec2_cluster.infra import EC2NodeCluster


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

    def __str__(self):
        return json.dumps(self.__dict__, indent=4)


class ConfigCluster:
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
        self.cluster_name = f'{template_name}-{node_count}_node-cluster_{cluster_id}'
        self.cluster = EC2NodeCluster(node_count=node_count,
                                      cluster_name=self.cluster_name,
                                      region=self.config.region)


    @property
    def config(self):
        return self._config


    def validate_config_dict(self, config_dict):
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
        return self.cluster.instance_ids

    @property
    def private_ips(self):
        return self.cluster.private_ips

    @property
    def public_ips(self):
        return self.cluster.public_ips

    @property
    def cluster_sg_id(self):
        return self.cluster.cluster_sg_id


    def any_node_is_running_or_pending(self):
        return self.cluster.any_node_is_running_or_pending()


    def wait_for_all_nodes_to_be_running(self):
        return self.cluster.wait_for_all_nodes_to_be_running()

    def wait_for_all_nodes_to_be_status_ok(self):
        return self.cluster.wait_for_all_nodes_to_be_status_ok()

    def wait_for_all_nodes_to_be_terminated(self):
        return self.cluster.wait_for_all_nodes_to_be_terminated()

    def launch(self, verbose=False):
        self.cluster.launch(az=self.config.az,
                            vpc_id=self.config.vpc_id,
                            subnet_id=self.config.subnet_id,
                            ami_id=self.config.ami_id,
                            ebs_snapshot_id=self.config.ebs_snapshot_id,
                            ebs_gbs=self.config.ebs_gbs,
                            ebs_type=self.config.ebs_type,
                            key_pair_name=self.config.key_pair_name,
                            sg_list=self.config.sg_list,
                            iam_role=self.config.iam_role,
                            instance_type=self.config.instance_type,
                            use_placement_group=self.config.use_placement_group,
                            ebs_iops=self.config.ebs_iops,
                            ebs_optimized_instance=self.config.ebs_optimized_instance,
                            tags=self.config.additional_tags,
                            timeout_secs=self.config.cluster_create_timeout_secs,
                            verbose=verbose)


    def terminate(self, verbose=False):
        self.cluster.terminate(verbose=verbose)

    def ips(self):
        if not self.any_node_is_running_or_pending():
            raise RuntimeError("Cluster does not exist. Cannot list ips of cluster that does not exist")

        return {
            "master_public_ip": self.cluster.public_ips[0],
            "worker_public_ips": self.cluster.public_ips[1:],
            "master_private_ip": self.cluster.private_ips[0],
            "worker_private_ips": self.cluster.private_ips[1:]
        }


