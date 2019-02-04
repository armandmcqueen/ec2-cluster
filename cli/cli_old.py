import argparse
import yaml
import os
try:
    import ujson as json
except ImportError:
    import json

from ec2_cluster.infra import EC2NodeCluster

class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

    def __str__(self):
        return json.dumps(self.__dict__, indent=4)



########################################################################################################
# Cluster logic
########################################################################################################


def cluster_exists(cluster):
    return cluster.any_node_is_running_or_pending()


def describe_ips(cluster):
    if not cluster_exists(cluster):
        raise RuntimeError("Cluster does not exist. Cannot describe runtime attributes of cluster that does not exist")

    return {
        "master_public_ip": cluster.public_ips[0],
        "worker_public_ips": cluster.public_ips[1:],
        "master_private_ip": cluster.private_ips[0],
        "worker_private_ips": cluster.private_ips[1:]
    }


def create(cluster, cfg, tags=None, verbose=False):
    if not cluster_exists(cluster):
        cluster.launch(az=cfg.az,
                       vpc_id=cfg.vpc_id,
                       subnet_id=cfg.subnet_id,
                       ami_id=cfg.ami_id,
                       ebs_snapshot_id=cfg.ebs_snapshot_id,
                       ebs_gbs=cfg.ebs_gbs,
                       ebs_type=cfg.ebs_type,
                       key_pair_name=cfg.key_pair_name,
                       sg_list=cfg.sg_list,
                       iam_role=cfg.iam_role,
                       instance_type=cfg.instance_type,
                       use_placement_group=cfg.use_placement_group,
                       ebs_iops=cfg.ebs_iops,
                       ebs_optimized_instance=cfg.ebs_optimized_instance,
                       tags=tags,
                       timeout_secs=cfg.cluster_create_timeout_secs,
                       verbose=verbose)
    cluster.wait_for_all_nodes_to_be_status_ok()


def terminate(cluster, verbose=False):
    if not cluster.any_node_is_running_or_pending():
        return
    cluster.terminate(verbose=verbose)


def ssh_cmd(cluster, cfg, in_vpc=False):
    ips = describe_ips(cluster)
    if in_vpc:
        master_ip = ips["master_private_ip"]
    else:
        master_ip = ips["master_public_ip"]

    return f'ssh -A {cfg.username}@{master_ip}'


########################################################################################################
# CLI logic
########################################################################################################


def validate_environment():
    pass


def validate_configs(cluster_configs, clusterdef_param_list):
    maybe_nonexistant_params = ["ebs_iops"]

    for p in clusterdef_param_list:
        param_name = p["param_name"]
        if param_name in maybe_nonexistant_params:
            continue

        assert param_name in cluster_configs.keys(), f'Mandatory argument {param_name} is missing'
        assert cluster_configs[param_name] is not None, f'Mandatory argument {param_name} is None'

    if cluster_configs["ebs_type"] == "io1":
        assert "ebs_iops" in cluster_configs.keys(), f'When ebs_type==io1, ebs_iops must be defined. Currently missing'
        assert cluster_configs["ebs_iops"] is not None, f'When ebs_type==io1, ebs_iops must be defined. Currently None'


def parse_type(type_as_str):
    if type_as_str == "str":
        return str
    elif type_as_str == "int":
        return int
    elif type_as_str == "float":
        return float
    elif type_as_str == "bool":
        return bool
    elif type_as_str == "list":
        return list
    else:
        raise RuntimeError(f'Unrecognized type string: {type_as_str}')



if __name__ == "__main__":

    ########################################################################################################
    # Argument and YAML config parsing and validation
    ########################################################################################################

    # Load the list of params used to launch a cluster from clusterdef_params.yaml. Can be set by a config.yaml file
    # or through command line args. If set in both config.yaml and cli args, the cli arg value with be used

    param_list_yaml_abspath = os.path.join(os.path.dirname(os.path.realpath(__file__)), "clusterdef_params.yaml")
    with open(param_list_yaml_abspath, 'r') as f:
        cluster_param_list = yaml.load(f)["params"]

    parser = argparse.ArgumentParser()


    parser.add_argument(
                        "action",
                        help="Action to take. Create cluster, delete cluster, describe cluster ips, "
                             "output ssh cmd string",
                        choices=["create", "terminate", "describe", "ssh_cmd", "test"])

    parser.add_argument(
                        "--verbose",
                        action="store_true")

    parser.add_argument(
                        "--config",
                        help="Path to yaml file with default cluster def params. Defaults can be overwritten by "
                             "additional command line arguments")

    parser.add_argument(
                        "--in_vpc",
                        help="If true, retrieves the nodes private ips. If false, public ips",
                        action="store_true")

    parser.add_argument(
                        "--clean_create",
                        help="Rebuild cluster cleanly? By default, will reuse cluster if one with the correct name "
                             "already exists. Does not detect mismatches between existing cluster and desired cluster",
                        action="store_true")

    # Add all the ClusterDef params as CLI arguments
    for param in cluster_param_list:
        parser.add_argument(
                            f'--{param["param_name"]}',
                            help=f'Cluster definition param. {param["param_desc"]}',
                            type=parse_type(param["param_type"]))

    args, leftovers = parser.parse_known_args()

    if args.action == "test":
        args.verbose = True

    def vlog(s):
        if args.verbose:
            print(f'[cli.py] {s}')

    cluster_configs = {}


    # Load ClusterDef params from config yaml if --config arg was user
    if args.config is not None:
        vlog("Found config yaml arg")
        config_yaml_abspath = args.config if args.config.startswith("/") else os.path.join(os.getcwd(), args.config)
        vlog(f'Loading default params from {config_yaml_abspath}')
        with open(config_yaml_abspath, 'r') as yml:
            cluster_configs = yaml.load(yml)
        vlog(f'Default params from config yaml: {json.dumps(cluster_configs, indent=4)}')


    # Add ClusterDef params from command line args. Overwrite yaml param value if the param is defined in both
    args_as_dict = vars(args)
    for param in cluster_param_list:
        param_name = param["param_name"]

        if args_as_dict[param_name] is not None:
            cluster_configs[param_name] = args_as_dict[param_name]



    if "ebs_optimized_instance" not in cluster_configs.keys():
        cluster_configs["ebs_optimized_instance"] = True

    validate_configs(cluster_configs, cluster_param_list) # Validate that all necessary params have been included
    cfg = AttrDict(cluster_configs) # Convert to AttrDict for readability (dot notation for lookup)
    vlog(f'Final cfg: {cfg}')

    validate_environment()

    ########################################################################################################
    # Cluster action
    ########################################################################################################

    cluster_name = f'{cfg.cluster_template_name}-{cfg.node_count}node-cluster{cfg.cluster_id}'

    vlog(f'Cluster name: {cluster_name}')

    # Define the EC2NodeCluster. This creates the cluster python object, but doesn't take any action in terms of AWS
    cluster = EC2NodeCluster(node_count=cfg.node_count,
                             cluster_name=cluster_name,
                             region=cfg.region)

    vlog(f'Action = {args.action}')

    if args.action == "test":
        pass

    elif args.action == "create":
        if cluster_exists(cluster):
            if args.clean_create:
                terminate(cluster, verbose=args.verbose)
            else:
                raise RuntimeError("Trying to create a cluster that already exists. Did you want to use the "
                                   "--clean_create flag?")
        create(cluster, cfg, verbose=args.verbose)

    elif args.action == "terminate":
        if cluster_exists(cluster):
            terminate(cluster, verbose=args.verbose)
        else:
            raise RuntimeError("Cannot terminate. The cluster does not exist.")

    elif args.action == "describe":
        if cluster_exists(cluster):
            print(describe_ips(cluster))
        else:
            print("Cluster does not exist")

    elif args.action == "ssh_cmd":
        if cluster_exists(cluster):
            print(ssh_cmd(cluster, cfg, args.in_vpc))
        else:
            raise RuntimeError("Cannot print SSH command. The cluster does not exist")

