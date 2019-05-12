#!/usr/bin/env python3

import argparse
import os
import yaml

from ec2_cluster.infra import ConfigCluster
from ec2_cluster.control import ClusterShell
from ec2_cluster.orch import set_up_passwordless_ssh_from_master_to_workers


# Translate `param_type` string in clusterdef_params to Python type for argparse arguments
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

def horovod_setup(cluster, ssh_key_path, ssh_to_private_ip=False):
    master_ip = cluster.ips["master_public_ip"] if not ssh_to_private_ip else cluster.ips["master_private_ip"]
    worker_ips = cluster.ips["worker_private_ips"]
    shell = ClusterShell(cluster.config.username,
                         master_ip,
                         worker_ips,
                         ssh_key_path=ssh_key_path,
                         use_bastion=False)
    set_up_passwordless_ssh_from_master_to_workers(shell, master_ip, worker_ips=worker_ips)


def handle_core():
    path_to_containing_dir = os.path.dirname(os.path.realpath(__file__))
    param_list_yaml_abspath = os.path.join(path_to_containing_dir, "../params/clusterdef_params.yaml")
    with open(param_list_yaml_abspath, 'r') as f:
        cluster_param_list = yaml.load(f)["params"]

    parser = argparse.ArgumentParser()

    parser.add_argument(
            "action",
            choices=["create", "delete", "describe", "ssh-cmd", "setup-horovod", "test"])

    parser.add_argument(
            "config",
            help="Path to config YAML file describing cluster")

    parser.add_argument(
            "--verbose",
            action="store_true")


    parser.add_argument(
            "--ssh_to_private_ip",
            help="Add this flag if you want the ssh-cmd or horovod-setup to use the private IP for the master instead "
                 "of the public IP. Typically this is only needed when you are running this CLI from an EC2 node "
                 "instead of your local machine.",
            action="store_true")

    parser.add_argument(
            "--clean_create",
            help="By default, create will fail if a cluster with this name already exists. With this flag, will instead"
                 "delete the existing cluster and launch a new one.",
            action="store_true")

    parser.add_argument(
            "--horovod",
            help="When creating this cluster, use --horovod to do horovod-setup after nodes are launched.",
            action="store_true")

    parser.add_argument(
            "--ssh_key_path",
            help="Absolute path to your local ssh_key. Required for horovod_setup or when using --horovod with create")

    # Add all the ClusterDef params as CLI arguments
    for param in cluster_param_list:
        parser.add_argument(
                f'--{param["param_name"]}',
                help=f'Cluster definition param. {param["param_desc"]}',
                type=parse_type(param["param_type"]))


    args, leftovers = parser.parse_known_args()

    if args.action == "test":
        args.verbose = True
        print("Tested!")
        quit()

    def vlog(s):
        if args.verbose:
            print(f'[cli_v2.py] {s}')


    # Extract any ClusterDef params from CLI arguments the user passed in
    args_as_dict = vars(args)
    cli_args = {param["param_name"]: args_as_dict[param["param_name"]]
                for param in cluster_param_list
                if args_as_dict[param["param_name"]] is not None}

    vlog(f'Command line args: {args}')
    vlog(f'ClusterDef args found in CLI args: {cli_args}')

    if args.config is None:
        config_yaml_abspath = None
    else:
        config_yaml_abspath = args.config if args.config.startswith("/") else os.path.join(os.getcwd(), args.config)

    vlog(f'Pulling yaml from: {config_yaml_abspath}')
    cluster = ConfigCluster(config_yaml_abspath, other_args=cli_args)
    vlog(f'Final config: {cluster.config}')




    vlog(f'Action = {args.action}')

    if args.action == "test":
        pass

    elif args.action == "create":
        if cluster.any_node_is_running_or_pending():
            if not args.clean_create:
                raise RuntimeError("Trying to create a cluster that already exists. Did you want to use the "
                                   "--clean_create flag?")
            else:
                vlog("clean_create: terminating existing instances")
                cluster.terminate(verbose=args.verbose)
                vlog("cleaned.")

        vlog("Launching cluster")
        cluster.launch(verbose=args.verbose)
        vlog("Cluster launched")

        if args.horovod:
            vlog("Starting Horovod setup")
            assert args.ssh_key_path is not None, "If using --horovod, you must provide --ssh_key_path"
            horovod_setup(cluster, args.ssh_key_path, ssh_to_private_ip=args.ssh_to_private_ip)
            vlog("Finished Horovod setup")



    elif args.action == "delete":
        if cluster.any_node_is_running_or_pending():
            vlog("Starting delete")
            cluster.terminate(verbose=args.verbose)
            vlog("Deletion complete")
        else:
            raise RuntimeError("Cannot terminate. The cluster does not exist.")

    elif args.action == "describe":
        vlog("describe output:")
        if not cluster.any_node_is_running_or_pending():
            print("Cluster does not exist")
        else:
            print(cluster.ips)



    elif args.action == "ssh_cmd":
        if not cluster.any_node_is_running_or_pending():
            raise RuntimeError("Cannot print SSH command. The cluster does not exist")
        else:
            ssh_ip = cluster.ips["master_public_ip"] if not args.ssh_to_private_ip else cluster.ips["master_private_ip"]
            print(f'ssh -A {cluster.config.username}:{ssh_ip}')

    elif args.action == "horovod-setup":
        if not cluster.any_node_is_running_or_pending():
            raise RuntimeError("Cannot run horovod setup. The cluster does not exist")
        else:
            vlog("Starting Horovod setup")
            assert args.ssh_key_path is not None, "If using horovod-setup, you must provide --ssh_key_path"
            horovod_setup(cluster, args.ssh_key_path, ssh_to_private_ip=args.ssh_to_private_ip)
            vlog("Finished Horovod setup")



