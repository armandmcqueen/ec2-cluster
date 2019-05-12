import os
import yaml
import argparse
from tabulate import tabulate

from ec2_cluster.utils import get_dlamis, get_my_amis


def text_wrap(txt, width=75):
    output_lines = []
    words = txt.split(" ")

    current_line = ""
    current_width = 0
    for word in words:
        if current_width + len(word) + 1 >= 75:
            output_lines.append(current_line)
            current_line = word + " "
            current_width = len(word) + 1
        else:
            current_line += word + " "
            current_width += len(word) + 1
    if current_line != "":
        output_lines.append(current_line)

    return "\n".join(output_lines)


def display_config_params(config_param_list):
    rows =[]
    for c in config_param_list:
        description = c['param_desc']
        description = text_wrap(description)
        rows.append([c['param_name'], c['param_type'], description])

    print("")
    print(tabulate(rows, headers=["Name", "Type", "Description"]))


def display_ami_list(amis):
    rows = []
    for ami in amis:
        description = ami['Description']
        description = text_wrap(description)
        rows.append([ami['Name'], ami['ImageId'], ami['SnapshotId'], description])

    print("")
    print(tabulate(rows, headers=["Name", "AMI Id", "EBS Snapshot Id", "Description"]))



def handle_utils():



    parser = argparse.ArgumentParser()

    parser.add_argument(
            "utils",
            choices=["utils"])

    parser.add_argument(
            "action",
            choices=["list-dlami", "list-ami", "describe-config"])

    parser.add_argument(
            "--dlami_type",
            help="[list-dlami] Which of the DLAMI flavors to retrieve info about - Ubuntu or Amazon Linux",
            choices=["Ubuntu", "AL"],
            default="Ubuntu"
    )

    parser.add_argument(
            "--region",
            help="[list-dlami or list-ami] Which AWS region to retrieve information about, e.g. us-east-1. Defaults to AWS CLI default region",
    )

    args, leftovers = parser.parse_known_args()

    if args.action == "list-dlami":
        dlamis = get_dlamis(region=args.region, ami_type=args.dlami_type)
        display_ami_list(dlamis)

    elif args.action == "list-ami":
        amis = get_my_amis(region=args.region)
        display_ami_list(amis)

    elif args.action == "describe-config":
        path_to_containing_dir = os.path.dirname(os.path.realpath(__file__))
        param_list_yaml_abspath = os.path.join(path_to_containing_dir, "../params/clusterdef_params.yaml")
        config_param_list = yaml.load(open(param_list_yaml_abspath, 'r'))["params"]

        display_config_params(config_param_list)





