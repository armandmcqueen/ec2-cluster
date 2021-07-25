import boto3
import time
import json
from typing import List
from pathlib import Path

from ec2_cluster.instances.node import EC2Node
from ec2_cluster.config.config import ClusterConfig, FIELDS
from ec2_cluster.shells.control import ClusterShell
from typing import Optional, Union, Dict, List
import warnings

# Why does this warning exist? wtf go away
warnings.filterwarnings(
                "ignore",
                "The _yaml extension module is now located at yaml._yaml and its location is "
                "subject to change.  To use the LibYAML-based parser and emitter, import "
                "from `yaml`: `from yaml import CLoader as Loader, CDumper as Dumper`.")
import yaml


def humanize_float(num): return "{0:,.2f}".format(num)

# TODO: Centralize error types
class EC2ClusterError(Exception):
    pass


def validate_config_file_dict(config_file_dict):
    for field_name, field_val in config_file_dict.items():
        # TODO: Better error handling UX
        assert field_name in FIELDS, f"Field '{field_name}' from config file isn't a recognized field name"
        assert isinstance(field_val, FIELDS[field_name].typ)

        is_valid = FIELDS[field_name].validation_fn(field_name, field_val)
        if not is_valid:
            raise EC2ClusterError(f"Cluster config file does not pass validation due "
                                  f"to field '{field_name}' having invalid value {field_val}")



class Cluster:
    # TODO: Fix docstrings for class (including overly verbose ones)
    """
    Class for defining an EC2NodeCluster in a config file

    Lightweight wrapper around EC2NodeCluster where you define the launch configuration at instantiation time instead
    of at runtime. Configuration values are loaded from a YAML file, with values optionally overwritten in `__init__`.

    Generally exposes the same API as EC2NodeCluster. The underlying EC2NodeCluster is always available via
    `self.cluster`.

    Args:
        config_yaml_path: Path to a yaml configuration file. None to load all params from `other_args`.
        other_args: Dictionary containing additional configuration values which will overwrite values from the
                    config file
    """
    """Class for managing a group of EC2 instances as a cluster.

    Layer on top of EC2Node. Allows you to work with instances as a group. For example, create and attach a security
    group that allows all the nodes to communicate or wait for all nodes to reach a certain state.

    In particular for distributed training on the largest GPU instances, there is not always enough capacity to launch
    a large cluster in one go. With EC2NodeCluster, you continue trying to add nodes to the cluster until the entire
    cluster has been created or until the user-set timeout is reached.

    Obviously, that can get expensive as you are paying for the nodes you do have while you wait for all the nodes to
    spawn, but if you need a cluster of a certain size, this is the easiest way to do that.

    EC2NodeCluster names the EC2Nodes based on the cluster_name. Each node gets a number from 1-N and that is postfixed
    to the cluster_name (e.g. 'MyCluster-Node1'). This ensures that the nodes have a definite order. Similar to EC2Node,
    each cluster should have a cluster_name unique in the region. In addition to EC2Node Name collisions, each
    EC2NodeCluster creates a new security group using the cluster_name that can be impacted by Name collisions. This
    is also true for placement groups if using them.

    """

    def __init__(
            self,
            config_file_path: Optional[Union[str, Path]] = None,
            cluster_id: Optional[str] = None,
            region: Optional[str] = None,
            vpc: Optional[str] = None,
            subnet: Optional[str] = None,
            ami: Optional[str] = None,
            ebs_type: Optional[str] = None,
            ebs_device_name: Optional[str] = None,
            ebs_size: Optional[int] = None,
            ebs_iops: Optional[int] = None,
            ebs_throughput: Optional[int] = None,
            instance_type: Optional[str] = None,
            num_instances: Optional[int] = None,
            iam_role: Optional[str] = None,
            keypair: Optional[str] = None,
            security_groups: Optional[List[str]] = None,
            timeout: Optional[int] = None,
            tags: Optional[List[Dict]] = None,
            username: Optional[str] = None,
            placement_group: Optional[bool] = None,
            cluster_id_append: Optional[str] = None,
            always_verbose: bool = False
    ):
        # Args must be at the top of __init__ so other variables don't pollute it
        args = locals()

        # The names of the args should match the fields in a ClusterConfig, with a few exceptions.
        # Programmatically ensure this to avoid drift.
        excluded_args = ["self", "config_file_path", "cluster_id_append", "always_verbose"]
        for excluded_arg in excluded_args:
            assert excluded_arg in args.keys()  # Don't let excluded_args drift either
        for arg_name in args.keys():
            if arg_name in excluded_args:
                continue
            assert arg_name in FIELDS, f"There is argument that is not recognized as a valid config field: {arg_name}"
        for field_name in FIELDS:
            assert field_name in args.keys(), f"There is a config field that is not being set via args: {field_name}"

        self.cfg = ClusterConfig()

        # Load values from file
        with Path(config_file_path).absolute().open() as f:
            config_file_dict = yaml.safe_load(f)

        validate_config_file_dict(config_file_dict)

        for field_name, field_val in config_file_dict.items():
            setattr(self.cfg, field_name, field_val)

        # Add in fields specified via args, overwriting any existing fields
        for field_name, field_val in args.items():
            if field_name in excluded_args:
                continue
            assert hasattr(self.cfg, field_name), f"Tried to dynamically set a field ({field_name}) " \
                                                  f"that doesn't exist in the statically defined " \
                                                  f"object (fields={self.cfg.field_names})"
            if field_val is not None:
                setattr(self.cfg, field_name, field_val)

        assert self.cfg.cluster_id is not None, "cluster_id must be defined"
        if cluster_id_append:
            assert isinstance(cluster_id_append, str), f"cluster_id_append must be a string " \
                                                       f"but was {type(cluster_id_append)}"
            self.cfg.cluster_id += cluster_id_append

        self.cfg.fill_in_defaults()
        self.cfg.validate()

        self._always_verbose = always_verbose
        self.node_names = [f'{self.cfg.cluster_id}-node{i+1}' for i in range(self.cfg.num_instances)]
        self.cluster_sg_name = f'{self.cfg.cluster_id}-intracluster-ssh'
        self.placement_group_name = f'{self.cfg.cluster_id}-placement-group' if self.cfg.placement_group else None
        self.nodes = [
            EC2Node(
                node_name,
                self.cfg.region,
                self.cfg.vpc,
                self.cfg.subnet,
                self.cfg.ami,
                self.cfg.instance_type,
                self.cfg.keypair,
                self.cfg.security_groups,
                self.cfg.iam_role,
                placement_group_name=self.placement_group_name,
                ebs_size=self.cfg.ebs_size,
                ebs_device_name=self.cfg.ebs_device_name,
                ebs_type=self.cfg.ebs_type,
                ebs_iops=self.cfg.ebs_iops,
                ebs_throughput=self.cfg.ebs_throughput,
                tags=self.cfg.tags,
                always_verbose=self._always_verbose
            )
            for node_name in self.node_names
        ]  # type: List[EC2Node]

        self.session = boto3.session.Session(region_name=self.cfg.region)
        self.ec2_client = self.session.client("ec2")
        self.ec2_resource = self.session.resource("ec2")

        self._cluster_sg_id = None

    def __enter__(self):
        """Creates a fresh cluster which will be automatically cleaned up.

        Will raise exception if the cluster already exists.
        """
        if self.any_node_is_running_or_pending():
            raise RuntimeError(f"Cluster with name '{self.cfg.cluster_id}' already exists.")
        try:
            self.launch(verbose=True)
        except KeyboardInterrupt as e:
            print("\n\nKeyboardInterrupt, cleaning up resources before exiting")
            self.terminate(verbose=True, fast_terminate=True)
            raise e
        except Exception as e:
            print("Encountered error, cleaning up resources before exiting")
            self.terminate(verbose=True, fast_terminate=True)
            raise e
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Cleaning up resources before exiting")
        self.terminate(verbose=True, fast_terminate=True)

    def _get_vlog(self, force_verbose=False, prefix=None):
        def vlog_fn_verbose(s):
            out = "" if prefix is None else f'[{prefix}] '
            out += s
            print(out)

        def vlog_fn_noop(s):
            pass

        vlog_fn = vlog_fn_verbose if self._always_verbose or force_verbose else vlog_fn_noop
        return vlog_fn

    def get_shell(self, ssh_key_path=None, use_bastion=False, use_public_ips=True,
                  wait_for_ssh=True, wait_for_ssh_timeout=120):
        """
        Create a ClusterShell from a ConfigCluster.

        :param ssh_key_path: The path to the SSH key required to SSH into the EC2 instances. Often ~/.ssh/something.pem.
                             If param is None, will assume that the key is available at ~/.ssh/${KEY_PAIR_NAME}.pem
        :param use_bastion: Whether or not to use the master node as the bastion host for SSHing to worker nodes.
        :param use_public_ips: Whether to build the ClusterShell from the instances public IPs or private IPs.
                               Typically this should be True when running code on a laptop/local machine and False
                               when running on an EC2 instance
        :param wait_for_ssh: If true, block until commands can be run on all instances. This can be useful when
                             you are launching EC2 instances, because the instances may be in the RUNNING state
                             but the SSH daemon may not yet be running.
        :param wait_for_ssh_timeout: Number of seconds to spend trying to run commands on the instances before failing.
                                     This is NOT the SSH timeout, this upper bounds the amount of time spent retrying
                                     failed SSH connections. Only used if wait_for_ssh=True.
        :return: ClusterShell
        """

        ips = self.public_ips if use_public_ips else self.private_ips

        if ssh_key_path is None:
            ssh_key_path = Path(f"~/.ssh/{self.cfg.keypair}.pem").expanduser()

        sh = ClusterShell(username=self.cfg.username,
                          master_ip=ips[0],
                          worker_ips=ips[1:],
                          ssh_key_path=ssh_key_path,
                          use_bastion=use_bastion,
                          wait_for_ssh=wait_for_ssh,
                          wait_for_ssh_timeout=wait_for_ssh_timeout)
        return sh


    @property
    def instance_ids(self):
        """A list of InstanceIds for the nodes in the cluster.

        All nodes must be in RUNNING or PENDING stats. Always in the same order: [Master, Worker1, Worker2, etc...]
        """
        if not self.any_node_is_running_or_pending():
            raise RuntimeError("No nodes are running for this cluster!")
        return [node.instance_id for node in self.nodes]


    @property
    def private_ips(self):
        """The list of private IPs for the nodes in the cluster.

        All nodes must be in RUNNING or PENDING stats. Output is always in the same order: [Master, Worker1, Worker2, etc...]
        """
        if not self.any_node_is_running_or_pending():
            raise RuntimeError("No nodes are running for this cluster!")
        return [node.private_ip for node in self.nodes]

    @property
    def public_ips(self):
        """A list of public IPs for the nodes in the cluster.

        All nodes must be in RUNNING or PENDING stats. Output is always in the same order: [Master, Worker1, Worker2, etc...]
        """
        if not self.any_node_is_running_or_pending():
            raise RuntimeError("No nodes are running for this cluster!")
        return [node.public_ip for node in self.nodes]


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
            "master_public_ip": self.public_ips[0],
            "worker_public_ips": self.public_ips[1:],
            "master_private_ip": self.private_ips[0],
            "worker_private_ips": self.private_ips[1:]
        }

    @property
    def cluster_sg_id(self):
        """Return the Id of the ClusterSecurityGroup

        When cluster is launched, a security group is created to allow the nodes to communicate with each other. This
        is deleted when the cluster is terminated.

        Raise exception if the ClusterSecurityGroup doesn't exist.
        """
        if self._cluster_sg_id is None:
            if not self.security_group_exists(self.cluster_sg_name):
                raise RuntimeError(f'Cluster security group "{self.cluster_sg_name}" does not exist!')
            self._cluster_sg_id = self.get_security_group_id_from_name(self.cluster_sg_name)
        return self._cluster_sg_id


    def create_cluster_sg(self, vpc_id, verbose=False):
        """Create the ClusterSecurityGroup that allows nodes to communicate with each other.

        :param vpc_id: The Id of the VPC that the cluster is in.
        """

        response = self.ec2_client.create_security_group(
            Description=self.cluster_sg_name,
            GroupName=self.cluster_sg_name,
            VpcId=vpc_id,
        )
        self._cluster_sg_id = response['GroupId']

        while not self.security_group_exists(self.cluster_sg_name):
            time.sleep(1)

        sg = self.ec2_resource.SecurityGroup(self.cluster_sg_id)
        sg.authorize_ingress(SourceSecurityGroupName=self.cluster_sg_name)


    def delete_cluster_sg(self):
        """Create the ClusterSecurityGroup that allows nodes to communicate with each other.

        Args:
            vpc_id: The Id of the VPC that the cluster is in.
        """
        response = self.ec2_client.delete_security_group(
            GroupId=self.cluster_sg_id,
            GroupName=self.cluster_sg_name
        )

    def security_group_exists(self, sg_name):
        """Return True if the security group with the given name exists"""
        res = self.ec2_client.describe_security_groups(
            Filters=[
                {
                    'Name': 'group-name',
                    'Values': [
                        sg_name,
                    ]
                },
            ]
        )
        return len(res['SecurityGroups']) > 0

    def get_security_group_id_from_name(self, sg_name):
        """Get the security group id from the name"""
        res = self.ec2_client.describe_security_groups(
            Filters=[
                {
                    'Name': 'group-name',
                    'Values': [
                        sg_name,
                    ]
                },
            ]
        )
        return res['SecurityGroups'][0]['GroupId']



    def list_placement_groups(self):
        """List all placement groups

        Returns:
            placement_groups (list): List of ``{'GroupName': 'string', 'State': 'pending'|'available'|'deleting'|'deleted', 'Strategy': 'cluster'|'spread'}``
        """
        response = self.ec2_client.describe_placement_groups()
        return response["PlacementGroups"]

    def placement_group_exists(self):
        """Return True if cluster placement group exists"""
        return self.placement_group_name in [pg["GroupName"] for pg in self.list_placement_groups()]

    def create_placement_group_if_doesnt_exist(self):
        """Create the cluster placement group if it doesn't exist. Do nothing if already exists"""
        if not self.placement_group_exists():
            response = self.ec2_client.create_placement_group(
                GroupName=self.placement_group_name,
                Strategy='cluster'
            )

    def delete_placement_group(self):
        """Delete the cluster placement group"""
        if self.placement_group_exists():
            response = self.ec2_client.delete_placement_group(
                GroupName=self.placement_group_name
            )

    def any_node_is_running_or_pending(self):
        """Return True if any node is in RUNNING or PENDING states"""
        for ec2_node in self.nodes:
            if ec2_node.is_running_or_pending():
                return True
        return False

    def wait_for_all_nodes_to_be_running(self):
        """Blocks until all nodes are in the RUNNING state"""
        for ec2_node in self.nodes:
            ec2_node.wait_for_instance_to_be_running()

    def wait_for_all_nodes_to_be_status_ok(self):
        """Blocks until all nodes have passed the EC2 health check.

        Once nodes are status OK, you can SSH to them. See EC2Node.wait_for_instance_to_be_status_ok() for details.
        """
        for ec2_node in self.nodes:
            ec2_node.wait_for_instance_to_be_status_ok()

    def wait_for_all_nodes_to_be_terminated(self):
        """Blocks until all nodes are in the TERMINATED state"""
        for ec2_node in self.nodes:
            try:
                ec2_node.wait_for_instance_to_be_terminated()
            except Exception as ex:
                print("[wait_for_all_nodes_to_be_terminated] Some error while waiting for nodes to be terminated")
                print(f'[wait_for_all_nodes_to_be_terminated] {ex}')
                print("[wait_for_all_nodes_to_be_terminated] Assuming non-fatal error. Continuing")
                pass


    def launch(self,
               dry_run=False,
               wait_secs=10,
               verbose=True):
        """Launch the cluster nodes.

        Will repeatedly try to launch instances until all nodes are launched or the timeout is reached.

        Args:
            vpc_id: The id of the VPC to launch the cluster in, e.g. 'vpc-123456789'
            subnet_id: The id of the subnet to launch the cluster in, e.g. 'subnet-123456789'
            ami_id: The id AMI, e.g. 'ami-123456789'
            ebs_snapshot_id: The snapshot id of the EBS instance to attach, e.g. 'snapshot-123456789'
            volume_gbs: The size of the volume in GBs
            volume_type: The type of the EBS volume. If 'io1' must include iops argument
            key_name: The name of the EC2 KeyPair for SSHing into the instance
            security_group_ids: A list of security group ids to attach. Must be a non-empty list. The
                                ClusterSecurityGroup id will be added to this list
            iam_ec2_role_name: The name of the EC2 role. The name, not the ARN.
            instance_type: The API name of the instance type to launch, e.g. 'p3.16xlarge'
            use_placement_group: True to launch instances in a placement group
            iops: If volume_type == 'io1', the number of provisioned IOPS for the EBS volume.
            eia_type: Optional. The Elastic Inference Accelerator type, e.g. 'eia1.large'
            ebs_optimized: Whether to use an EBS optimized instance. Should basically always be True. Certain
                                    older instance types don't support EBS optimized instance or offer at a small fee.
            tags: List of custom tags to attach to the EC2 instance. List of dicts, each with a 'Key' and a 'Value'
                  field. Normal EC2 tag length restrictions apply. Key='Name' is reserved for EC2Node use.
            dry_run: True to make test EC2 API call that confirms syntax but doesn't actually launch the instance.
            timeout_secs: The maximum number of seconds to spend launching the cluster nodes before timing out. Pass in
                          None to never time out (None can be either the Python type or the string 'None')
            wait_secs: The number of seconds to wait before retrying launching a node.
            verbose: True to print out detailed information about progress.
        """
        vlog = self._get_vlog(verbose, 'EC2NodeCluster.launch')

        if self.any_node_is_running_or_pending():
            raise RuntimeError("Nodes with names matching this cluster already exist!")

        if self.security_group_exists(self.cluster_sg_name):
            vlog("Cluster security group already exists. No need to recreate")
        else:
            vlog("Creating cluster security group")
            self.create_cluster_sg(self.cfg.vpc, verbose=verbose)

        if self.cfg.placement_group:
            vlog("Creating placement group")
            self.create_placement_group_if_doesnt_exist()
        else:
            vlog("No placement group needed")

        for node in self.nodes:
            node.security_groups.append(self.cluster_sg_id)
            if self.cfg.placement_group:
                node.placement_group_name = self.placement_group_name

        start = time.time()
        for launch_ind, ec2_node in enumerate(self.nodes):
            while True:
                try:

                    ec2_node.launch(dry_run=dry_run)

                    vlog(f'Node {launch_ind+1} of {self.cfg.num_instances} successfully launched')
                    break
                except Exception as e:
                    vlog(f'Error launching node: {str(e)}')
                    vlog(f'EC2NodeCluster.launch TODO: Only repeat when the error is insufficient capacity.')

                    if self.cfg.timeout is not None and (time.time() - start) > self.cfg.timeout:
                        vlog(f'Timed out trying to launch node #{launch_ind+1}. Max timeout of {self.cfg.timeout} seconds reached')
                        vlog("Now trying to clean up partially launched cluster")
                        for terminate_ind, ec2_node_to_delete in enumerate(self.nodes):
                            try:
                                if terminate_ind >= launch_ind:
                                    break   # Don't try to shut down nodes that weren't launched.
                                vlog(f'Terminating node #{terminate_ind+1} of {self.cfg.num_instances}')
                                ec2_node_to_delete.detach_security_group(self.cluster_sg_id)
                                ec2_node_to_delete.terminate()
                                vlog(f'Node #{terminate_ind+1} successfully terminated')
                            except:
                                vlog(f'Error terminating node #{terminate_ind+1}')
                                vlog(str(e))

                        vlog("Deleting cluster SG")
                        self.delete_cluster_sg()
                        vlog("Now waiting for all nodes to reach TERMINATED state. May take some time.")
                        self.wait_for_all_nodes_to_be_terminated()
                        vlog("All nodes have been terminated!")
                        raise RuntimeError(f'EC2NodeCluster failed to launch. Last error while launching node was: "{str(e)}"')
                    else:

                        vlog(f'Retrying launch of node #{launch_ind+1} in {wait_secs} seconds.')
                        if self.cfg.timeout is None:
                            vlog(f'There is no timeout. Elapsed time trying to launch this node: {humanize_float(time.time() - start)} seconds')
                        else:
                            vlog(f'Will time out after {self.cfg.timeout} seconds. Current elapsed time: {humanize_float(time.time() - start)} seconds')
                        time.sleep(wait_secs)

        vlog("Now waiting for all nodes to reach RUNNING state")
        self.wait_for_all_nodes_to_be_running()
        vlog("All nodes are running!")


    def terminate(self, verbose=False, fast_terminate=False):
        """Terminate all nodes in the cluster and clean up security group and placement group

        Args:
            verbose: True to print out detailed information about progress.
            fast_terminate: If True, will not wait for the nodes to be shut down, instead returning as soon as the node
                            termination has been triggered. NOTE: If this mode is chosen and the nodes were launched
                            into a placement group, the placement group will not be deleted (a placement group is a
                            logical EC2 resource that has no cost associated with it)
        """
        vlog = self._get_vlog(verbose, 'EC2NodeCluster.terminate')

        if not self.any_node_is_running_or_pending():
            vlog("No nodes exist to terminate")
        else:
            for i, ec2_node in enumerate(self.nodes):
                ec2_node.detach_security_group(self.cluster_sg_id)
                ec2_node.terminate()
                vlog(f'Node {i + 1} of {self.cfg.num_instances} successfully triggered deletion')

        if self.security_group_exists(self.cluster_sg_name):
            self.delete_cluster_sg()
            vlog("Cluster SG deleted")
        else:
            vlog(f"Cluster SG ({self.cluster_sg_name}) does not exist")

        if fast_terminate:
            return

        vlog("Waiting for all nodes to reach terminated state")
        self.wait_for_all_nodes_to_be_terminated()

        if self.placement_group_exists():
            self.delete_placement_group()
            vlog("Placement group deleted!")


