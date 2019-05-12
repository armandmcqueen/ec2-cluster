import boto3
import json
import yaml
import os
import time

def humanize_float(num):
    return "{0:,.2f}".format(num)



class AttrDict(dict):
    """
    Class for working with dicts using dot notation
    """
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

    def __str__(self):
        return json.dumps(self.__dict__, indent=4)



class EC2Node:
    """A class managing a single EC2 instance.

    Allows you to launch, describe and terminate an EC2 instance. Also has convenience methods for waiting for the
    instance to reach a certain state, e.g. wait for status OK, after which you can SSH to the instance.

    This class is designed for managing long-running jobs without an always-on control plane. In order to do this, each
    ``EC2Node``-managed instance in an AWS region has a unique Name (the value of the 'Name' tag). When you instantiate an
    ``EC2Node``, you pass in this Name, which allows the code to query the EC2 API to see if that instance already exists
    in EC2.

    This is generally an easy, intuitive way to keep track of which node is which across sessions. However, this means
    you have to careful with your node Names to ensure that there aren't accidental collisions, e.g. two teammates pick
    the Name *test-node* and they end up both trying to control the same EC2 instance.

    ``EC2Node`` expects that only one person is trying to control an instance at a time. Behavior is unknown when there
    are multiple EC2 instances with the same Name (that should never happen when using ``EC2Node``).

    ``EC2Node`` only queries the EC2 API for RUNNING or PENDING nodes. That that means nodes outside of those states are
    invisible.
        - ``EC2Node`` will not being able to wait for a node to be in TERMINATED state if you did not query the EC2 API
          for the InstanceId before it entered the SHUTTING-DOWN state.
        - ``EC2Node`` will completely ignore any STOPPED nodes. Can lead to duplicate Names if the STOPPED nodes are then
          started manually.
    """

    def __init__(self, name, region, always_verbose=False):
        """
        Args:
            name: The unique Name of the ``EC2Node``
            region: The AWS region
            always_verbose (bool): True if you want all ``EC2Node`` functions to run in verbose mode.
        """
        self.name = name
        self.region = region

        self.session = boto3.session.Session(region_name=region)
        self.ec2_client = self.session.client("ec2")
        self.ec2_resource = self.session.resource("ec2")

        # Instance information retrieved from EC2 API. Lazy loaded
        self._instance_info = None
        self._always_verbose = always_verbose




    # Retrieves info from AWS APIs
    def _lazy_load_instance_info(self):
        if not self._instance_info:
            instance_info = self.query_for_instance_info()
            if instance_info is None:
                raise RuntimeError("Could not find info for instance. Perhaps it is not in 'RUNNING' "
                                   "or 'PENDING' state?")
            self._instance_info = instance_info


    @property
    def instance_id(self):
        """The EC2 InstanceId.

        Retrieved by calling the EC2 API. Will use a cached response if it has already called the API. Instance must be
        in the RUNNING or PENDING states.
        """
        self._lazy_load_instance_info()
        return self._instance_info["InstanceId"]


    @property
    def private_ip(self):
        """The private IP of the instance.

        Retrieved by calling the EC2 API. Will use a cached response if it has already called the API. Instance must be
        in the RUNNING or PENDING states.
        """
        self._lazy_load_instance_info()
        return self._instance_info["PrivateIpAddress"]

    @property
    def public_ip(self):
        """The public IP of the instance.

        Retrieved by calling the EC2 API. Will use a cached response if it has already called the API. Instance must be
        in the RUNNING or PENDING states.

        Will return None if instance does not have a public IP.
        """
        self._lazy_load_instance_info()
        return self._instance_info["PublicIpAddress"] if "PublicIpAddress" in self._instance_info.keys() else None

    @property
    def security_groups(self):
        """The list of security groups attached to the instance.

        Retrieved by calling the EC2 API. Will use a cached response if it has already called the API. Instance must be
        in the RUNNING or PENDING states.

        Returns a list of security group ids.
        """

        self._lazy_load_instance_info()
        return [sg["GroupId"] for sg in self._instance_info["SecurityGroups"]]



    def detach_security_group(self, sg_id):
        """Remove a security group from the instance.

        Instance must be in the RUNNING or PENDING states. No effect, no exception if the security group is not already
        attached to the instance.
        """

        if not self.is_running_or_pending():
            raise RuntimeError("Cannot remove security group if the instance isn't running")

        new_sgs = [sg for sg in self.security_groups if sg != sg_id]
        self.ec2_client.modify_instance_attribute(InstanceId=self.instance_id, Groups=new_sgs)


    def query_for_instance_info(self):
        """Retrieve instance info for any EC2 node in the RUNNING or PENDING state that has the correct 'Name' tag.

        Returns None if no such instance exists. Otherwise returns information in the form returned by
        `describe_instances <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.describe_instances>`_.
        Specifically, returns ``response["Reservations"][0]["Instances"][0]``
        """

        response = self.ec2_client.describe_instances(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [
                        self.name,
                    ]
                },
                {
                    'Name': 'instance-state-name',
                    'Values': [
                        'running',
                        'pending'
                    ]
                },
            ]
        )

        exists = len(response["Reservations"]) > 0
        if not exists:
            return None

        instance_info = response['Reservations'][0]['Instances'][0]
        return instance_info


    def is_running_or_pending(self):
        """Check the EC2 API to see if the instance is in the RUNNING or PENDING states"""
        return self.is_in_state(['running', 'pending'])



    def is_in_state(self, states):
        """Call the EC2 API to see if the instance is in any of the given states.

        Args:
            states: The list of states. Options are: 'pending'|'running'|'shutting-down'|'terminated'|'stopping'|'stopped'.
                    Can be a string if only checking a single state
        Returns:
            bool: True if the instance exists and is in any of those states
        """
        if isinstance(states, str):
            states = [states]

        response = self.ec2_client.describe_instances(
                Filters=[
                    {
                        'Name': 'tag:Name',
                        'Values': [
                            self.name,
                        ]
                    },
                    {
                        'Name': 'instance-state-name',
                        'Values': states
                    },
                ]
        )

        exists = len(response["Reservations"]) > 0
        return exists


    def wait_for_instance_to_be_running(self):
        """Block until the the instance reaches the RUNNING state.

        Will raise exception if non-RUNNING terminal state is reached (e.g. the node is TERMINATED) or if it times out.
        Uses the default timeout, which as of 2019-02-11, was 600 seconds.
        """

        waiter = self.ec2_client.get_waiter('instance_running')
        waiter.wait(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [
                        self.name,
                    ]
                }
            ]
        )


    def wait_for_instance_to_be_status_ok(self):
        """Block until the the instance reaches the OK status.

        Note: status is not the same as state. Status OK means the `health check
        <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/monitoring-system-instance-status-check.html>`_ that EC2
        does showed no issues.


        Status OK is important because it is an indicator that the instance is ready to receive SSH connections, which
        may not be true immediately after entering the RUNNING state, but prior to having Status OK.
        """
        waiter = self.ec2_client.get_waiter('instance_status_ok')
        waiter.wait(InstanceIds=[self.instance_id])

    def wait_for_instance_to_be_terminated(self):
        """Block until the the instance reaches the TERMINATED state.

        Will raise exception if it times out. Uses the default timeout, which as of 2019-02-11, was 600 seconds. May
        raise exception if non-TERMINATED terminal state is reached (e.g. the node is RUNNING). Haven't checked.
        """
        waiter = self.ec2_client.get_waiter('instance_terminated')
        waiter.wait(
            Filters=[
                {
                    'Name': 'instance-id',
                    'Values': [
                        self.instance_id,
                    ]
                }
            ]
        )


    def launch(self,
               az,
               vpc_id,
               subnet_id,
               ami_id,
               ebs_snapshot_id,
               volume_size_gb,
               volume_type,
               key_name,
               security_group_ids,
               iam_ec2_role_name,
               instance_type,
               placement_group_name=None,
               iops=None,
               eia_type=None,
               ebs_optimized=True,
               tags=None,
               dry_run=False):

        """Launch an instance.

        Raises exception if instance with the given Name is already RUNNING or PENDING.

        :param az: The availability zone, e.g. 'us-east-1f'
        :param vpc_id: The id of the VPC, e.g. 'vpc-123456789'
        :param subnet_id: The id of the subnet, e.g. 'subnet-123456789'
        :param ami_id: The id of the AMI, e.g. 'ami-123456789'
        :param ebs_snapshot_id: The id of the EBS snapshot, e.g. 'snapshot-123456789'. May not be required, unconfirmed.
        :param volume_size_gb: The size of the EBS volume in GBs.
        :param volume_type: The type of the EBS volume. If type is 'io1', must pass in iops argument
        :param key_name: The name of the EC2 KeyPair for SSHing into the instance
        :param security_group_ids: A list of security group ids to attach. Must be a non-empty list
        :param iam_ec2_role_name: The name of the EC2 role. The name, not the ARN.
        :param instance_type: The API name of the instance type to launch, e.g. 'p3.16xlarge'
        :param placement_group_name: Optional. The name of a placement group to launch the instance into.
        :param iops: If volume_type == 'io1', the number of provisioned IOPS for the EBS volume.
        :param eia_type: Optional. The Elastic Inference Accelerator type, e.g. 'eia1.large'
        :param ebs_optimized: Whether to use an EBS optimized instance. Should basically always be True. Certain older
                              instance types don't support EBS optimized instance or offer at a small fee.
        :param tags: List of custom tags to attach to the EC2 instance. List of dicts, each with a 'Key' and a 'Value'
                     field. Normal EC2 tag length restrictions apply. Key='Name' is reserved for EC2Node use.
        :param dry_run: True to make test EC2 API call that confirms syntax but doesn't actually launch the instance.
        :return: EC2 API response in format return by `run_instances <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.run_instances>`_
        """

        #############################################
        # Validate input
        #############################################

        assert isinstance(security_group_ids, list), "security_group_ids must be a nonempty list"
        assert len(security_group_ids) > 0, "security_group_ids must be a nonempty list"

        if eia_type is not None:
            assert eia_type.startswith("eia1"), "eia_type must be in the form `eia1.large`"

        if volume_type == 'io1':
            assert iops is not None

        if tags:
            assert isinstance(tags, list), "Tags must be a list if not None"
            assert len(tags) != 0, "Tags cannot be an empty list. Use None instead"
            for tag in tags:
                assert isinstance(tag, dict), "Elements in tags must be dicts"
                assert 'Key' in tag.keys(), "Each tag must have both a 'Key' and a 'Value' field. 'Key' missing"
                assert tag['Key'] != "Name", "'Name' tag cannot be included as a tag. It will be set according to " \
                                             "the Name defined at instantiation"
                assert 'Value' in tag.keys(), "Each tag must have both a 'Key' and a 'Value' field. 'Value' missing"



        #############################################
        # Convert input to match SDK argument syntax
        #############################################

        # Optional placement group
        placement_params = {"AvailabilityZone": az}
        if placement_group_name is not None:
            placement_params["GroupName"] = placement_group_name

        # EIA
        if eia_type is None:
            eia_param_list = []
        else:
            eia_param_list = [{'Type': eia_type}]

        # Tags
        all_tags = [{'Key': 'Name', 'Value': self.name}]
        if tags:
            all_tags += tags

        # EBS
        ebs_params = {
            'SnapshotId': ebs_snapshot_id,
            'VolumeSize': volume_size_gb,
            'VolumeType': volume_type
        }

        if iops:
            ebs_params['Iops'] = iops


        ########################################################
        # Ensure there are never two nodes with the same name
        ########################################################

        if self.is_running_or_pending():
            raise RuntimeError(f'Instance with Name {self.name} already exists')


        #############################################
        # Make the API call
        #############################################

        response = self.ec2_client.run_instances(
            BlockDeviceMappings=[
                {
                    'DeviceName': "/dev/xvda",
                    'Ebs': ebs_params,
                },
            ],
            ImageId=ami_id,
            InstanceType=instance_type,
            KeyName=key_name,
            MaxCount=1,
            MinCount=1,
            Monitoring={
                'Enabled': False
            },
            Placement=placement_params,
            SecurityGroupIds=security_group_ids,
            SubnetId=subnet_id,
            DryRun=dry_run,
            EbsOptimized=ebs_optimized,
            IamInstanceProfile={
                'Name': iam_ec2_role_name
            },
            ElasticInferenceAccelerators=eia_param_list,
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': all_tags
                },
            ]
        )
        return response

    def terminate(self, dry_run=False):
        """Terminate the instance.

        After triggering termination, removes the 'Name' tag from the instance, which allows you to immediately launch a
        new node with the same Name.

        Args:
            dry_run (bool): Make EC2 API call as a test.
        """
        instance_id = self.instance_id
        response = self.ec2_client.terminate_instances(
            InstanceIds=[
                instance_id,
            ],
            DryRun=dry_run
        )

        instance = self.ec2_resource.Instance(instance_id)

        instance.delete_tags(Tags=[{'Key': 'Name'}])












class EC2NodeCluster:
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
    def __init__(self,
                 node_count,
                 cluster_name,
                 region,
                 always_verbose=False):
        """
        Args:
            node_count: Number of nodes in the cluster
            cluster_name: The unique name of the cluster.
            region: The AWS region
            always_verbose: True to force all EC2NodeCluster methods to run in verbose mode
        """

        self._always_verbose = always_verbose

        self.node_count = node_count
        self.region = region
        self.cluster_name = cluster_name
        self.node_names = [f'{self.cluster_name}-node{i+1}' for i in range(node_count)]
        self.cluster_sg_name = f'{self.cluster_name}-intracluster-ssh'
        self.cluster_placement_group_name = f'{self.cluster_name}-placement-group'  # Defined, but might not be used

        self.nodes = [EC2Node(node_name, self.region)
                      for node_name in self.node_names]

        self.session = boto3.session.Session(region_name=region)
        self.ec2_client = self.session.client("ec2")
        self.ec2_resource = self.session.resource("ec2")

        self._cluster_sg_id = None


    def _get_vlog(self, force_verbose=False, prefix=None):
        def vlog_fn_verbose(s):
            out = "" if prefix is None else f'[{prefix}] '
            out += s
            print(out)

        def vlog_fn_noop(s):
            pass

        vlog_fn = vlog_fn_verbose if self._always_verbose or force_verbose else vlog_fn_noop
        return vlog_fn

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


    def create_cluster_sg(self, vpc_id):
        """Create the ClusterSecurityGroup that allows nodes to communicate with each other.

        :param vpc_id: The Id of the VPC that the cluster is in.
        """
        if self.security_group_exists(self.cluster_sg_name):
            print("Cluster SG already exists. No need to recreate")
            return

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
        return self.cluster_placement_group_name in [pg["GroupName"] for pg in self.list_placement_groups()]

    def create_placement_group_if_doesnt_exist(self):
        """Create the cluster placement group if it doesn't exist. Do nothing if already exists"""
        if not self.placement_group_exists():
            response = self.ec2_client.create_placement_group(
                GroupName=self.cluster_placement_group_name,
                Strategy='cluster'
            )

    def delete_placement_group(self):
        """Delete the cluster placement group"""
        if self.placement_group_exists():
            response = self.ec2_client.delete_placement_group(
                GroupName=self.cluster_placement_group_name
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
               az,
               vpc_id,
               subnet_id,
               ami_id,
               ebs_snapshot_id,
               volume_gbs,
               volume_type,
               key_name,
               security_group_ids,
               iam_ec2_role_name,
               instance_type,
               use_placement_group=False,
               iops=None,
               eia_type=None,
               ebs_optimized=True,
               tags=None,
               dry_run=False,
               timeout_secs=None,
               wait_secs=10,
               verbose=True):
        """Launch the cluster nodes.

        Will repeatedly try to launch instances until all nodes are launched or the timeout is reached.

        Args:
            az: The az to launch the cluster in, e.g. 'us-east-1f'
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
            timeout_secs: The maximum number of seconds to spend launching the cluster nodes before timing out. None to
                          never time out.
            wait_secs: The number of seconds to wait before retrying launching a node.
            verbose: True to print out detailed information about progress.
        """

        vlog = self._get_vlog(verbose, 'EC2NodeCluster.launch')

        if self.any_node_is_running_or_pending():
            raise RuntimeError("Nodes with names matching this cluster already exist!")

        vlog("Creating cluster SG if needed")
        self.create_cluster_sg(vpc_id)
        security_group_ids.append(self.cluster_sg_id)

        if use_placement_group:
            vlog("Creating placement group")
            self.create_placement_group_if_doesnt_exist()
        else:
            vlog("No placement group needed")

        start = time.time()
        for launch_ind, ec2_node in enumerate(self.nodes):
            while True:

                vlog("-----")
                try:

                    ec2_node.launch(az,
                                    vpc_id,
                                    subnet_id,
                                    ami_id,
                                    ebs_snapshot_id,
                                    volume_gbs,
                                    volume_type,
                                    key_name,
                                    security_group_ids,
                                    iam_ec2_role_name,
                                    instance_type,
                                    placement_group_name=self.cluster_placement_group_name if use_placement_group else None,
                                    iops=iops,
                                    eia_type=eia_type,
                                    ebs_optimized=ebs_optimized,
                                    tags=tags,
                                    dry_run=dry_run)

                    vlog(f'Node {launch_ind+1} of {self.node_count} successfully launched')
                    break
                except Exception as e:
                    vlog(f'Error launching node: {str(e)}')
                    vlog(f'EC2NodeCluster.launch TODO: Only repeat when the error is insufficient capacity.')

                    if timeout_secs is not None and (time.time() - start) > timeout_secs:
                        vlog(f'Timed out trying to launch node #{launch_ind+1}. Max timeout of {timeout_secs} seconds reached')
                        vlog("Now trying to clean up partially launched cluster")
                        for terminate_ind, ec2_node_to_delete in enumerate(self.nodes):
                            vlog("-----")
                            try:
                                if terminate_ind >= launch_ind:
                                    break   # Don't try to shut down nodes that weren't launched.
                                vlog(f'Terminating node #{terminate_ind+1} of {self.node_count}')
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
                        if timeout_secs is None:
                            vlog(f'There is no timeout. Elapsed time trying to launch this node: {humanize_float(time.time() - start)} seconds')
                        else:
                            vlog(f'Will time out after {timeout_secs} seconds. Current elapsed time: {humanize_float(time.time() - start)} seconds')
                        time.sleep(wait_secs)

        vlog("-----")
        vlog("Now waiting for all nodes to reach RUNNING state")
        self.wait_for_all_nodes_to_be_running()
        vlog("All nodes are running!")


    def terminate(self, verbose=False):
        """Terminate all nodes in the cluster and clean up security group and placement group

        Args:
            verbose: True to print out detailed information about progress.
        """
        vlog = self._get_vlog(verbose, 'EC2NodeCluster.terminate')

        if not self.any_node_is_running_or_pending():
            vlog("No nodes exist to terminate")
        else:
            for i, ec2_node in enumerate(self.nodes):
                vlog("-----")
                ec2_node.detach_security_group(self.cluster_sg_id)
                ec2_node.terminate()
                vlog(f'Node {i + 1} of {self.node_count} successfully triggered deletion')
            vlog("-----")
            vlog("Waiting for all nodes to reach terminated state")
            self.wait_for_all_nodes_to_be_terminated()

        if self.security_group_exists(self.cluster_sg_id):
            self.delete_cluster_sg()
            vlog("Cluster SG deleted")

        if self.placement_group_exists():
            self.delete_placement_group()
            vlog("Placement group deleted!")







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
        maybe_nonexistant_params = ["iops", 'ebs_optimized', 'additional_tags', 'placement_group']

        for p in self.paramdef_list:
            param_name = p["param_name"]
            if param_name in maybe_nonexistant_params:
                continue

            assert param_name in config_dict.keys(), f'Mandatory argument {param_name} is missing'
            assert config_dict[param_name] is not None, f'Mandatory argument {param_name} is None'


        # iops special case. Must be set when ebs_type=="io1"
        if config_dict["volume_type"] == "io1":
            assert "iops" in config_dict.keys(), f'When volume_type==io1, iops must be defined. Currently missing'
            assert config_dict["iops"] is not None, f'When volume_type==io1, iops must be defined. Currently None'
        else:
            config_dict["iops"] = None

        # ebs_optimized_instances special case. Defaults to True
        if "ebs_optimized" not in config_dict.keys() or config_dict["ebs_optimized"] is None:
            config_dict["ebs_optimized"] = True

        # additional_tags special case. Defaults to None as expected by EC2NodeCluster
        if "additional_tags" not in config_dict.keys() or config_dict["additional_tags"] is None:
            config_dict["additional_tags"] = None

        # placement_group special case. Defaults to False
        if "placement_group" not in config_dict.keys() or config_dict["placement_group"] is None:
            config_dict["placement_group"] = False


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
                            security_group_ids=self.config.security_group_ids,
                            iam_ec2_role_name=self.config.iam_ec2_role_name,
                            instance_type=self.config.instance_type,
                            use_placement_group=self.config.placement_group,
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


