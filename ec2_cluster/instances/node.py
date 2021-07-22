import boto3




class EC2Node:
    """A class managing a single EC2 instance.

        This is a class for working with individual instances as objects. It can be user-facing, but it is
        not the primary way to interact with instances.

        We are changing how tags work to make unintentional conflicts less frequent.
        - Name is an important tag for AWS console usability, but shouldn't be relied on for queries
        - One tag is cluster-specific that can be used to find all instances created by ec3
        - One set of tags is cluster-specific and can be used to query all instances in the cluster
        - One tag is instance specific

        A cluster is defined as a set of identical instances in the same location. They must have identical:
        - Region
        - VPC
        - KeyPair
        - AMI
        - Username (dependent on the AMI)
        - InstanceType
        - IAM role
        - Security group ids
        - Cluster Name
        Possibly the same, maybe not:
        - Subnet ID
        - EBS Snapshot ID
        Things that may change over time
        - Number of instances
        - Everything else requires a tear-down and recreation

        ## Tags

        managed-by: ec3
        ec3-cluster-name:
        ec3-node-name:
        Name:
        """



    def __init__(
            self,
            name,
            region,
            vpc,
            subnet,
            ami,
            instance_type,
            keypair,
            security_group_ids,
            iam_role_name,
            placement_group_name=None,
            volume_size_gb=200,
            volume_type='gp3',
            volume_iops=3000,
            volume_throughput=None,
            # ebs_optimized=True,
            tags=None,
            always_verbose=False
    ):
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
            :param ebs_optimized: Whether to use an EBS optimized instance. Should basically always be True. Certain older
                                  instance types don't support EBS optimized instance or offer at a small fee.
            :param tags: List of custom tags to attach to the EC2 instance. List of dicts, each with a 'Key' and a 'Value'
                         field. Normal EC2 tag length restrictions apply. Key='Name' is reserved for EC2Node use.
            :param dry_run: True to make test EC2 API call that confirms syntax but doesn't actually launch the instance.
            :return: EC2 API response in format return by `run_instances <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.run_instances>`_
            """

        self.name = name
        self.region = region
        self.vpc = vpc
        self.subnet = subnet
        self.ami = ami
        self.instance_type = instance_type
        self.keypair = keypair
        self.security_group_ids = security_group_ids
        self.iam_role_name = iam_role_name
        self.placement_group_name = placement_group_name
        self.volume_size_gb = volume_size_gb
        self.volume_type = volume_type
        self.volume_iops = volume_iops
        self.volume_throughput = volume_throughput

        # TODO: Correctly determine if it should be EBS optimized
        self.ebs_optimized = True
        self.tags = tags

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


    def launch(self, dry_run=False):

        # TODO: Move validation to init
        #############################################
        # Validate input
        #############################################

        assert isinstance(self.security_group_ids, list), "security_group_ids must be a nonempty list"
        assert len(self.security_group_ids) > 0, "security_group_ids must be a nonempty list"

        if self.volume_type == 'io1':
            assert self.volume_iops is not None

        # TODO: Validate volume throughput makes sense (is it required for gp3?)
        # TODO: Validate iops isn't present in cases where it can't be set

        if self.tags:
            assert isinstance(self.tags, list), "Tags must be a list if not None"
            assert len(self.tags) != 0, "Tags cannot be an empty list. Use None instead"
            for tag in self.tags:
                assert isinstance(tag, dict), "Elements in tags must be dicts"
                assert 'Key' in tag.keys(), "Each tag must have both a 'Key' and a 'Value' field. 'Key' missing"
                assert tag['Key'] != "Name", "'Name' tag cannot be included as a tag. It will be set according to " \
                                             "the Name defined at instantiation"
                assert 'Value' in tag.keys(), "Each tag must have both a 'Key' and a 'Value' field. 'Value' missing"



        #############################################
        # Convert input to match SDK argument syntax
        #############################################

        # Optional placement group
        # placement_params = {"AvailabilityZone": az}
        placement_params = {}
        if self.placement_group_name is not None:
            placement_params["GroupName"] = self.placement_group_name

        # EIA
        # if eia_type is None:
        #     eia_param_list = []
        # else:
        #     eia_param_list = [{'Type': eia_type}]

        # Tags
        all_tags = [{'Key': 'Name', 'Value': self.name}]
        if self.tags:
            all_tags += self.tags

        # EBS
        ebs_params = {
            # 'SnapshotId': ebs_snapshot_id,
            'VolumeSize': self.volume_size_gb,
            'VolumeType': self.volume_type
        }

        if self.volume_iops:
            ebs_params['Iops'] = self.volume_iops

        if self.volume_throughput:
            ebs_params['Throughput'] = self.volume_throughput

        iam_instance_profile = {}
        if self.iam_role_name is not None:
            iam_instance_profile['Name'] = self.iam_role_name


        ########################################################
        # Ensure there are never two nodes with the same name
        ########################################################

        if self.is_running_or_pending():
            raise RuntimeError(f'Instance with Name {self.name} already exists')


        #############################################
        # Make the API call
        #############################################

        response = self.ec2_client.run_instances(
            # BlockDeviceMappings=[
            #     {
            #         'DeviceName': "/dev/xvda",
            #         'Ebs': ebs_params,
            #     },
            # ],
            ImageId=self.ami,
            InstanceType=self.instance_type,
            KeyName=self.keypair,
            MaxCount=1,
            MinCount=1,
            Monitoring={
                'Enabled': False
            },
            Placement=placement_params,
            SecurityGroupIds=self.security_group_ids,
            SubnetId=self.subnet,
            DryRun=dry_run,
            IamInstanceProfile=iam_instance_profile,
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

