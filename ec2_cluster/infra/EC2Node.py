import boto3
import json



class EC2Node:

    def __init__(self, name, region, always_verbose=False):
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
        self._lazy_load_instance_info()
        return self._instance_info["InstanceId"]


    @property
    def private_ip(self):
        self._lazy_load_instance_info()
        return self._instance_info["PrivateIpAddress"]

    @property
    def public_ip(self):
        """
        Return the public ip of the instance if available. Instance must exist and be in the PENDING or RUNNING state

        DEV: Can there be multiple public IPs? What is behavior in that case?
        :return: Public IP string or None if not assigned public IP
        """
        self._lazy_load_instance_info()
        return self._instance_info["PublicIpAddress"] if "PublicIpAddress" in self._instance_info.keys() else None

    @property
    def security_groups(self):
        self._lazy_load_instance_info()
        return [sg["GroupId"] for sg in self._instance_info["SecurityGroups"]]



    def detach_security_group(self, sg_id):
        if not self.is_running_or_pending():
            raise RuntimeError("Cannot remove security group if the instance isn't running")

        new_sgs = [sg for sg in self.security_groups if sg != sg_id]
        self.ec2_client.modify_instance_attribute(InstanceId=self.instance_id, Groups=new_sgs)


    def query_for_instance_info(self, dry_run=False):
        # Returns None if there is no instance with this name in RUNNING or PENDING state
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
            ],
            DryRun=dry_run,
        )

        exists = len(response["Reservations"]) > 0
        if not exists:
            return None

        instance_info = response['Reservations'][0]['Instances'][0]
        return instance_info


    def is_running_or_pending(self, dry_run=False):
        return self.is_in_state(['running', 'pending'])


    # 'pending'|'running'|'shutting-down'|'terminated'|'stopping'|'stopped'
    def is_in_state(self, states):
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


    def wait_for_instance_to_be_running(self, dry_run=False):
        waiter = self.ec2_client.get_waiter('instance_running')
        waiter.wait(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [
                        self.name,
                    ]
                }
            ],
            DryRun=dry_run
        )

    # This is an indicator that the node can be SSHed to
    def wait_for_instance_to_be_status_ok(self, dry_run=False):
        waiter = self.ec2_client.get_waiter('instance_status_ok')
        waiter.wait(
            InstanceIds=[self.instance_id],
            DryRun=dry_run
        )

    def wait_for_instance_to_be_terminated(self, dry_run=False):
        waiter = self.ec2_client.get_waiter('instance_terminated')
        waiter.wait(
            Filters=[
                {
                    'Name': 'instance-id',
                    'Values': [
                        self.instance_id,
                    ]
                }
            ],
            DryRun=dry_run
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

        """

        :param az:
        :param vpc_id:
        :param subnet_id:
        :param ami_id:
        :param ebs_snapshot_id:
        :param iops:
        :param volume_size_gb:
        :param volume_type:
        :param key_name:
        :param security_group_ids:
        :param iam_ec2_role_name:
        :param instance_type:
        :param placement_group_name:
        :param eia_type:
        :param ebs_optimized:
        :param tags: List of dicts with shape [{'Key': key1, 'Value': val1}, {'Key': key2, 'Value': val2}]
        :param dry_run:
        :return:
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
                                             "the name defined at instantiation"
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
            raise RuntimeError(f'Instance with name {self.name} already exists')


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
        instance_id = self.instance_id
        response = self.ec2_client.terminate_instances(
            InstanceIds=[
                instance_id,
            ],
            DryRun=dry_run
        )

        instance = self.ec2_resource.Instance(instance_id)

        # Unname the instance. Allows you to spin up a new node with the same name while the old one shuts down
        instance.delete_tags(Tags=[{'Key': 'Name'}])





