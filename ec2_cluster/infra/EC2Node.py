import boto3
import json



class EC2Node:
    """Class wrapping AWS SDK to manage EC2 instances.

    Each node has a unique name that is stored in the 'Name' tag in EC2 when it is created. This allows
    you to control the same instance across sessions (e.g. launch a script to create a node and then the
    next day launch another script that deletes the node).

    When creating an instance of the class, you describe the attributes of the instance (AMI id, EBS size, security
    groups, etc.). When launching an instance, the instance is guaranteed to have those attributes. If not launching
    a new instance (e.g. using this code to get info about the instance or to attach a new security group), this code
    does not make any effort to ensure that the instance currently running matches the description.

    Attributes:
        name: Unique name identifying the EC2 instance.
        region: The AWS region you are working in, e.g. 'us-west-1'.
        az: The availability zone to launch instances in, e.g. 'us-west-1a'.
        vpc_id: The id of the VPC to launch instances in, e.g. 'vpc-123456789'. The location of the VPC must
                match the region.
        subnet_id: The id of the subnet to launch instance in, e.g. 'subnet-1234567890'.
        ami_id: The id of the AMI to launch, e.g. 'ami-123456789'.
        ebs_snapshot_id: The id of the EBS snapshot associated with the AMI. Must not be necessary?
        iops: If using the 'io1' EBS type, the number of IOPS to provision.
        volume_size_gb: The size of the EBS volume to attach to the instance. May not work correctly?
        volume_type: The EBS type, e.g. 'io1' or 'gp2'
        key_name: The name of the EC2 KeyPair to associate with the instance.
        security_group_ids: A (possibly empty) list of security groups to attach to the instance on launch,
                            e.g. ['sg-123456789'].
        iam_ec2_role_name: The name of the IAM role to attach to the instance.
        instance_type: The API name of the instance type to launch, e.g. p3.16xlarge.
        placement_group_name: Optional. The name of the placement group to launch the instance into.
        eia_type: Optional. The type of Elastic Inference Accelerator to attach to the instance, e.g. 'eia1.large'
    """

    def __init__(self,
                 unique_name,
                 region,
                 az,
                 vpc_id,
                 subnet_id,
                 ami_id,
                 ebs_snapshot_id,
                 iops,
                 volume_size_gb,
                 volume_type,
                 key_name,
                 security_group_ids,
                 iam_ec2_role_name,
                 instance_type,
                 placement_group_name=None,
                 eia_type=None):

        self.session = boto3.session.Session(region_name=region)
        self.ec2_client = self.session.client("ec2")
        self.ec2_resource = self.session.resource("ec2")

        self.name = unique_name
        self.region = region
        self.az = az

        self.vpc_id = vpc_id
        self.subnet_id = subnet_id
        self.ami_id = ami_id
        self.ebs_snapshot_id = ebs_snapshot_id
        self.iops = iops
        self.volume_size_gb = volume_size_gb
        self.volume_type = volume_type
        self.key_name = key_name
        self.security_group_ids = security_group_ids
        self.iam_ec2_role_name = iam_ec2_role_name
        self.instance_type = instance_type

        self.placement_group_name = placement_group_name

        if eia_type is not None:
            assert eia_type.startswith("eia1"), "eia_type must be in the form `eia1.large`"
        self.eia_type = eia_type

        self._instance_id = None
        self._private_ip = None
        self._public_ip = None



    def instance_id(self):
        if self._instance_id is None:
            self._load_instance_info()
        return self._instance_id

    def private_ip(self):
        if not self.is_running_or_pending():
            raise RuntimeError("Node is not running or pending!")

        if self._private_ip is None:
            self._load_instance_info()
        return self._private_ip

    def public_ip(self):
        if not self.is_running_or_pending():
            raise RuntimeError("Node is not running or pending!")

        if self._public_ip is None:
            self._load_instance_info()
        return self._public_ip

    def _load_instance_info(self):
        instance_info = self.instance_info()
        if instance_info is None:
            raise RuntimeError("Could not find info for instance. Perhaps it is not in 'running' or 'pending' state?")
        self._instance_id = instance_info["InstanceId"]
        self._private_ip = instance_info["PrivateIpAddress"]
        self._public_ip = instance_info["PublicIpAddress"] if "PublicIpAddress" in instance_info.keys() else None

    def add_sg(self, sg_id):
        # print(f'Adding sg {sg_id} to node {self.name}. Existing sgs = {self.security_group_ids}')
        self.security_group_ids += [sg_id]

    def add_sg_list(self, sg_id_list):
        # print(f'Adding sg list {sg_id_list} to node {self.name}. Existing sgs = {self.security_group_ids}')
        self.security_group_ids += sg_id_list

    def detach_security_group(self, sg_id):
        self.security_group_ids = [sg for sg in self.security_group_ids if sg != sg_id]
        if not self.is_running_or_pending():
            raise RuntimeError("Cannot remove security group if the instance isn't running")

        self.ec2_client.modify_instance_attribute(InstanceId=self.instance_id(), Groups=self.security_group_ids)

    def instance_info(self, dry_run=False):
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
            InstanceIds=[self.instance_id()],
            DryRun=dry_run
        )

    def wait_for_instance_to_be_terminated(self, dry_run=False):
        waiter = self.ec2_client.get_waiter('instance_terminated')
        waiter.wait(
            Filters=[
                {
                    'Name': 'instance-id',
                    'Values': [
                        self.instance_id(),
                    ]
                }
            ],
            DryRun=dry_run
        )

    def launch(self, ebs_optimized=True, tags=None, dry_run=False):
        # print(self.security_group_ids)
        # Tags are list of dicts with shape {'Key': key0, 'Value': val0}
        if self.is_running_or_pending():
            raise RuntimeError(f'Instance with name {self.name} already exists')

        placement_params = {"AvailabilityZone": self.az}


        if self.placement_group_name is not None:
            placement_params["GroupName"] = self.placement_group_name


        if self.eia_type is None:
            eia_param_list = []
        else:
            eia_param_list = [{'Type': self.eia_type}]




        all_tags = [{'Key': 'Name', 'Value': self.name}]
        if tags:
            if type(tags) is not list:
                raise RuntimeError("Tags must be a list if not None")
            if len(tags) == 0:
                raise RuntimeError("Tags cannot be an empty list. Use None instead")
            if type(tags[0]) is not dict:
                raise RuntimeError("Elements in tags must be dicts")
            for tag in tags:
                if 'Key' not in tag.keys() or 'Value' not in tag.keys():
                    raise RuntimeError("Each tag must have a 'Key' field and a 'Value' field")
            all_tags += tags

        # print(self.volume_size_gb)

        response = self.ec2_client.run_instances(
            BlockDeviceMappings=[
                {
                    'DeviceName': "/dev/xvda",
                    'Ebs': {
                        'Iops': self.iops,
                        'SnapshotId': self.ebs_snapshot_id,
                        'VolumeSize': self.volume_size_gb,
                        'VolumeType': self.volume_type
                    },
                },
            ],
            ImageId=self.ami_id,
            InstanceType=self.instance_type,
            KeyName=self.key_name,
            MaxCount=1,
            MinCount=1,
            Monitoring={
                'Enabled': False
            },
            Placement=placement_params,
            SecurityGroupIds=self.security_group_ids,
            SubnetId=self.subnet_id,
            DryRun=dry_run,
            EbsOptimized=ebs_optimized,
            IamInstanceProfile={
                'Name': self.iam_ec2_role_name
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
        instance_id = self.instance_id()
        response = self.ec2_client.terminate_instances(
            InstanceIds=[
                instance_id,
            ],
            DryRun=dry_run
        )

        instance = self.ec2_resource.Instance(instance_id)

        # Unname the instance. Allows you to spin up a new node with the same name while the old one shuts down
        instance.delete_tags(Tags=[{'Key': 'Name'}])




if __name__ == "__main__":

    node1 = EC2Node(unique_name='test-node-1',
                    region="us-west-2",
                    az='us-west-2c',
                    vpc_id='vpc-09fe736b3807bbecf',
                    subnet_id='subnet-016dd4822d0e200b1',
                    ami_id="ami-0b294f219d14e6a82",
                    ebs_snapshot_id="snap-013f2dc8c2ecc97d9",
                    iops=5000,
                    volume_size_gb=500,
                    volume_type='io1',
                    key_name='"ec2-cluster-test',
                    security_group_ids=["sg-08f2dd4548d863796"],
                    iam_ec2_role_name='ec2-cluster-test-role',
                    instance_type='p3.16xl')
    node1.launch()
    node1.wait_for_instance_to_be_running()
    node1.wait_for_instance_to_be_status_ok()

    print("Instance is up and running")
    print(node1.instance_id())
    print(node1.private_ip())
    print(node1.public_ip())

    node1.terminate()

