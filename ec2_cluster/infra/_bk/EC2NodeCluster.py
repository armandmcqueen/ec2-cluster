import time

import boto3

from ec2_cluster.infra import EC2Node


def humanize_float(num):
    return "{0:,.2f}".format(num)

class EC2NodeCluster:
    def __init__(self,
                 node_count,
                 cluster_name,
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
                 as_placement_group=False,
                 eia_type=None):


        self.session = boto3.session.Session(region_name=region)
        self.ec2_client = self.session.client("ec2")
        self.ec2_resource = self.session.resource("ec2")

        self.node_count = node_count
        self.cluster_name = cluster_name

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

        self.use_placement_group = as_placement_group
        self.cluster_placement_group_name = f'{self.cluster_name}-placement-group'
        pg_name_param = self.cluster_placement_group_name if self.use_placement_group else None

        self.eia_type = eia_type

        self.node_names = [f'{self.cluster_name}-node{i}' for i in range(node_count)]
        self.nodes = [EC2Node(node_name,
                              self.region,
                              self.az,
                              self.vpc_id,
                              self.subnet_id,
                              self.ami_id,
                              self.ebs_snapshot_id,
                              self.iops,
                              self.volume_size_gb,
                              self.volume_type,
                              self.key_name,
                              [sg_id for sg_id in self.security_group_ids],
                              self.iam_ec2_role_name,
                              self.instance_type,
                              placement_group_name=pg_name_param,
                              eia_type=self.eia_type)
                      for node_name in self.node_names]

        self.cluster_sg_name = f'{self.cluster_name}-intracluster-ssh'


        self._cluster_sg_id = None
        self._instance_ids = None
        self._private_ips = None
        self._public_ips = None



    def instance_ids(self):
        if not self.any_node_is_running_or_pending():
            raise RuntimeError("No nodes are running for this cluster!")
        if self._instance_ids is None:
            self.load_instance_infos()
        return self._instance_ids


    def private_ips(self):
        if not self.any_node_is_running_or_pending():
            raise RuntimeError("No nodes are running for this cluster!")
        if self._private_ips is None:
            self.load_instance_infos()
        return self._private_ips


    def public_ips(self):
        if not self.any_node_is_running_or_pending():
            raise RuntimeError("No nodes are running for this cluster!")
        if self._public_ips is None:
            self.load_instance_infos()
        return self._public_ips


    def load_instance_infos(self):
        self._instance_ids = [node.instance_id() for node in self.nodes]
        self._private_ips = [node.private_ip() for node in self.nodes]
        self._public_ips = [node.public_ip() for node in self.nodes]


    def cluster_sg_id(self):
        if self._cluster_sg_id is None:
            if not self.security_group_exists(self.cluster_sg_name):
                raise RuntimeError(f'Cluster security group "{self.cluster_sg_name}" does not exist!')
            self._cluster_sg_id = self.get_security_group_id_from_name(self.cluster_sg_name)
        return self._cluster_sg_id

    def create_cluster_sg(self, dry_run=False):
        if self.security_group_exists(self.cluster_sg_name):
            print("Cluster SG already exists. No need to recreate")
            return

        response = self.ec2_client.create_security_group(
            Description=self.cluster_sg_name,
            GroupName=self.cluster_sg_name,
            VpcId=self.vpc_id,
            DryRun=dry_run
        )
        self._cluster_sg_id = response['GroupId']

        while not self.security_group_exists(self.cluster_sg_name):
            time.sleep(1)

        sg = self.ec2_resource.SecurityGroup(self.cluster_sg_id())
        sg.authorize_ingress(SourceSecurityGroupName=self.cluster_sg_name)

    def delete_cluster_sg(self, dry_run=False):
        response = self.ec2_client.delete_security_group(
            GroupId=self.cluster_sg_id(),
            GroupName=self.cluster_sg_name,
            DryRun=dry_run
        )

    def security_group_exists(self, sg_name):
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
        response = self.ec2_client.describe_placement_groups()
        # returns list of:
        # {'GroupName': 'string', 'State': 'pending'|'available'|'deleting'|'deleted', 'Strategy': 'cluster'|'spread'}
        return response["PlacementGroups"]

    def check_placement_group_exists(self):
        return self.cluster_placement_group_name in [pg["GroupName"] for pg in self.list_placement_groups()]

    def create_placement_group_if_doesnt_exist(self):
        if not self.check_placement_group_exists():
            response = self.ec2_client.create_placement_group(
                GroupName=self.cluster_placement_group_name,
                Strategy='cluster'
            )

    def delete_placement_group(self):
        if self.check_placement_group_exists():
            response = self.ec2_client.delete_placement_group(
                GroupName=self.cluster_placement_group_name
            )



    def any_node_is_running_or_pending(self):
        for ec2_node in self.nodes:
            if ec2_node.is_running_or_pending():
                return True
        return False

    def wait_for_all_nodes_to_be_running(self):
        for ec2_node in self.nodes:
            ec2_node.wait_for_instance_to_be_running()

    def wait_for_all_nodes_to_be_status_ok(self):
        for ec2_node in self.nodes:
            ec2_node.wait_for_instance_to_be_status_ok()

    def wait_for_all_nodes_to_be_terminated(self):
        for ec2_node in self.nodes:
            try:
                ec2_node.wait_for_instance_to_be_terminated()
            except:
                pass




    # max_timeout_secs is on a per-node basis: successfully launching a node resets the timeout timer.
    # max_timeout_secs=None to retry forever
    # wait_secs is how long we wait between attempts to launch ec2 node.
    def launch(self, ebs_optimized=True, tags=None, dry_run=False, max_timeout_secs=None, wait_secs=10, verbose=True):

        def vlog(s):
            if verbose:
                print(s)

        if self.any_node_is_running_or_pending():
            raise RuntimeError("Nodes with names matching this cluster already exist!")

        self.create_cluster_sg()

        if self.use_placement_group:
            self.create_placement_group_if_doesnt_exist()

        for launch_ind, ec2_node in enumerate(self.nodes):
            ec2_node.add_sg(self.cluster_sg_id())
            start = time.time()
            while True:
                print("-----")
                try:
                    ec2_node.launch(ebs_optimized=ebs_optimized, tags=tags, dry_run=dry_run)
                    vlog(f'Node {launch_ind+1} of {self.node_count} successfully launched')
                    break
                except Exception as e:
                    vlog(f'Error launching node: {str(e)}')

                    if max_timeout_secs is not None and (time.time() - start) > max_timeout_secs:
                        vlog(f'Timed out trying to launch node #{launch_ind+1}. Max timeout of {max_timeout_secs} seconds reached')
                        vlog("Now trying to clean up partially launched cluster")
                        for terminate_ind, ec2_node_to_delete in enumerate(self.nodes):
                            vlog("-----")
                            try:
                                if terminate_ind >= launch_ind:
                                    break   # Don't try to shut down nodes that weren't launched.
                                vlog(f'Terminating node #{terminate_ind+1} of {self.node_count}')
                                ec2_node_to_delete.detach_security_group(self.cluster_sg_id())
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
                        if max_timeout_secs is None:
                            vlog(f'There is no timeout. Elapsed time trying to launch this node: {humanize_float(time.time() - start)} seconds')
                        else:
                            vlog(f'Will time out after {max_timeout_secs} seconds. Current elapsed time: {humanize_float(time.time() - start)} seconds')
                        time.sleep(wait_secs)

        print("-----")
        vlog("Now waiting for all nodes to reach RUNNING state")
        self.wait_for_all_nodes_to_be_running()

    def terminate(self):
        for i, ec2_node in enumerate(self.nodes):
            print("-----")
            ec2_node.detach_security_group(self.cluster_sg_id())
            ec2_node.terminate()
            print(f'Node {i + 1} of {self.node_count} successfully triggered deletion')
        self.delete_cluster_sg()

        print("-----")
        print("Waiting for all nodes to reach terminated state")
        self.wait_for_all_nodes_to_be_terminated()
        if self.use_placement_group:
            print("Deleting placement group")
            self.delete_placement_group()
