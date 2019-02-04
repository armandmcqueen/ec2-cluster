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
                 always_verbose=False):

        self._always_verbose = always_verbose

        self.node_count = node_count
        self.region = region
        self.cluster_name = cluster_name
        self.node_names = [f'{self.cluster_name}-node{i}' for i in range(node_count)]
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
        if not self.any_node_is_running_or_pending():
            raise RuntimeError("No nodes are running for this cluster!")
        return [node.instance_id for node in self.nodes]


    @property
    def private_ips(self):
        if not self.any_node_is_running_or_pending():
            raise RuntimeError("No nodes are running for this cluster!")
        return [node.private_ip for node in self.nodes]

    @property
    def public_ips(self):
        if not self.any_node_is_running_or_pending():
            raise RuntimeError("No nodes are running for this cluster!")
        return [node.public_ip for node in self.nodes]

    @property
    def cluster_sg_id(self):
        if self._cluster_sg_id is None:
            if not self.security_group_exists(self.cluster_sg_name):
                raise RuntimeError(f'Cluster security group "{self.cluster_sg_name}" does not exist!')
            self._cluster_sg_id = self.get_security_group_id_from_name(self.cluster_sg_name)
        return self._cluster_sg_id


    def create_cluster_sg(self, vpc_id):
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


    def delete_cluster_sg(self, dry_run=False):
        response = self.ec2_client.delete_security_group(
            GroupId=self.cluster_sg_id,
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

    def placement_group_exists(self):
        return self.cluster_placement_group_name in [pg["GroupName"] for pg in self.list_placement_groups()]

    def create_placement_group_if_doesnt_exist(self):
        if not self.placement_group_exists():
            response = self.ec2_client.create_placement_group(
                GroupName=self.cluster_placement_group_name,
                Strategy='cluster'
            )

    def delete_placement_group(self):
        if self.placement_group_exists():
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
            except Exception as ex:
                print("[wait_for_all_nodes_to_be_terminated] Some error while waiting for nodes to be terminated")
                print(f'[wait_for_all_nodes_to_be_terminated] {ex}')
                print("[wait_for_all_nodes_to_be_terminated] Assuming non-fatal error. Continuing")
                pass




    # max_timeout_secs is on a per-node basis: successfully launching a node resets the timeout timer.
    # max_timeout_secs=None to retry forever
    # wait_secs is how long we wait between attempts to launch ec2 node.
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
               use_placement_group=False,
               iops=None,
               eia_type=None,
               ebs_optimized=True,
               tags=None,
               dry_run=False,
               max_timeout_secs=None,
               wait_secs=10,
               verbose=True):

        vlog = self._get_vlog(verbose, 'EC2NodeCluster.launch')

        if self.any_node_is_running_or_pending():
            raise RuntimeError("Nodes with names matching this cluster already exist!")

        vlog("Creating cluster SG if needed")
        self.create_cluster_sg(vpc_id)

        if use_placement_group:
            vlog("Creating placement group")
            self.create_placement_group_if_doesnt_exist()

        for launch_ind, ec2_node in enumerate(self.nodes):
            start = time.time()
            while True:
                vlog("-----")
                try:

                    ec2_node.launch(az,
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

                    if max_timeout_secs is not None and (time.time() - start) > max_timeout_secs:
                        vlog(f'Timed out trying to launch node #{launch_ind+1}. Max timeout of {max_timeout_secs} seconds reached')
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
                        if max_timeout_secs is None:
                            vlog(f'There is no timeout. Elapsed time trying to launch this node: {humanize_float(time.time() - start)} seconds')
                        else:
                            vlog(f'Will time out after {max_timeout_secs} seconds. Current elapsed time: {humanize_float(time.time() - start)} seconds')
                        time.sleep(wait_secs)

        vlog("-----")
        vlog("Now waiting for all nodes to reach RUNNING state")
        self.wait_for_all_nodes_to_be_running()
        vlog("All nodes are running!")


    def terminate(self, verbose=False):
        vlog = self._get_vlog(verbose, 'EC2NodeCluster.terminate')

        for i, ec2_node in enumerate(self.nodes):
            vlog("-----")
            ec2_node.detach_security_group(self.cluster_sg_id)
            ec2_node.terminate()
            vlog(f'Node {i + 1} of {self.node_count} successfully triggered deletion')
        self.delete_cluster_sg()
        vlog("Cluster SG deleted")
        vlog("-----")
        vlog("Waiting for all nodes to reach terminated state")
        self.wait_for_all_nodes_to_be_terminated()
        if self.placement_group_exists():
            self.delete_placement_group()
            vlog("Placement group deleted!")
