import os
import random
import shlex
import subprocess
import time
from fabric2 import Connection, ThreadingGroup

# NOTE 1: Not certain I actually need this, but was a proposed fix for 'error reading SSH banner' and I haven't seen
#         that error since. https://github.com/paramiko/paramiko/issues/673#issuecomment-436815430

def humanize_float(num):
    return "{0:,.2f}".format(num)


MAX_CONNS_PER_GROUP = 10

class ClusterShell:
    """
    ClusterShell lets you run commands on multiple EC2 instances.

    ClusterShell takes in information about a set of EC2 instances that exist and allows you to run commands on
    some or all of the nodes. It also has convenience methods for copying files between the local filesystem and
    the cluster.
    """


    def __init__(self, username, master_ip, worker_ips, ssh_key_path, use_bastion=False):
        """
        Args:
            username: The username used to ssh to the instance. Often 'ubuntu' or 'ec2-user'
            master_ip: A single IP for the master node. Typically should be the public IP if the location this code is
                       running is outside of the VPC and the private IP if running from another EC2 node in the same
                       VPC. In many cases, the distinction between master and workers is arbitrary. If use_bastion is
                       True, the master node will be the bastion host.
            worker_ips: A possibly empty list of ips for the worker nodes. If there is a single worker, a string IP can
                        be passed in.
            ssh_key_path: The path to the SSH key required to SSH into the EC2 instances. Often ~/.ssh/something.pem
            use_bastion (bool): Whether or not to use the master node as the bastion host for SSHing to worker nodes.
        """
        if not isinstance(worker_ips, list):
            worker_ips = [worker_ips]

        self._username = username
        self._master_ip = master_ip
        self._worker_ips = worker_ips
        self._all_ips = [self._master_ip] + self._worker_ips
        self.use_bastion = use_bastion

        connect_kwargs = {
            "key_filename": os.path.expanduser(ssh_key_path),
            "banner_timeout": 30    # NOTE 1 above
        }

        self._master_conn = Connection(user=self._username,
                                       host=self._master_ip,
                                       forward_agent=True,
                                       connect_kwargs=connect_kwargs)

        worker_conns = []
        for worker_ip in self._worker_ips:
            if self.use_bastion:
                c = Connection(user=self._username,
                               host=worker_ip,
                               connect_kwargs=connect_kwargs,
                               gateway=Connection(user=self._username,
                                                  host=master_ip,
                                                  forward_agent=True,
                                                  connect_kwargs=connect_kwargs))
            else:
                c = Connection(user=self._username, host=worker_ip, connect_kwargs=connect_kwargs)

            worker_conns.append(c)

        self._individual_worker_conns = worker_conns
        self._worker_conns = ThreadingGroup.from_connections(worker_conns)
        self._all_conns = ThreadingGroup.from_connections([self._master_conn] + worker_conns)

    def run_local(self, cmd):
        """Run a shell command on the local machine.

        Will wait for the command to finish and raise an exception if the return code is non-zero.

        Args:
            cmd: The shell command to run

        Returns:
             The stdout of the command as a byte string.
        """
        return subprocess.check_output(shlex.split(cmd))


    def run_on_master(self, cmd):
        """Run a shell command on the master node.

        Args:
            cmd: The shell command to run

        Returns:
            Result: An invoke Result object. `http://docs.pyinvoke.org/en/latest/api/runners.html#invoke.runners.Result`
        """
        return self._master_conn.run(cmd)


    def run_on_all(self, cmd):
        """Run a shell command on every node.

        Args:
            cmd: The shell command to run

        Returns:
             Nothing at the moment. This needs to be fixed.
        """
        print("[run_on_all] TODO: return useful results")
        t0 = time.time()

        if self.use_bastion:
            if len(self._worker_ips) >= (MAX_CONNS_PER_GROUP - 1):
                print("run_on_all_workaround")
                self._run_on_all_workaround(cmd, MAX_CONNS_PER_GROUP)
                print(f'Workaround complete. {humanize_float(time.time() - t0)} secs')
                return

        self._all_conns.run(cmd)
        print(f'run_on_all (default) complete. {humanize_float(time.time() - t0)} secs')
        return


    # TODO: Confirm this is required with (10+ nodes)
    def _run_on_all_workaround(self, cmd, group_size):
        total_conns = len(self._worker_conns) + 1
        print(f'{total_conns} Nodes')
        groups = []

        group_conns = []
        for i, worker_conn in enumerate(self._individual_worker_conns):
            if i % group_size == 0 and i != 0:
                groups.append(ThreadingGroup.from_connections(group_conns))
                group_conns = []
            group_conns.append(worker_conn)


        if len(group_conns) != 0 and len(group_conns) != group_size:
            group_conns.append(self._master_conn)
            groups.append(ThreadingGroup.from_connections(group_conns))
            print("Added master to ThreadingGroup")
            print(f'{len(groups)} groups created @ {MAX_CONNS_PER_GROUP} max per group')

        else:
            if len(group_conns) != 0:
                groups.append(ThreadingGroup.from_connections(group_conns))
            print(f'Running master seperately.')
            print(f'{len(groups)+1} "groups" (serial executions) created @ {MAX_CONNS_PER_GROUP} max per group')
            t0 = time.time()
            print("Running master")
            self.run_on_master(cmd)
            dt = time.time() - t0
            print(f'Ran master. Took {humanize_float(dt)} secs')

        for i, worker_conn_group in enumerate(groups):
            t0 = time.time()
            print(f'Starting group {i + 1} of {len(groups)}')
            worker_conn_group.run(cmd)
            dt = time.time() - t0
            print(f'Finished group {i+1} of {len(groups)}. Took {humanize_float(dt)} secs.')


    def copy_from_master_to_local(self, remote_abs_path, local_abs_path):
        """Copy a file from the master node to the local node.

        Args:
            remote_abs_path: The absolute path of the file on the master node
            local_abs_path: The absolute path to save the file to on the local file system.
        """
        return self._master_conn.get(remote_abs_path, local_abs_path)


    # TODO: Clean this code up
    # local_abs_path must be a directory
    def copy_from_all_to_local(self, remote_abs_path, local_abs_path):
        """Copy files from all nodes to the local filesystem.

        There will be one directory per node containing the file.

        Args:
            remote_abs_path: The absolute path of the file to download. Can be a directory or a cp/scp string including
                             wildcards
            local_abs_path: The absolute path of a directory on the local filesystem to download the files into. The
                            directory must already exist.
        """
        if not os.path.isdir(local_abs_path):
            raise RuntimeError(f'[ClusterShell.copy_from_all_to_local] local_abs_path must be a dir: {local_abs_path}')

        tmp_path = f'/tmp/{random.randint(0, 1_000_000)}'
        self.run_on_master(f'rm -rf {tmp_path}')
        self.run_on_master(f'mkdir -p {tmp_path}') # create a staging folder in /tmp on master node

        # Create and populate staging folder for master data
        master_node_tmp_path = f'{tmp_path}/0' # Rank 0 is master
        self.run_on_master(f'mkdir -p {master_node_tmp_path}')
        self.run_on_master(f'echo {self.master_ip()} > {master_node_tmp_path}/ip.txt') # Save ip of master node
        self.run_on_master(f'cp -r {remote_abs_path} {master_node_tmp_path}/') # Copy master's data to staging folder

        # Create and populate staging folder for each worker's data
        for ind, worker_ip in enumerate(self._worker_ips):
            worker_id = ind + 1
            worker_node_tmp_path = f'{tmp_path}/{worker_id}'
            self.run_on_master(f'mkdir -p {worker_node_tmp_path}')
            self.run_on_master(f'echo {worker_ip} > {worker_node_tmp_path}/ip.txt')  # Save ip of worker
            self.run_on_master(f'scp -r {self._username}@{worker_ip}:{remote_abs_path} {worker_node_tmp_path}/')

        self.copy_from_master_to_local(f'{tmp_path}/*', local_abs_path)
        self.run_on_master(f'rm -r {tmp_path}') # Clean up staging folder


    def copy_from_local_to_master(self, local_abs_path, remote_abs_path):
        """Copy a file from the local filesystem to the master node.

        Args:
            local_abs_path: The absolute path of the file to send to the master node
            remote_abs_path: The absolute path where the file will be saved on the master node
        """
        return self._master_conn.put(local_abs_path, remote_abs_path)

    def copy_from_local_to_all(self, local_abs_path, remote_abs_path):
        """Copy a file from the local filesystem to every node in the cluster.

        Args:
            local_abs_path: The absolute path of the file to send to the master and worker nodes
            remote_abs_path: The absolute path where the file will be saved on the master and worker nodes
        """
        self.copy_from_local_to_master(local_abs_path, remote_abs_path)
        for worker_conn in self._individual_worker_conns:
            worker_conn.put(local_abs_path, remote_abs_path)

    @property
    def username(self):
        """The username used to instantiate the ClusterShell"""
        return self._username

    @property
    def master_ip(self):
        """The master IP used to instantiate the ClusterShell"""
        return self._master_ip

    @property
    def non_master_ips(self):
        """All IPs other than the master node. May be an empty list"""
        return self._worker_ips

    @property
    def all_ips(self):
        """A list of master and worker IPs"""
        return self._all_ips








