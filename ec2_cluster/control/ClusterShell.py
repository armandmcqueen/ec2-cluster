import os
import random
import shlex
import subprocess
import time
from fabric2 import Connection, ThreadingGroup

def humanize_float(num):
    return "{0:,.2f}".format(num)


MAX_CONNS_PER_GROUP = 10

class ClusterShell:


    def __init__(self, username, master_ip, worker_ips, ssh_key_path, use_bastion=True):
        self._username = username
        self._master_ip = master_ip
        self._worker_ips = worker_ips
        self._all_ips = [self._master_ip] + self._worker_ips
        self.use_bastion = use_bastion

        connect_kwargs = {"key_filename": os.path.expanduser(ssh_key_path)}

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
        return subprocess.check_output(shlex.split(cmd))

    def run_on_master(self, cmd):
        return self._master_conn.run(cmd)

    def run_on_all(self, cmd):
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
        return self._master_conn.get(remote_abs_path, local_abs_path)


    # TODO: Clean this code up
    # local_abs_path must be a directory
    def copy_from_all_to_local(self, remote_abs_path, local_abs_path):
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
        return self._master_conn.put(local_abs_path, remote_abs_path)

    def copy_from_local_to_all(self, local_abs_path, remote_abs_path):
        self.copy_from_local_to_master(local_abs_path, remote_abs_path)
        for worker_conn in self._individual_worker_conns:
            worker_conn.put(local_abs_path, remote_abs_path)

    def username(self):
        return self._username

    def master_ip(self):
        return self._master_ip

    def non_master_ips(self):
        return self._worker_ips

    def all_ips(self):
        return self._all_ips

    def clean_shutdown(self):
        pass






