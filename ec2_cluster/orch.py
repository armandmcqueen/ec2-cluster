def check_ip_in_known_hosts_cmd(ip):
    return f'ssh-keygen -F {ip}'


def add_to_known_hosts_cmd(ip):
    return f'ssh-keyscan {ip} >> ~/.ssh/known_hosts'


def set_up_passwordless_ssh_from_master_to_workers(remote_shell, master_ip, worker_ips=None, verbose=False):
    if verbose:
        def vlog(s):
            print(f'[orch.passwordless_ssh] {s}')
    else:
        def vlog(s):
            pass

    if worker_ips is None:
        worker_ips = []

    vlog("Setting up ssh from master to master's localhost")
    remote_shell.run_on_master(add_to_known_hosts_cmd('localhost'), hide=True)
    vlog("Setting up ssh from master to master's private IP")
    remote_shell.run_on_master(add_to_known_hosts_cmd(master_ip), hide=True)
    worker_count = len(worker_ips)
    for i, worker_ip in enumerate(worker_ips):
        vlog(f'Setting up ssh from master to worker {i+1} of {worker_count} private IP')
        remote_shell.run_on_master(add_to_known_hosts_cmd(worker_ip), hide=True)



# return list of strings, e.g. ['localhost slots=8', '172.0.0.0 slots=8']
def generate_hostfile(cluster, sh, slots, use_localhost=False):
    master_host = 'localhost' if use_localhost else cluster.ips['master_private_ip']
    hosts = [master_host]
    worker_hosts = cluster.ips['worker_private_ips']
    hosts.extend(worker_hosts)

    entries = [f'{host} slots={slots}' for host in hosts]

    hostfile_str = "\n".join(entries)

    sh.run_on_master(f'echo "{hostfile_str}" > ~/hostfile')
