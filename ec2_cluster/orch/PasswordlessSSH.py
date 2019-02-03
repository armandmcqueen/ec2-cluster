def check_ip_in_known_hosts_cmd(ip):
    return f'ssh-keygen -F {ip}'


def add_to_known_hosts_cmd(ip):
    return f'ssh-keyscan {ip} >> ~/.ssh/known_hosts'


def set_up_passwordless_ssh_from_master_to_workers(remote_shell, master_ip, worker_ips=None):
    if worker_ips is None:
        worker_ips = []

    remote_shell.run_on_master(add_to_known_hosts_cmd('localhost'))
    remote_shell.run_on_master(add_to_known_hosts_cmd(master_ip))
    for worker_ip in worker_ips:
        remote_shell.run_on_master(add_to_known_hosts_cmd(worker_ip))
