
import os

from ec2_cluster.control import ClusterShell


if __name__ == '__main__':
    username = "ec2-user"
    master_ip = ""
    worker_public_ips = ""

    ssh_key_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "_gitignored/secrets/ec2-cluster-test.pem")

    sh = ClusterShell(username, master_ip, worker_public_ips, ssh_key_path)

    out = sh.run_on_all("hostname")

    print("-----")
    print(out)
