
import os

from ec2_cluster.control import ClusterShell


if __name__ == '__main__':
    # ./ec2-cluster-ctl create     --config configs/test.yaml     --cluster_template_name large-cluster-test       --node_count 20
    # ./ec2-cluster-ctl describe     --config configs/test.yaml     --cluster_template_name large-cluster-test       --node_count 20
    # ./ec2-cluster-ctl terminate     --config configs/test.yaml     --cluster_template_name large-cluster-test       --node_count 20

    ips = {
        "master_public_ip": "34.214.54.210",
        "worker_public_ips": ["34.221.78.155", "54.201.178.158", "52.36.186.64", "35.164.49.14", "54.190.25.16",
                              "35.160.171.50", "35.162.65.68", "52.34.246.158", "35.165.156.191", "35.167.241.10",
                              "54.214.164.182", "34.223.0.90", "34.221.32.79", "54.70.21.237", "34.209.64.99",
                              "18.236.231.188", "54.214.224.102", "54.187.152.149", "18.237.235.21"],
        "master_private_ip": "172.31.6.190",
        "worker_private_ips": ["172.31.3.3", "172.31.2.85", "172.31.6.173", "172.31.3.105", "172.31.10.222",
                               "172.31.8.80", "172.31.5.223", "172.31.12.80", "172.31.0.58", "172.31.15.2",
                               "172.31.11.94", "172.31.1.153", "172.31.0.32", "172.31.11.4", "172.31.9.14",
                               "172.31.7.17", "172.31.4.68", "172.31.8.177", "172.31.6.21"]
    }
    username = "ubuntu"
    master_ip = ips["master_public_ip"]
    worker_ips = ips["worker_public_ips"]

    ssh_key_path = "/Users/armandmcqueen/.ssh/ec2-cluster-test.pem"

    print(ssh_key_path)
    print(os.path.isfile(ssh_key_path))

    sh = ClusterShell(username, master_ip, worker_ips, ssh_key_path)

    out = sh.run_on_all("hostname")

    print("-----")
    print(out)
