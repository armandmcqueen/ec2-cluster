import ec2_cluster
from ec2_cluster import Cluster, ClusterShell


if __name__ == '__main__':
    print("ec2-cluster version:", ec2_cluster.__version__)

    Cluster(config_file_path="ec2cluster.yaml").terminate(fast_terminate=True)
    with Cluster(config_file_path="ec2cluster.yaml") as cluster:
        sh = cluster.get_shell()
        hostnames = sh.run_on_all("hostname")

        for hostname in hostnames:
            print("HOSTNAME")
            print(hostname.stdout.rstrip("\n"))




