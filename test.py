import ec2_cluster
from ec2_cluster.infra import ConfigCluster
from ec2_cluster.control import ClusterShell

from pathlib import Path

test_cfg_file = Path(__file__).parent/"test_config.yaml"



def get_test_cluster():
    print("ec2-cluster version:", ec2_cluster.__version__)
    cluster = ConfigCluster(config_yaml_path=test_cfg_file)

    if not cluster.any_node_is_running_or_pending():
        cluster.launch(verbose=True)

    print("Cluster id:", cluster.cluster_name)
    return cluster





if __name__ == '__main__':
    cluster = get_test_cluster()
    # cluster.terminate(verbose=True)
    # quit()
    sh = ClusterShell.from_ec2_cluster(cluster, ssh_key_path="~/.ssh/ec2-cluster-test.pem")
    hostnames = sh.run_on_all("hostname", hide=True)


    # cluster.terminate(verbose=True)


