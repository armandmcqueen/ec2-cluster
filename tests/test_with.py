# import ec2_cluster
# from ec2_cluster.infra import ConfigCluster
# from ec2_cluster.control import ClusterShell
#
#
#
#
#
#
#
# if __name__ == '__main__':
#     print("ec2-cluster version:", ec2_cluster.__version__)
#
#     with ConfigCluster(config_yaml_path="cluster.yaml") as cluster:
#         sh = cluster.get_shell()
#         hostnames = sh.run_on_all("hostname")
#
#         for hostname in hostnames:
#             print("HOSTNAME")
#             print(hostname.stdout.rstrip("\n"))
#
#
#
#
