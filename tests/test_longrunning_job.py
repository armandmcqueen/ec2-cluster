# import argparse
#
# import ec2_cluster as ec3
#
#
# def launch_job():
#     cluster = ec3.infra.ConfigCluster("cluster.yaml")
#     cluster.launch(verbose=True)
#     sh = cluster.get_shell()
#     sh.copy_from_local_to_all("job_script.py", "job_script.py")
#     sh.run_on_all("python job_script.py > job.log 2>&1 &")
#
#
# def check_on_job():
#     cluster = ec3.infra.ConfigCluster("cluster.yaml")
#     sh = cluster.get_shell()
#     statuses = sh.run_on_all("tail -n 1 job.log", hide=True)
#     for status in statuses:
#         if status.stdout.rstrip("\n") != "job_script.py complete":
#             print("Job is not yet complete on all instances")
#             return
#
#     print("Job is finally done on all instances!")
#
#     sh.copy_from_all_to_local("job.log", "./results/")
#     cluster.terminate(verbose=True)
#
#
#
# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument("action", choices=["launch", "check", "terminate"])
#
#     args, leftovers = parser.parse_known_args()
#
#     if args.action == "launch":
#          launch_job()
#     elif args.action == "check":
#         check_on_job()
#     elif args.action == "terminate":
#         ec3.infra.ConfigCluster("cluster.yaml").terminate(verbose=True, fast_terminate=True)
