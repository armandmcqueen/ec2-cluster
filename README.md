# ec2-cluster

Quickly spin up EC2 instances, run commands/scripts on them, and then (optionally) automatically shut them down. Minus all the boilerplate of `boto3` and manual SSH/`pdsh` commands.

```python
import ec2_cluster as ec3

with ec3.Cluster('ec2cluster.yaml', num_instances=5) as cluster:
    cluster.upload("perf_test.py")  # copy local file to every instance
    sh = cluster.get_shell()
    results = sh.run("python perf_test.py")
    script_ouputs = [result.stdout for result in results]

# Cluster is torn down when context is exited
```

`ec3` can also be used without a context manager and then the instances won't be shut down until shutdown is manually triggered. This can be useful for long-running jobs.


## Long-running tasks

`ec2-cluster` is also designed for long-running tasks where you may not want to keep your local machine awake for the full duration. The library relies on EC2 tags to keep track of EC2 instances, letting you interact with a cluster across sessions without needing an always-on control plane.

Below is one way to launch complicated, long-running jobs and download the results at some later time. `check_on_job()` can be run in a different session or even a different machine than `launch_job()` as long as they have the same `cluster.yaml` and are using the same AWS account.

```python
import ec2_cluster as ec3

def launch_job():
    cluster = ec3.Cluster("ec2cluster.yaml")
    cluster.launch(verbose=True)
    sh = cluster.get_shell()
    sh.copy_from_local_to_all("job_script.py", "job_script.py")
    sh.run_on_all("python job_script.py > job.log 2>&1 &")  # Launch script as background process


def check_on_job():
    cluster = ec3.Cluster("cluster.yaml")
    sh = cluster.get_shell()
    statuses = sh.run_on_all("tail -n 1 job.log", hide=True)
    for status in statuses:
        if status.stdout.rstrip("\n") != "job_script.py complete":
            print("Job is not yet complete on all instances")
            return
    
    print("Job is finally done on all instances!")
     
    sh.copy_from_all_to_local("job.log", "./results/")
    cluster.terminate(verbose=True)
```
This will create a directory on your local machine:
```
results/
├── 0
│   ├── ip.txt
│   └── job.log
├── 1
│   ├── ip.txt
│   └── job.log
└── 3
    ├── ip.txt
    └── job.log
```




