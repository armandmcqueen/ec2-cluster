# ec2-cluster

Simple library and CLI to manage and work with clusters of EC2 instances. Multi-purpose, but created to make distributed deep learning infrastructure easier.


### Goals

- Provide the minimal set of features to run distributed deep learning training jobs on EC2 instances.
- Provide libraries, not a framework or platform.
- Make cluster environments reproducible to allow for parallelization of experiments
- Make cluster launches fast 
- Focus on iterative, not disruptive, improvements on the common methodology of manually launching EC2 instances, ssh-ing to them, configuring environments by hand and running training scripts*

### Quick Start

### Libraries

EC2Node and EC2NodeCluster are classes for working with EC2 instances.

RemoteShell and ClusterShell



