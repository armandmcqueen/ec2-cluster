# ec2-cluster

Simple library and CLI to work with clusters of EC2 instances. Multi-purpose, but created to make deep learning distributed training infrastructure easier. 

`ec2-cluster` is designed for simple distributed tasks where Kubernetes is overkill or where fast cluster spin up/down is crucial. It provides the ability to launch a cluster, to retrieve IP addresses for all nodes in the cluster, to delete the cluster and to execute commands on some or all of the instances. 

Other benefits:
- Resilient to EC2 capacity limits. If instances are not available, `ec2-cluster` will retry until the all nodes in the cluster are created or until the user-set timeout is reached.
- Easy to quickly launch duplicate clusters for parallel training runs.
- Can write orchestration logic that needs to be run when launching a cluster, e.g. enabling passwordless ssh between all instances for Horovod-based training

## Examples

### Library

### CLI

## Goals

- Provide the minimal set of features to run distributed deep learning training jobs on EC2 instances.
- Provide libraries, not a framework or platform.
- Make cluster environments reproducible to allow for parallelization of experiments
- Make cluster launches fast
- Be resilient to EC2 capacity limitations
- Encourage ephemeral infrastructure design
- Focus on iterative, not disruptive, improvements on the common methodology of manually launching EC2 instances, ssh-ing to them, configuring environments by hand and running scripts

### Overview

`ec2-cluster` can be consumed in two ways:

- A CLI for launching, describing and deleting clusters. 
- A python library for scripting.  

This library has three main components:L
- **infra**: creating cluster infrastructure
- **orch**: orchestrating simple runtime cluster configuration (e.g. generate a hostfile with runtime IPs)
- **control**: running commands on the cluster

### CLI Quick Start

### Library Quick Start





