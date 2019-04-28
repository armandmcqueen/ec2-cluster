# ec2-cluster

Simple library and CLI to manage and work with clusters of EC2 instances. Multi-purpose, but created to make distributed deep learning infrastructure easier. 

`ec2-cluster` is designed for simple distributed tasks where Kubernetes is overkill or where fast cluster spin up/down is crucial. Example use cases are running distributed deep learning on an expensive cluster or running distributed load testing from many EC2 instances.

### Goals

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





