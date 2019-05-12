# EC2 Cluster CLI


# Quick start

`ecc create config.yaml --horovod`

`ecc delete config.yaml`

`ecc utils list-dlamis --region=us-east-1`

`ecc utils list-amis --region=us-east-1`

`ecc utils describe-params`




### create

Will create a cluster if does not exist. If clean_create, will delete the cluster and create it anew. If not clean_create and a cluster already exists, will raise RuntimeError and do nothing.

### terminate

Will terminate the cluster and wait for the shutdown to complete if it exists. If the cluster doesn't exist, will raise RuntimeError and do nothing.

### describe

Will output the public and private ips of the cluster as a json

### ssh_cmd

Will output a string that can be run to ssh to the master node

### test

Enables --verbose and handles the argument and YAML parsing. Then does nothing.