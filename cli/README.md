# CLI

Describe the cluster configuration in a yaml file and then use the CLI to create, delete and retrieve details about the cluster.

`ec3 create config.yaml`

`ec3 describe config.yaml`

`ec3 delete config.yaml`


## Quick Start

#### Create config.yaml

Create a config file with information about VPC, AMI and cluster size. See [this example config](configs/test.yaml).

`ec3 utils describe-config` to see details about the params in config.yaml.



`ec3 utils list-amis` and `ec3 utils list-dlami` will help you find the AMI ids and the EBS snapshot ids for your AMI or for AWS's Deep Learning AMI, respectively.

#### Create the cluster

Run `ec3 create config.yaml` to create the cluster. It should happen in about 1 minute. Add the `--verbose` flag if it doesn't seem to be working.

`ec3 describe config.yaml` will list the public and private IPs of the nodes in the cluster.

`ec3 delete config.yaml` will tear the cluster down.

## Horovod

Running Horovod requires a little setup to allow mpirun to SSH between nodes. This can be done when you create the cluster with  `ec3 create config.yaml --horovod`

This is equivalent to  `ec3 create config.yaml` followed by `ec3 setup-horovod config.yaml`

## Naming

ec2-cluster keeps track of nodes by using the 'Name' tag in EC2. For a given node, the Name must be unique in a region.

A common use case is to create multiple identical clusters to run training jobs in parallel. To this end, nodes are named using this pattern:

`${CLUSTER_TEMPLATE_NAME}-${NODE_COUNT}node-cluster${CLUSTER_ID}-node${NODE_ID}`

 For example, [test.yaml](configs/test.yaml) creates two nodes with these names:
- `ec2-cluster-test-2node-cluster1-node1`
- `ec2-cluster-test-2node-cluster1-node2`
 
 
#### CLI config overwrite

To enable this, you can overwrite any param in config.yaml at create time:

- `ec3 create config.yaml --cluster_id=2`
- `ec3 create config.yaml --node_count=2`
- `ec3 create config.yaml --volume_gbs=500`

NOTE: If you overwrite `cluster_template_name`, `node_count` or `cluster_id`, you will create a cluster with a new name so you will need to pass in that flag any other time you want to use `ec3` to control that cluster. Overwriting any other flag does not change the name.

The following list of commands and results illustrates this.

- `ec3 create config.yaml` will create `ec2-cluster-test-2node-cluster1`
- `ec3 create config.yaml --cluster_id=2` will create `ec2-cluster-test-2node-cluster2`
- `ec3 create config.yaml --cluster_id=3 --volume_gbs=500` will create `ec2-cluster-test-2node-cluster3`
- `ec3 create config.yaml --volume_gbs=500` will try to create `ec2-cluster-test-2node-cluster1` and fail because it already exists
- `ec3 create config.yaml --cluster_id=2 --node_count=4` will create `ec2-cluster-test-4node-cluster2`
- `ec3 delete config.yaml` will delete `ec2-cluster-test-2node-cluster1`
- `ec3 delete config.yaml --node_count=4` will fail because `ec2-cluster-test-4node-cluster1` does not exist
- `ec3 delete config.yaml --cluster_id=3` will successfully delete `ec2-cluster-test-2node-cluster3` despite skipping the `--volume_gbs` flag.



  
