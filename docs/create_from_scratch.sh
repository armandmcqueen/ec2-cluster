#!/usr/bin/env bash

rm -rf _build
rm -rf apidocs

#
#sphinx-apidoc -o apidocs ../ec2_cluster \
#    --separate

sphinx-apidoc -o apidocs ../ec2_cluster \
    ../ec2_cluster/infra/EC2Node.py \
    ../ec2_cluster/infra/EC2NodeCluster.py \
    ../ec2_cluster/control/ClusterShell.py \
    ../ec2_cluster/control/RemoteShell.py \
    ../ec2_cluster/orch/PasswordlessSSH.py \
    --separate

#.. autosummary::
#   :nosignatures:
#
#   ec2_cluster.infra.EC2Node
#   ec2_cluster.infra.EC2NodeCluster



# .. autosummary::
#   :toctree:
#   :nosignatures:
#
#   ec2_cluster.infra.EC2Node
#   ec2_cluster.infra.EC2NodeCluster

./test_sphinx_edit.sh

make html