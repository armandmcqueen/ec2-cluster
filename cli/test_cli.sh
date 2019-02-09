#!/usr/bin/env bash

ec2-cluster-ctl terminate \
    --config configs/test.yaml \
    --cluster_template_name ec2-cluster-test-overwrite



ec2-cluster-ctl terminate \
    --config configs/test.yaml \
    --cluster_template_name ec2-cluster-test-overwrite \
    --verbose


ec2-cluster-ctl ssh_cmd \
    --config configs/test.yaml \
    --cluster_template_name ec2-cluster-test-overwrite


ec2-cluster-ctl create \
    --config configs/test.yaml \
    --cluster_template_name ec2-cluster-test-overwrite \
    --verbose





ec2-cluster-ctl create \
    --config configs/test.yaml \
    --cluster_template_name large-cluster-test \
    --verbose



ec2-cluster-ctl create \
    --clean_create \
    --config configs/test.yaml \
    --cluster_template_name ec2-cluster-test-overwrite \
    --verbose


ec2-cluster-ctl describe \
    --config configs/test.yaml \
    --cluster_template_name ec2-cluster-test-overwrite \
    --verbose


ec2-cluster-ctl describe \
    --cluster_template_name ec2-cluster-test-overwrite
    --node_count 2 \
    --cluster_id 2 \
    --verbose



ec2-cluster-ctl describe \
    --cluster_template_name ec2-cluster-test
    --node_count 2 \
    --cluster_id armand

