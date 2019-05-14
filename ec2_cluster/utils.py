import yaml
import boto3
import os


def get_dlamis(region, ami_type="Ubuntu"):

    assert ami_type in ['Ubuntu', 'Amazon Linux']


    session = boto3.session.Session(region_name=region)
    ec2_client = session.client("ec2")

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.describe_images
    response = ec2_client.describe_images(
            ExecutableUsers=[
                'all',
            ],
            Filters=[
                {
                    'Name': 'name',
                    'Values': [
                        f'Deep Learning AMI ({ami_type}) Version *',
                    ]
                },

            ],
            Owners=[
                'amazon',  # 'self' to get AMIs created by you
            ]
    )


    images = []
    for image in response['Images']:
        name = image['Name']
        description = image['Description']
        image_id = image['ImageId']
        snapshot_id = image['BlockDeviceMappings'][0]['Ebs']['SnapshotId']
        version = float(name.split('Version')[1].strip())

        images.append({
            'Name': name,
            'Version': version,
            'Description': description,
            'ImageId': image_id,
            'SnapshotId': snapshot_id
        })


    return sorted(images, key = lambda i: i['Version'], reverse=True)





def get_my_amis(region):

    session = boto3.session.Session(region_name=region)
    ec2_client = session.client("ec2")

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.describe_images
    response = ec2_client.describe_images(Owners=['self'])

    images = []
    for image in response['Images']:
        name = image['Name']
        if 'Description' in image.keys():
            description = image['Description']
        else:
            description = "No description available"
        image_id = image['ImageId']
        snapshot_id = image['BlockDeviceMappings'][0]['Ebs']['SnapshotId']

        images.append({
            'Name': name,
            'Description': description,
            'ImageId': image_id,
            'SnapshotId': snapshot_id
        })

    return sorted(images, key = lambda i: i['Name'])


def get_config_params():
    path_to_containing_dir = os.path.dirname(os.path.realpath(__file__))
    param_list_yaml_abspath = os.path.join(path_to_containing_dir, "clusterdef_params.yaml")
    config_param_list = yaml.safe_load(open(param_list_yaml_abspath, 'r'))["params"]
    return config_param_list


