from typing import List
import boto3
import ec2_cluster.config.aws as aws


def get_default_vpc(sess: boto3.Session) -> aws.VpcInfo:
    ec2_client = sess.client('ec2')
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.describe_vpcs
    response = ec2_client.describe_vpcs(
        Filters=[
            {
                'Name': 'isDefault',
                'Values': [
                    'true',
                ]
            },
        ],
    )

    default_vpc = aws.VpcInfo.from_vpc_description_json(response["Vpcs"][0])
    return default_vpc




def get_default_subnets(sess: boto3.Session, vpc_id: str) -> List[aws.SubnetInfo]:
    ec2_client = sess.client('ec2')
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.describe_subnets
    response = ec2_client.describe_subnets(
        Filters=[
            {
                'Name': 'vpc-id',
                'Values': [
                    vpc_id,
                ]
            }
        ],
    )
    subnets = [aws.SubnetInfo.from_subnet_description_json(subnet_description) for subnet_description in response["Subnets"]]
    subnets.sort(key=lambda x: x.availability_zone_id)
    return subnets


def get_default_ami(sess: boto3.Session, architecture: aws.ImageArchitecture=aws.ImageArchitecture.x86_64):
    ec2_client = sess.client("ec2")

    filters = []
    if architecture == aws.ImageArchitecture.x86_64:
        filters.append({
            'Name': 'description',
            # Several image variants created for each version. This one is what the EC2 launcher offers
            'Values': [f'Amazon Linux 2 AMI 2.0.* {architecture.value} HVM ebs']
        })
    elif architecture == aws.ImageArchitecture.arm64:
        filters.append({
            'Name': 'description',
            # Several image variants created for each version. This one is what the EC2 launcher offers
            'Values': [f'Amazon Linux 2 LTS Arm64 AMI 2.0.* {architecture.value} HVM gp2']
        })

    filters.append({
        'Name': 'architecture',
        'Values': [architecture.value]
    })


    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.describe_images
    response = ec2_client.describe_images(
        ExecutableUsers=[
            'all',
        ],
        Filters=filters,
        Owners=[
            'amazon',  # 'self' to get AMIs created by you
        ]
    )
    images = [aws.ImageInfo.from_image_description_json(i) for i in response["Images"]]
    images.sort(reverse=True, key=lambda i: i.creation_date)
    most_recent_image = images[0]
    return most_recent_image

def get_default_ebs_device_name(sess: boto3.Session, ami):
    ec2_client = sess.client('ec2')
    response = ec2_client.describe_images(ImageIds=[ami])

    # This was only tested with AMIs that have a single block device mapping.
    # Not sure if that is going to introduce a bug at some point
    block_device_mapping = response["Images"][0]["BlockDeviceMappings"][0]
    ebs_device_name = block_device_mapping["DeviceName"]
    return ebs_device_name