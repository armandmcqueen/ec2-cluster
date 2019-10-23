import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
        name="ec2_cluster",
        version="0.3.2",
        author="Armand McQueen",
        author_email="armandmcqueen@gmail.com",
        description="A tool for launching and running commands on a cluster of EC2 instances",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/armandmcqueen/ec2-cluster",
        packages=setuptools.find_packages(),
        include_package_data=True,
        classifiers=[
            "Programming Language :: Python :: 3.6"
        ],
        install_requires=[
            'paramiko==2.5.1', # Bug in paramiko 2.6.0 (https://github.com/armandmcqueen/ec2-cluster/issues/10)
            'fabric2',
            'boto3',
            'pyyaml',
            'tabulate',
        ],
        scripts=['cli/ec3']
)
