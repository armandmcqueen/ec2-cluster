import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
        name="ec2_cluster",
        version="0.3.1",
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
            'fabric2',
            'boto3',
            'pyyaml',
            'tabulate',
            'cryptography==2.4.2'
        ],
        scripts=['cli/ec3']
)
