import pytest

from ec2_cluster.config.config import ClusterConfig, ClusterConfigValidationError

# Loading default AMI is slow
@pytest.mark.slow
def test_minimal_config():
    cfg = ClusterConfig(
        cluster_id="1",
        region="us-east-1",
        iam_role="role",
        keypair="keypair",
    )
    cfg.fill_in_defaults()
    cfg.validate()

def test_near_minimal_config():
    cfg = ClusterConfig(
        cluster_id="1",
        region="us-east-1",
        iam_role="role",
        keypair="keypair",
        ami="ami-033d0645d1f338a00",
        username="ec2-user",
    )
    cfg.fill_in_defaults()
    cfg.validate()


def test_config_missing_defaults():
    cfg = ClusterConfig()
    with pytest.raises(ClusterConfigValidationError):
        cfg.validate()

if __name__ == '__main__':
    test_minimal_config()
    test_near_minimal_config()
    test_config_missing_defaults()