name = "ec2-cluster"

from pathlib import Path
version_file = Path(__file__).parent/"VERSION"
version = version_file.open('r').read().strip()

