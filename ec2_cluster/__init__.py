name = "ec2-cluster"

from pathlib import Path
version_file = Path(__file__).parent/"VERSION"
__version__ = version_file.open('r').read().strip()

from . import infra, control, orch, utils

