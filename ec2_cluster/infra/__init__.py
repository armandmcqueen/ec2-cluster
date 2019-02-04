from pkgutil import iter_modules
import os


from .EC2Node import EC2Node
from .EC2NodeCluster import EC2NodeCluster
from .ConfigCluster import ConfigCluster






__SPHINX_STRICT__ = ["EC2Node",
                     "EC2NodeCluster",
                     "ConfigCluster"]
__all__ = []

# Namespace improvement from Tensorpack.
# Allows import like:   `from ec2_cluster.infra import EC2Node`
# instead of:           `from ec2_cluster.infra.EC2Node import EC2Node
def _global_import(name, strict_sphinx=True):
    p = __import__(name, globals(), locals(), level=1)
    lst = p.__all__ if '__all__' in dir(p) else dir(p)
    if lst:
        del globals()[name]
        for k in lst:
            # print(k)
            if not k.startswith('__') and k not in __all__:
                if strict_sphinx and k not in __SPHINX_STRICT__:
                    # print(f'Skipped due to strict sphinx - {k}')
                    continue
                # print(f'Added - {k}: {p.__dict__[k]}')
                globals()[k] = p.__dict__[k]
                __all__.append(k)


_CURR_DIR = os.path.dirname(__file__)
for _, module_name, _ in iter_modules(
       [_CURR_DIR]):
    # print(f'MODULE - {module_name}')
    srcpath = os.path.join(_CURR_DIR, module_name + '.py')
    if not os.path.isfile(srcpath):
        continue
    if not module_name.startswith('_'):
        _global_import(module_name)
