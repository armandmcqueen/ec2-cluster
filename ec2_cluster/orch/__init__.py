from pkgutil import iter_modules
import os

# Workaround to make PyCharm happy
# https://github.com/celery/kombu/blob/7d13f9b95d0b50c94393b962e6def928511bfda6/kombu/__init__.py#L34-L36
STATICA_HACK = True
globals()['kcah_acitats'[::-1].upper()] = False
if STATICA_HACK:
    from .PasswordlessSSH import *





__SPHINX_STRICT__ = ["check_ip_in_known_hosts_cmd",
                     "add_to_known_hosts_cmd",
                     "set_up_passwordless_ssh_from_master_to_workers"]
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
            if not k.startswith('__') and k not in __all__:
                if strict_sphinx and k not in __SPHINX_STRICT__:
                    continue
                globals()[k] = p.__dict__[k]
                __all__.append(k)


_CURR_DIR = os.path.dirname(__file__)
for _, module_name, _ in iter_modules(
       [_CURR_DIR]):
    srcpath = os.path.join(_CURR_DIR, module_name + '.py')
    if not os.path.isfile(srcpath):
        continue
    if not module_name.startswith('_'):
        _global_import(module_name)
