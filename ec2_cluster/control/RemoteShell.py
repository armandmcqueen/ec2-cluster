from abc import ABC, abstractmethod


class RemoteShell(ABC):

    @abstractmethod
    def run_local(self, cmd):
        pass

    @abstractmethod
    def run_on_master(self, cmd):
        pass

    @abstractmethod
    def run_on_all(self, cmd):
        pass

    # We do not have run_on_workers because that lends itself to bugs on single node clusters

    @abstractmethod
    def copy_from_local_to_master(self, local_abs_path, remote_abs_path):
        pass

    @abstractmethod
    def copy_from_local_to_all(self, local_abs_path, remote_abs_path):
        pass

    @abstractmethod
    def copy_from_master_to_local(self, remote_abs_path, local_abs_path):
        pass

    @abstractmethod
    def copy_from_all_to_local(self, remote_abs_path, local_abs_dirpath):
        # local path must be a directory
        pass

    @abstractmethod
    def username(self):
        # retuns string
        pass

    @abstractmethod
    def master_ip(self):
        # returns string
        pass

    @abstractmethod
    def non_master_ips(self):
        # returns list (may be empty)
        pass

    @abstractmethod
    def all_ips(self):
        # returns list
        pass

    @abstractmethod
    def clean_shutdown(self):
        pass

