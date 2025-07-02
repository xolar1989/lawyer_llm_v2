from abc import ABC, abstractmethod


class DaskCluster(ABC):

    @classmethod
    @abstractmethod
    def build(cls, **kwargs):
        pass

    @abstractmethod
    def delete_dask_cluster(self):
        pass

    @abstractmethod
    def get_workers_count(self):
        pass

    @abstractmethod
    def get_cluster_url(self) -> str:
        pass
