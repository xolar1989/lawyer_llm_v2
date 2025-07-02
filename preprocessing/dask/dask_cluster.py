from abc import ABC, abstractmethod


class DaskCluster(ABC):



    @classmethod
    @abstractmethod
    def build(cls, **kwargs):
        pass