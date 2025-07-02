from preprocessing.dask.dask_cluster import DaskCluster


class LocalDaskCluster(DaskCluster):


    @classmethod
    def build(cls, **kwargs):
        pass


    def run_docker_compose(self):