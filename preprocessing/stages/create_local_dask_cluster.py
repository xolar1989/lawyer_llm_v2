from preprocessing.dask.dask_cluster import DaskCluster
from preprocessing.dask.local_dask_cluster import LocalDaskCluster
from preprocessing.utils.stage_def import FlowStep, FlowStepError


class CreateLocalDaskCluster(FlowStep):
    dask_cluster: DaskCluster = None

    @classmethod
    @FlowStep.step(task_run_name='create_local_dask_cluster', cache_result_in_memory=False, persist_result=False)
    def run(cls, num_workers) -> DaskCluster:
        return LocalDaskCluster.build(num_workers=num_workers)

    @classmethod
    def rollback(cls, exception: FlowStepError):
        print(f"Rolling back {cls.__name__}")

        return super(CreateLocalDaskCluster, cls).rollback(exception=exception)
