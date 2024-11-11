from preprocessing.utils.dask_cluster import DaskCluster
from preprocessing.utils.stage_def import FlowStep, FlowStepError


class GetExistingDaskCluster(FlowStep):
    dask_cluster: DaskCluster = None

    @classmethod
    @FlowStep.step(task_run_name='get_existing_dask_cluster')
    def run(cls, stack_name: str):
        return DaskCluster.from_existing(stack_name=stack_name)

    @classmethod
    def rollback(cls, exception: FlowStepError):
        print(f"Rolling back {cls.__name__}")

        return super(GetExistingDaskCluster, cls).rollback(exception=exception)
