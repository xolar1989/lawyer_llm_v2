import uuid

from preprocessing.utils.dask_cluster import DaskCluster
from preprocessing.utils.sensor import Sensor
from preprocessing.utils.stage_def import FlowStep, FlowStepError


class CreateDaskCluster(FlowStep):
    dask_cluster: DaskCluster = None

    @classmethod
    @FlowStep.step(task_run_name='create_dask_cluster')
    def run(cls, stack_name: str, cluster_name: str, workers_service_name: str, flow_run_id: uuid,
            flow_run_name: str, cluster_props: dict = None):
        return DaskCluster.build(
            stack_name=stack_name,
            cluster_name=cluster_name,
            workers_service_name=workers_service_name,
            flow_run_id=flow_run_id,
            flow_run_name=flow_run_name,
            cluster_props=cluster_props
        )

    @classmethod
    def rollback(cls, exception: FlowStepError):
        print(f"Rolling back {cls.__name__}")

        return super(CreateDaskCluster, cls).rollback(exception=exception)
